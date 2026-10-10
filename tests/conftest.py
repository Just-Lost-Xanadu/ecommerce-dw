"""测试夹具：把整个链路跑在一个**临时数仓**上。

关键点：环境变量必须在 `import dw` **之前**设置。
`dw.config` 里的路径是在模块导入时求值的，导入之后再改环境变量已经晚了——
这个顺序是这类"配置集中在模块顶层"写法的固有约束，写测试时最容易踩。
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

# ---- 必须在导入 dw 之前设置 ------------------------------------------------
_TMP = Path(tempfile.mkdtemp(prefix="dw-test-"))
os.environ["DW_DATA_DIR"] = str(_TMP / "data")
os.environ["DW_WAREHOUSE"] = str(_TMP / "data" / "warehouse.duckdb")
os.environ["DW_META_DB"] = str(_TMP / "data" / "meta.sqlite")
os.environ["DW_DASHBOARD_DIR"] = str(_TMP / "dashboard")

from dw import config, db, gen_source, runner

# 小样本：够验证规则与口径，又能让整套测试在几十秒内跑完。
# 取值依据：要保证每类脏数据都注入到（见 gen_source 的 _dirty_n 下限），
# 同时按"天 × 用户"分组后不能太稀疏——太小的话漏斗/留存会因为没数据而失真，
# 测试就会变成在验证"空数据也返回 0"这种没有意义的性质。
TEST_SCALE = 0.08


@pytest.fixture(scope="session", autouse=True)
def _cleanup():
    yield
    shutil.rmtree(_TMP, ignore_errors=True)


@pytest.fixture(scope="session")
def baseline() -> dict:
    """生成小样本源数据，返回统计信息。"""
    return gen_source.generate(seed=7, scale=TEST_SCALE)


@pytest.fixture(scope="session")
def first_run(baseline: dict):
    """跑一次完整链路（含质量校验），后续测试复用这次结果。"""
    result = runner.run_pipeline(verbose=False)
    return result


@pytest.fixture()
def con():
    """数仓连接（只读用，测试里不要写）。"""
    with db.warehouse() as conn:
        yield conn


@pytest.fixture(scope="session")
def window(first_run) -> tuple:
    return first_run.window_start, first_run.window_end


# --------------------------------------------------------------------------
# 子进程隔离运行
# --------------------------------------------------------------------------
# 为什么需要它：质量规则的"灵敏度"测试要反复改脏数据开关再跑链路，
# 而链路是**有状态**的（写同一个数仓文件）。如果直接在主进程里跑，
# 后一次运行会把前一次的结果覆盖掉，于是别的测试再去断言"孤儿商品在 ADS 里"
# 就会失败——**测试之间通过共享数仓互相污染**，这是最典型的测试反模式。
#
# 修法是让每次对照实验跑在**独立的临时目录 + 独立进程**里：
# 状态天然隔离，而且顺带验证了"换一个目录也能跑起来"（部署时真正需要的能力）。
_RUNNER_SNIPPET = r"""
import json, os, sys
from datetime import date
from dw import config, gen_source, runner, db, paths

dirty_key = sys.argv[1]
if dirty_key != "__all__":
    config.DIRTY[dirty_key] = False

gen_source.generate(seed=7, scale=float(os.environ["DW_TEST_SCALE"]))
result = runner.run_pipeline(verbose=False)

payload = {
    "quality": [
        {
            "rule": r.rule, "layer": r.layer, "passed": r.passed,
            "actual": r.actual, "threshold": r.threshold,
            "affected_rows": r.affected_rows, "blocking": r.blocking,
            "lower_is_better": r.lower_is_better,
        }
        for r in result.quality
    ],
    "blocked": result.blocked,
    "steps": {s["step"]: s["status"] for s in result.steps()},
}

with db.warehouse() as con:
    # DWD 的剔除原因分布（证明"标记"而不是"静默删除"）
    payload["reject_reasons"] = {
        row[0]: row[1]
        for row in con.execute(
            "SELECT reject_reason, count(*) FROM dwd_order_detail "
            "WHERE reject_reason IS NOT NULL GROUP BY reject_reason"
        ).fetchall()
    }
    # DWD 去重结论
    n, uniq = con.execute(
        "SELECT count(*), count(DISTINCT order_id) FROM dwd_order_detail"
    ).fetchone()
    payload["dwd_rows"], payload["dwd_unique_orders"] = n, uniq
    # ODS 层的重复单号数：去重的**对象**是否存在，只能在 ODS 层看到
    on, ouniq = con.execute(
        "SELECT count(*), count(DISTINCT order_id) FROM ods_order_info"
    ).fetchone()
    payload["ods_rows"], payload["ods_dup_orders"] = on, on - ouniq
    # 孤儿商品是否落进了「未知类目」
    gmv, cnt = con.execute(
        "SELECT coalesce(sum(gmv), 0), coalesce(sum(order_cnt), 0) "
        "FROM ads_gmv_category_1d WHERE category = '未知类目'"
    ).fetchone()
    payload["unknown_category_gmv"] = float(gmv)
    payload["unknown_category_orders"] = int(cnt)
    # 各层行数
    payload["table_rows"] = {
        t: db.table_count(con, t)
        for t in ("ods_order_info", "dwd_order_detail", "dim_sku",
                  "dws_trade_user_1d", "ads_daily_metrics")
    }
    # 日志侧的丢弃量：dwd_page_log 丢掉了多少行（null user_id 的日志、窗口外迟到日志）
    payload["log_rows_ods"] = int(
        con.execute("SELECT count(*) FROM ods_user_log").fetchone()[0]
    )
    payload["log_rows_dwd"] = int(
        con.execute("SELECT count(*) FROM dwd_page_log").fetchone()[0]
    )
    payload["log_dropped"] = payload["log_rows_ods"] - payload["log_rows_dwd"]
    # 日志侧「落在统计窗口之外」的分区文件数 —— late_arrival 注入的可见证据。
    # 注意：这些文件**不会**被 ODS 加载（ODS 按窗口加载），所以只能在这里看。
    _ws, _we = result.window_start, result.window_end
    payload["late_log_partitions"] = sum(
        1
        for p in paths.raw_dir().glob("user_log_*.csv")
        if not (_ws <= date.fromisoformat(p.stem[len("user_log_"):]) <= _we)
    )

print("@@RESULT@@" + json.dumps(payload, ensure_ascii=True))
"""


def _run_isolated(dirty_key: str) -> dict:
    """在独立临时目录 + 独立进程里跑一遍链路，返回质量结果与关键核对数据。"""
    import json
    import subprocess

    tmp = Path(tempfile.mkdtemp(prefix=f"dw-iso-{dirty_key}-"))
    env = {
        **os.environ,
        "DW_DATA_DIR": str(tmp / "data"),
        "DW_WAREHOUSE": str(tmp / "data" / "warehouse.duckdb"),
        "DW_META_DB": str(tmp / "data" / "meta.sqlite"),
        "DW_DASHBOARD_DIR": str(tmp / "dash"),
        "DW_TEST_SCALE": str(TEST_SCALE),
        "PYTHONPATH": str(config.ROOT / "src"),
        "PYTHONIOENCODING": "utf-8",
    }
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _RUNNER_SNIPPET, dirty_key],
            cwd=str(config.ROOT), env=env,
            capture_output=True, text=True, encoding="utf-8", timeout=900,
            check=False,  # 下面手工判 returncode，这里显式声明是刻意的
        )
        if proc.returncode != 0:
            raise AssertionError(
                f"隔离运行失败（dirty_key={dirty_key}）：\n{proc.stdout}\n{proc.stderr}"
            )
        for line in proc.stdout.splitlines():
            if line.startswith("@@RESULT@@"):
                return json.loads(line[len("@@RESULT@@"):])
        raise AssertionError(f"隔离运行没有输出结果：\n{proc.stdout}\n{proc.stderr}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


class _IsolatedRuns:
    """按需运行并缓存隔离结果。

    `__all__` = 全量脏数据；其余键 = 关掉那一类脏数据。
    缓存是必要的：一次隔离运行要跑完整链路（几十秒），
    不缓存的话 7 个参数化用例会跑 7 次全量。
    """

    def __init__(self) -> None:
        self._cache: dict[str, dict] = {}

    def __call__(self, dirty_key: str) -> dict:
        if dirty_key not in self._cache:
            self._cache[dirty_key] = _run_isolated(dirty_key)
        return self._cache[dirty_key]


@pytest.fixture(scope="session")
def isolated_runs() -> _IsolatedRuns:
    return _IsolatedRuns()


@pytest.fixture(scope="session")
def all_dirty(isolated_runs) -> dict:
    """全量脏数据下的一次完整运行结果。"""
    return isolated_runs("__all__")


# --------------------------------------------------------------------------
# 通用隔离项目：在同一临时目录里按需跑一到两次，收集指纹
# --------------------------------------------------------------------------
# 约定（写成环境变量传给子进程，避免命令行转义中文与引号）：
#   DW_ISO_FIRST   JSON: {表名: SQL}   第一次运行后要收集的指纹
#   DW_ISO_SECOND  JSON: {表名: SQL}   第二次运行后要收集的指纹
#   DW_ISO_WINDOW  "full"（默认）| "last_day"   第二次运行只跑最后一天
_ISO_PROJECT_SNIPPET = r"""
import json, os
from dw import config, gen_source, runner, db, paths

VOLATILE = {"_batch_id", "_load_time", "_source_file"}


def collect(spec):
    out = {}
    with db.warehouse() as con:
        for name, sql in spec.items():
            rows = con.execute(sql).fetchall()
            cols = [d[0] for d in con.description]
            keep = [i for i, c in enumerate(cols) if c not in VOLATILE]
            out[name] = [[_jsonable(r[i]) for i in keep] for r in rows]
    return out


def _jsonable(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return str(v)   # date / Decimal 等统一转字符串，避免 JSON 序列化差异


first_spec = json.loads(os.environ.get("DW_ISO_FIRST", "{}"))
second_spec = json.loads(os.environ.get("DW_ISO_SECOND", "{}"))
mode = os.environ.get("DW_ISO_WINDOW", "full")

gen_source.generate(seed=7, scale=float(os.environ["DW_TEST_SCALE"]))
r1 = runner.run_pipeline(verbose=False)
result = {"first": collect(first_spec) if first_spec else {}, "_w1": [str(r1.window_start), str(r1.window_end)]}

if second_spec:
    if mode == "last_day":
        runner.run_pipeline(window_start=r1.window_end, window_end=r1.window_end, verbose=False)
    else:
        runner.run_pipeline(verbose=False)
    result["second"] = collect(second_spec)

print("@@RESULT@@" + json.dumps(result, ensure_ascii=True))
"""


class _IsoProject:
    """在隔离目录里跑项目并收集指纹；缓存键相同的调用只跑一次。"""

    def __init__(self) -> None:
        self._cache: dict[tuple, dict] = {}

    def __call__(
        self,
        key: str,
        first: dict | None = None,
        second: dict | None = None,
        second_window: str = "full",
    ) -> dict:
        cache_key = (key, second_window, bool(second))
        if cache_key in self._cache:
            return self._cache[cache_key]
        self._cache[cache_key] = _run_iso_project(
            key, first or {}, second or {}, second_window
        )
        return self._cache[cache_key]


_PRUNE_CACHE: dict | None = None


def _run_prune_probe() -> dict:
    """在独立临时目录 + 独立进程里跑「窗口 A → 窗口 B」，验证幽灵分区被清理。"""
    import json
    import subprocess

    tmp = Path(tempfile.mkdtemp(prefix="dw-prune-"))
    env = {
        **os.environ,
        "DW_DATA_DIR": str(tmp / "data"),
        "DW_WAREHOUSE": str(tmp / "data" / "warehouse.duckdb"),
        "DW_META_DB": str(tmp / "data" / "meta.sqlite"),
        "DW_DASHBOARD_DIR": str(tmp / "dash"),
        "DW_TEST_SCALE": str(TEST_SCALE),
        "PYTHONPATH": str(config.ROOT / "src"),
        "PYTHONIOENCODING": "utf-8",
    }
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _PRUNE_SNIPPET],
            cwd=str(config.ROOT), env=env,
            capture_output=True, text=True, encoding="utf-8", timeout=900,
            check=False,
        )
        if proc.returncode != 0:
            raise AssertionError(
                f"幽灵分区探测运行失败：\n{proc.stdout}\n{proc.stderr}"
            )
        for line in proc.stdout.splitlines():
            if line.startswith("@@RESULT@@"):
                return json.loads(line[len("@@RESULT@@"):])
        raise AssertionError(f"幽灵分区探测没有输出结果：\n{proc.stdout}\n{proc.stderr}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="session")
def prune_probe() -> dict:
    """幽灵分区探测结果（一次要跑两轮完整链路，所以缓存起来只跑一次）。"""
    global _PRUNE_CACHE
    if _PRUNE_CACHE is None:
        _PRUNE_CACHE = _run_prune_probe()
    return _PRUNE_CACHE


def _run_iso_project(
    key: str, first: dict, second: dict, second_window: str
) -> dict:
    import json
    import subprocess

    tmp = Path(tempfile.mkdtemp(prefix=f"dw-proj-{key}-"))
    env = {
        **os.environ,
        "DW_DATA_DIR": str(tmp / "data"),
        "DW_WAREHOUSE": str(tmp / "data" / "warehouse.duckdb"),
        "DW_META_DB": str(tmp / "data" / "meta.sqlite"),
        "DW_DASHBOARD_DIR": str(tmp / "dash"),
        "DW_TEST_SCALE": str(TEST_SCALE),
        "PYTHONPATH": str(config.ROOT / "src"),
        "PYTHONIOENCODING": "utf-8",
        "DW_ISO_FIRST": json.dumps(first, ensure_ascii=True),
        "DW_ISO_SECOND": json.dumps(second, ensure_ascii=True),
        "DW_ISO_WINDOW": second_window,
    }
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _ISO_PROJECT_SNIPPET],
            cwd=str(config.ROOT), env=env,
            capture_output=True, text=True, encoding="utf-8", timeout=900,
            check=False,
        )
        if proc.returncode != 0:
            raise AssertionError(
                f"隔离项目运行失败（key={key}）：\n{proc.stdout}\n{proc.stderr}"
            )
        for line in proc.stdout.splitlines():
            if line.startswith("@@RESULT@@"):
                return json.loads(line[len("@@RESULT@@"):])
        raise AssertionError(f"隔离项目没有输出结果：\n{proc.stdout}\n{proc.stderr}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="session")
def iso_project() -> _IsoProject:
    return _IsoProject()


# --------------------------------------------------------------------------
# 幽灵分区探测：跑「窗口 A →（换结算日重新造数）→ 窗口 B」，验证窗口 A 被清掉
# --------------------------------------------------------------------------
_PRUNE_SNIPPET = r"""
import json, os
from dw import config, gen_source, paths, runner, db

scale = float(os.environ["DW_TEST_SCALE"])

# ---- 第一轮：结算日 A（gen_source 的默认 SETTLE_DATE），全窗口跑批 ----
gen_source.generate(seed=7, scale=scale)
base_a = paths.load_baseline()
start_a, end_a = paths.default_window(base_a)
runner.run_pipeline(window_start=start_a, window_end=end_a,
                    prune_out_of_window=True, verbose=False)

# ---- 第二轮：把结算日整体推后（模拟「改了结算日之后重新造数并重跑」）----
#      两轮窗口不重叠 —— 这正是产生「幽灵分区」的真实场景
gen_source.SETTLE_DATE = "2026-12-05"
gen_source.generate(seed=7, scale=scale)
base_b = paths.load_baseline()
start_b, end_b = paths.default_window(base_b)
r2 = runner.run_pipeline(window_start=start_b, window_end=end_b,
                         prune_out_of_window=True, verbose=False)

_range = (f"dt < DATE '{start_b.isoformat()}' "
          f"OR dt > DATE '{end_b.isoformat()}'")

with db.warehouse() as con:
    outside = con.execute(
        f"SELECT count(*) FROM ods_order_info WHERE {_range}"
    ).fetchone()[0]
    dwd_outside = con.execute(
        f"SELECT count(*) FROM dwd_order_detail WHERE {_range}"
    ).fetchone()[0]
    total_rows = con.execute("SELECT count(*) FROM ods_order_info").fetchone()[0]
    n_days = con.execute(
        "SELECT count(DISTINCT dt) FROM ods_order_info"
    ).fetchone()[0]
    min_dt, max_dt = con.execute(
        "SELECT min(dt), max(dt) FROM ods_order_info"
    ).fetchone()

ods_message = ""
for _row in r2.steps():
    if _row["step"] == "ods":
        ods_message = _row["message"] or ""

payload = {
    "window_a": [str(start_a), str(end_a)],
    "window_b": [str(start_b), str(end_b)],
    "disjoint": bool(end_a < start_b),
    "outside_rows": int(outside),
    "dwd_outside_rows": int(dwd_outside),
    "total_rows": int(total_rows),
    "distinct_dt": int(n_days),
    "min_dt": str(min_dt),
    "max_dt": str(max_dt),
    "ods_message": ods_message,
}
print("@@RESULT@@" + json.dumps(payload, ensure_ascii=True))
"""
