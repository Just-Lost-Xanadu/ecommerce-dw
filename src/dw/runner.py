"""调度与编排：按依赖顺序执行各层，记录运行 trace，并在质量门禁失败时**阻断下游**。

依赖顺序：ods → dim → dwd → quality → dws → ads
  · dim 必须在 dwd 之前：dwd 的 LEFT JOIN dim_sku 需要维表已存在；
  · quality 放在 dws 之前：**质量不合格就不该让脏数据流进指标层**。

幂等：每层内部都是"按分区先删后插"或"全量重算"，所以同一个窗口重跑任意多次，
结果逐行一致（`tests/test_idempotent.py` 把这件事锁住）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import duckdb

from dw import (
    config, db, layer_ads, layer_dim, layer_dwd, layer_dws, layer_ods,
    paths, quality, runlog,
)


@dataclass
class RunResult:
    run_id: str
    batch_id: str
    window_start: date
    window_end: date
    rows: dict[str, int] = field(default_factory=dict)
    quality: list[quality.RuleResult] = field(default_factory=list)
    blocked: bool = False
    block_reason: str = ""
    meta_path: object | None = None

    @property
    def blocking_failures(self) -> list[quality.RuleResult]:
        return quality.blocking_failures(self.quality)

    def steps(self) -> list:
        """按 run_id 从运行记录里取回本轮的每一步。

        为什么从库里读而不是在内存里留一份：**跑批的进程可能中途挂掉**，
        而"这一轮跑到哪一步、哪一步慢、丢了多少行"恰恰是排查失败时最需要的信息。
        落库之后，即使进程没了，也能用 `dw.cli runs` 或直接查 SQLite 把现场捞出来。
        """
        con = db.meta_connect(self.meta_path)  # type: ignore[arg-type]
        try:
            return list(
                con.execute(
                    "SELECT * FROM run_steps WHERE run_id = ? ORDER BY started_at, step",
                    (self.run_id,),
                )
            )
        finally:
            con.close()


def _persist_quality(
    con_meta, run_id: str, results: list[quality.RuleResult]
) -> None:
    """把质量结果落库（保留历史，供趋势分析）。"""
    from datetime import datetime

    con_meta.executemany(
        "INSERT OR REPLACE INTO quality_results"
        "(run_id, layer, rule, layer_table, passed, actual, threshold,"
        " affected_rows, blocking, detail, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                run_id, r.layer, r.rule, r.table,
                1 if r.passed else 0, r.actual, r.threshold,
                r.affected_rows, 1 if r.blocking else 0, r.detail,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            for r in results
        ],
    )
    con_meta.commit()


def run_pipeline(
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    batch_id: str | None = None,
    force: bool = False,
    with_quality: bool = True,
    dashboard: bool = False,
    verbose: bool = True,
    warehouse_path=None,
    meta_path=None,
) -> RunResult:
    """跑完整链路。

    force=True 时**忽略质量阻断继续跑完**——用于演示"如果不加门禁会发生什么"，
    也是真实运维里"已知问题、先出数"的应急通道（但要留痕）。
    """
    baseline = paths.load_baseline()
    if window_start is None or window_end is None:
        window_start, window_end = paths.default_window(baseline)

    log = runlog.RunLog(meta_path=meta_path)
    batch = batch_id or log.run_id
    result = RunResult(
        run_id=log.run_id, batch_id=batch,
        window_start=window_start, window_end=window_end,
        meta_path=meta_path,
    )
    dates = paths.dates_between(window_start, window_end)

    if verbose:
        print(f"[run] run_id={log.run_id}  窗口 {window_start} ~ {window_end}  "
              f"({len(dates)} 天)  batch={batch}")

    try:
        with db.warehouse(warehouse_path) as con:
            # ---- 建表：一次性保证全部表结构存在（幂等，重复执行无副作用）----
            paths.ensure_schema(con)

            # ---- ODS ----
            with log.step("ods") as st:
                r = layer_ods.run(con, dates, batch)
                st.update(r)
            result.rows["ods"] = r["out_rows"]

            # ---- DIM ----
            with log.step("dim") as st:
                r = layer_dim.run(con, dates, batch, window_start, window_end)
                st.update(r)
            result.rows["dim"] = r["out_rows"]

            # ---- DWD ----
            with log.step("dwd") as st:
                r = layer_dwd.run(con, dates, batch, window_start, window_end)
                st.update(r)
            result.rows["dwd"] = r["out_rows"]

            # ---- 质量门禁 ----
            if with_quality:
                with log.step("quality") as st:
                    results = quality.run_quality(con, dates)
                    result.quality = results
                    _persist_quality(log.con, log.run_id, results)
                    passed = sum(1 for x in results if x.passed)
                    st["message"] = f"{passed}/{len(results)} 条规则通过"
                if verbose:
                    print(quality.format_report(results))

            # ---- 门禁判定：失败即阻断下游 ----
            if result.blocking_failures and not force:
                names = ", ".join(f"{r.rule}({r.actual:.4f} < {r.threshold})"
                                  for r in result.blocking_failures)
                reason = f"{len(result.blocking_failures)} 条阻断级规则未通过：{names}"
                result.blocked = True
                result.block_reason = reason
                for step in ("dws", "ads"):
                    log.record_skipped(step, reason)
                if verbose:
                    print(f"\n[BLOCKED] {reason}")
                    print("         下游（dws/ads）已阻断，指标表未更新。")
                    print("         处理方式：修源数据/修规则后重跑；"
                          "或用 --force 强制出数（会在 trace 里留痕）。")
                return result

            # ---- DWS ----
            with log.step("dws") as st:
                r = layer_dws.run(con, dates, batch, window_start, window_end)
                st.update(r)
            result.rows["dws"] = r["out_rows"]

            # ---- ADS ----
            with log.step("ads") as st:
                r = layer_ads.run(con, dates, batch, window_start, window_end)
                st.update(r)
            result.rows["ads"] = r["out_rows"]
            if verbose:
                print(f"[ads] 窗口 GMV = {r['total_gmv']:,.2f}")

            # ---- 看板（可选）----
            if dashboard:
                from dw import export_dashboard

                with log.step("dashboard") as st:
                    out = export_dashboard.export(
                        con, window_start, window_end, batch
                    )
                    st["message"] = str(out.name)
                if verbose:
                    print(f"[dashboard] 已生成：{out}")
    finally:
        if verbose:
            print("\n[run] 步骤明细：")
            print("\n".join(log.summary()))
        log.close()

    return result
