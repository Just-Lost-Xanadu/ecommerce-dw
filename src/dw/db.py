"""DuckDB（数仓）与 SQLite（运行元数据）的连接管理。

为什么要分成两个库：
- **数仓本体**（DuckDB）只放业务数据，分层表、维度表、指标表；
- **运行元数据**（SQLite）放"这个数仓是怎么跑出来的"——每步耗时、影响行数、质量结果。
  两者生命周期不同：数仓可以随时全量重建，而**运行历史必须留存**，否则没法回答
  "昨天那次失败是什么原因""质量指标是不是在退化"。
"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import duckdb

from dw import config

# ---- 数仓（DuckDB） --------------------------------------------------------
# 锁冲突时给出的可操作指引。抽成常量是为了让测试能直接断言"指引里必须包含
# 可执行的步骤"，而不是去匹配一大段文案。
_LOCK_HINT = """\
这几乎总是因为**上一轮跑批被中断后，还有子进程占着这个文件**
（DuckDB 单文件库同一时刻只允许一个写连接）。
注意：DuckDB 报出的 PID **可能是已经退出的进程**，所以不要只照着 PID 去找——照了也找不到。

按顺序做：
  1) 先看有没有残留进程，有就结束掉：
       Get-Process python -ErrorAction SilentlyContinue
       Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force
  2) 确认没有别的地方开着这个库：另一个终端、DB 客户端、IDE 的数据库插件。
  3) 都清了还报同样的错，说明锁没释放干净：删掉同目录下的
       <库名>.duckdb.wal 与 <库名>.duckdb.tmp
     再重试。（它们只是未落盘的中间产物，但**如果上次跑批是被强杀的，
     保险做法是清库后重跑一次**，别直接接着用可能不完整的数仓。）
  4) 想彻底绕开：换一个数仓路径，例如
       set DW_WAREHOUSE=data\\warehouse2.duckdb
"""


def _connect_error(path: Path, exc: BaseException) -> str:
    """把底层连接异常翻译成"接下来该做什么"。

    这里**不能用 `"lock" in text` 这种子串判断**：`blocked` / `unlocked` 之类
    同形词会误命中，于是把"路径不存在"这类问题也说成"残留进程占着库"，
    排查方向直接被带偏。所以只认 DuckDB 实际会打出的那几句短语。
    """
    text = str(exc)
    low = text.lower()
    looks_like_lock = any(
        phrase in low
        for phrase in (
            "conflicting lock",
            "could not set lock",
            "already open in",
            "file is already open",
        )
    )
    head = f"无法打开数仓文件：{path}\n底层报错：{text}\n\n"
    if looks_like_lock:
        return head + _LOCK_HINT
    return head + (
        "这不像锁冲突。请检查：路径是否存在、当前用户对该目录有没有写权限、"
        "磁盘是否已满，以及文件是否被别的程序（杀毒、同步盘）占用。"
    )


def connect(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """打开数仓连接。首次调用会自动创建目录与文件。

    被中断的跑批会留下持锁的子进程，之后每个命令都失败；而 DuckDB 的原始报错只说
    "File is already open in python.exe (PID xxx)" 且 **PID 往往是死的**——
    照着它排查会白费时间。所以这里统一抛 `RuntimeError` 并附上可操作的步骤。
    """
    path = Path(db_path or config.WAREHOUSE_DB)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        con = duckdb.connect(str(path))
    except (duckdb.Error, OSError) as exc:
        raise RuntimeError(_connect_error(path, exc)) from exc
    # 让 DuckDB 的字符串比较/排序按大小写不敏感语义走，
    # 避免中文列值排序结果随环境（locale）不同而变化。
    con.execute("SET default_collation = 'nocase'")
    # 关闭"保留插入顺序"。跑批场景下所有产出都是聚合结果或按 dt 分区读写的，
    # **没有任何一处依赖行的物理顺序**，而关闭它可以省掉 DuckDB 维护行序的开销
    # （在百万行级的写入上实测有可观收益）。
    con.execute("SET preserve_insertion_order = false")
    return con


@contextlib.contextmanager
def warehouse(db_path: Path | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    con = connect(db_path)
    try:
        yield con
    finally:
        con.close()


def list_tables(con: duckdb.DuckDBPyConnection, schema: str = "main") -> list[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
        [schema],
    ).fetchall()
    return [r[0] for r in rows]


def table_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    """表行数。表不存在时返回 -1，便于调用方区分"空表"和"没建表"。"""
    try:
        return int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
    except duckdb.CatalogException:
        return -1


def tables_with_column(
    con: duckdb.DuckDBPyConnection, column: str, schema: str = "main"
) -> list[str]:
    """列出含指定列的所有基表（按表名排序）。"""
    rows = con.execute(
        "SELECT c.table_name FROM information_schema.columns c "
        "JOIN information_schema.tables t "
        "  ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
        "WHERE c.table_schema = ? AND c.column_name = ? "
        "  AND t.table_type = 'BASE TABLE' "
        "ORDER BY c.table_name",
        [schema, column],
    ).fetchall()
    return [r[0] for r in rows]


def prune_out_of_window(
    con: duckdb.DuckDBPyConnection, window_start, window_end,
) -> dict[str, int]:
    """把"不属于当前统计窗口"的历史分区从各层表里删掉（幽灵分区清理）。

    为什么必须有这一步：
      ODS 的幂等是「**按 dt 分区先删后插**」——它只能保证「这一天不会被写两遍」，
      **完全无法阻止别的窗口留下的分区一直躺在表里**。
      真实踩过的场景：换个结算日重新造数后重跑，新窗口与旧窗口的日期不重叠，
      于是 ods_order_info 里同时存在两代数据（实测 181,400 × 2 = 362,800 行、
      两个不同的 _batch_id）；更糟的是**质量规则和对账都带 `dt BETWEEN 窗口`
      过滤，完全看不见它们**，最后是 `dw.cli tables` 的全表 count(*) 才暴露。

    为什么要有开关：本项目同时支持「全窗口跑批」和「只补一天（T+1 日批）」。
      补数场景下窗口**之外**的那些天是**合法数据**，绝不能删。
      所以删除只在「这一轮是权威的全窗口重建」时进行，由调用方通过
      `runner.run_pipeline(prune_out_of_window=...)` 决定（默认自动判断）。

    覆盖范围：所有含 dt 列的基表（含 dwd / dws / ads 层）。
      **不含 dim_date**——它是纯日历表，layer_dim 明确只重建窗口内的日期、
      有意保留窗口外的历史日期，不应该被这里删掉。

    返回 {表名: 删除行数}，只含真正删了行的表。
    """
    start, end = window_start.isoformat(), window_end.isoformat()
    where = f"dt < DATE '{start}' OR dt > DATE '{end}'"
    pruned: dict[str, int] = {}
    for table in tables_with_column(con, "dt"):
        n = int(
            con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
        )
        if n:
            con.execute(f"DELETE FROM {table} WHERE {where}")
            pruned[table] = n
    return pruned


# ---- 运行元数据（SQLite） --------------------------------------------------
_META_SCHEMA = """
CREATE TABLE IF NOT EXISTS run_steps (
    run_id       TEXT    NOT NULL,
    step         TEXT    NOT NULL,
    step_seq     INTEGER,                 -- 真实执行顺序 1,2,3…；排序以它为准
    status       TEXT    NOT NULL,        -- ok / failed / blocked
    started_at   TEXT    NOT NULL,        -- 步骤**开始**的时刻（精确到秒）
    duration_ms  INTEGER NOT NULL,
    in_rows      INTEGER,
    out_rows     INTEGER,
    dropped_rows INTEGER,
    message      TEXT,
    PRIMARY KEY (run_id, step)
);

CREATE TABLE IF NOT EXISTS quality_results (
    run_id      TEXT    NOT NULL,
    layer       TEXT    NOT NULL,
    rule        TEXT    NOT NULL,
    layer_table TEXT    NOT NULL,
    passed      INTEGER NOT NULL,          -- 1/0
    actual      REAL,
    threshold   REAL,
    affected_rows INTEGER,
    blocking    INTEGER NOT NULL,          -- 1 = 失败即阻断下游
    detail      TEXT,
    created_at  TEXT    NOT NULL,
    PRIMARY KEY (run_id, rule)
);

CREATE INDEX IF NOT EXISTS idx_quality_rule ON quality_results(rule, created_at);
"""


def _migrate_meta(con: sqlite3.Connection) -> None:
    """增量迁移：给**已存在**的 run_steps 补上新列。

    为什么不能只靠 `CREATE TABLE IF NOT EXISTS`：老库里 run_steps 已经存在，
    `IF NOT EXISTS` 会让整条 CREATE 被跳过，新列**永远加不上**，
    于是写入时直接报 `no such column: step_seq`。
    所以表结构变更必须写成「先查现有列 → 缺什么补什么」的显式迁移。
    用 `PRAGMA table_info` 而不是 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`：
    后者在多数 SQLite 版本上并不支持，写了会直接语法报错。
    """
    cols = {row[1] for row in con.execute("PRAGMA table_info(run_steps)")}
    if "step_seq" not in cols:
        con.execute("ALTER TABLE run_steps ADD COLUMN step_seq INTEGER")
    con.commit()


def meta_connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = Path(db_path or config.META_DB)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    # WAL：跑批时读历史与写当前步不互相阻塞。
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(_META_SCHEMA)
    _migrate_meta(con)
    con.commit()
    return con
