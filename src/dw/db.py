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
def connect(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """打开数仓连接。首次调用会自动创建目录与文件。"""
    path = Path(db_path or config.WAREHOUSE_DB)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
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


# ---- 运行元数据（SQLite） --------------------------------------------------
_META_SCHEMA = """
CREATE TABLE IF NOT EXISTS run_steps (
    run_id       TEXT    NOT NULL,
    step         TEXT    NOT NULL,
    status       TEXT    NOT NULL,        -- ok / failed / blocked
    started_at   TEXT    NOT NULL,
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


def meta_connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = Path(db_path or config.META_DB)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    # WAL：跑批时读历史与写当前步不互相阻塞。
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(_META_SCHEMA)
    con.commit()
    return con
