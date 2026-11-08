"""ODS 层：原始数据落地（原样搬入，不做业务清洗）。

幂等策略：**按分区覆盖写**——先 `DELETE WHERE dt = ?`，再插入。
所以同一天跑多少次，结果都只有一份。这是整条链路幂等的地基：
如果 ODS 就会写重复，上层不管怎么修都对不上账。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from dw import paths


def _load_one(
    con: duckdb.DuckDBPyConnection,
    template: str,
    table: str,
    src: Path,
    dt: date,
    date_col: str,
    batch_id: str,
) -> int:
    """加载单个分区。返回该分区行数。"""
    # 分区覆盖：先删后插。用事务包住，避免"删了但插入失败"留下空分区。
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DELETE FROM {table} WHERE dt = DATE '{dt.isoformat()}'")
        if src.is_file():
            con.execute(
                paths.render(
                    template,
                    # 注意：只传裸路径。render() 会统一加引号——
                    # 这里再手动包一层引号会变成 ''path''，DuckDB 会当成字面量匹配不到文件。
                    src=src.as_posix(),
                    src_name=src.name,
                    batch_id=batch_id,
                    dt=dt.isoformat(),
                    dt_col=date_col,
                )
            )
        rows = int(
            con.execute(
                f"SELECT count(*) FROM {table} WHERE dt = DATE '{dt.isoformat()}'"
            ).fetchone()[0]
        )
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return rows


def run(
    con: duckdb.DuckDBPyConnection,
    dates: list[date],
    batch_id: str,
) -> dict:
    """把 dates 里每一天的源数据加载进 ODS。"""
    if not paths.raw_exists():
        raise FileNotFoundError(
            "源数据不存在，请先运行：python -m dw.cli gen"
        )

    # 建表（只执行模板里的 CREATE TABLE 部分，避免带着空路径去跑 INSERT）
    paths.create_tables(con, "ods_order_info", "ods_user_log")

    # 先完整检查所有分区的存在性，再动任何一张表：旧实现"边删边读"，
    # 中间才遇到缺失时前面的分区已经被覆盖，留下一次半成品跑批。
    missing: list[str] = []
    sources: list[tuple[date, Path, Path]] = []
    for dt in dates:
        order_src = paths.dt_csv("order_info", dt)
        log_src = paths.dt_csv("user_log", dt)
        # 订单与日志两侧都要报缺失：只记订单侧时，日志分区整段缺失会静默变成
        # 流量层空数据，而告警里一个字都不会提。
        if not order_src.is_file():
            missing.append(f"{dt.isoformat()}(order)")
        if not log_src.is_file():
            missing.append(f"{dt.isoformat()}(log)")
        sources.append((dt, order_src, log_src))

    if missing:
        # 缺失窗口内分区**不能当空分区**：旧实现会先 DELETE 再加载 0 行，质量门禁
        # 仍全绿、ADS 当天 GMV=0，看起来像"那天没生意"而不是"文件丢了"。
        # 在接入边界直接失败，比让空数据流进指标层便宜得多。
        shown = ", ".join(missing[:8]) + (" ..." if len(missing) > 8 else "")
        raise FileNotFoundError(
            f"统计窗口内有 {len(missing)} 个源分区文件缺失：{shown}。"
            "请先运行 python -m dw.cli gen 重新造数，或把缺失的分区补回 data/raw/by_dt/。"
        )

    rows_order = 0
    rows_log = 0
    for dt, order_src, log_src in sources:
        rows_order += _load_one(
            con, "ods_order_info", "ods_order_info", order_src, dt, "order_date", batch_id
        )
        rows_log += _load_one(
            con, "ods_user_log", "ods_user_log", log_src, dt, "log_date", batch_id
        )

    return {
        "in_rows": rows_order + rows_log,
        "out_rows": rows_order + rows_log,
        "order_rows": rows_order,
        "log_rows": rows_log,
        "message": f"order={rows_order:,} log={rows_log:,}",
    }
