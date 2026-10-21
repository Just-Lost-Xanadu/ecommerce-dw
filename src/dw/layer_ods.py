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

    rows_order = 0
    rows_log = 0
    missing: list[str] = []

    for dt in dates:
        order_src = paths.dt_csv("order_info", dt)
        log_src = paths.dt_csv("user_log", dt)
        # 订单与日志两侧都要报缺失：只记订单侧时，日志分区整段缺失会静默变成
        # 流量层空数据，而告警里一个字都不会提。
        if not order_src.is_file():
            missing.append(f"{dt.isoformat()}(order)")
        if not log_src.is_file():
            missing.append(f"{dt.isoformat()}(log)")
        rows_order += _load_one(
            con, "ods_order_info", "ods_order_info", order_src, dt, "order_date", batch_id
        )
        rows_log += _load_one(
            con, "ods_user_log", "ods_user_log", log_src, dt, "log_date", batch_id
        )

    if missing:
        # 分区文件缺失不是错误（那一天本来就可能没有数据），但必须留下痕迹。
        print(f"[ods] 以下日期没有源分区文件，已按空分区处理：{', '.join(missing[:5])}"
              + (" ..." if len(missing) > 5 else ""))

    return {
        "in_rows": rows_order + rows_log,
        "out_rows": rows_order + rows_log,
        "order_rows": rows_order,
        "log_rows": rows_log,
        "message": f"order={rows_order:,} log={rows_log:,}",
    }
