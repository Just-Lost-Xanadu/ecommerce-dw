"""DWD 层：明细数据层（清洗 + 有效性判定 + 剔除审计）。

这一层是整条链路的"收口点"：所有口径（去重、有效性、退款处理）都在这里定死，
上层只引用 DWD，不再各自过滤——**这就是"对不上账"问题的根治方式**。
"""

from __future__ import annotations

from datetime import date

import duckdb

from dw import paths


def _run_per_date(
    con: duckdb.DuckDBPyConnection,
    template: str,
    dates: list[date],
    batch_id: str,
    settle: date,
    window_start: date,
) -> None:
    """按天执行模板（模板里已含 DELETE + INSERT，天然幂等）。"""
    for dt in dates:
        con.execute(
            paths.render(
                template,
                batch_id=batch_id,
                dt=dt.isoformat(),
                settle=settle.isoformat(),
                window_start=window_start.isoformat(),
            )
        )


def run(
    con: duckdb.DuckDBPyConnection,
    dates: list[date],
    batch_id: str,
    window_start: date,
    window_end: date,
) -> dict:
    # 订单明细：去重 + 有效性判定 + 剔除审计。
    # 注意 DWD 依赖 dim_sku（LEFT JOIN 取 sku_name），所以 runner 里 DIM 必须先于 DWD。
    _run_per_date(con, "dwd_order_detail", dates, batch_id, window_end, window_start)

    # 页面日志：丢空 user_id + 丢窗口外迟到数据
    for dt in dates:
        con.execute(
            paths.render(
                "dwd_page_log",
                batch_id=batch_id,
                dt=dt.isoformat(),
                settle=window_end.isoformat(),
                window_start=window_start.isoformat(),
            )
        )

    start, end = dates[0].isoformat(), dates[-1].isoformat()
    order_total = int(con.execute(
        f"SELECT count(*) FROM dwd_order_detail WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    ).fetchone()[0])
    order_valid = int(con.execute(
        f"SELECT count(*) FROM dwd_order_detail WHERE dt BETWEEN DATE '{start}' AND DATE '{end}' AND is_valid"
    ).fetchone()[0])
    log_total = int(con.execute(
        f"SELECT count(*) FROM dwd_page_log WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    ).fetchone()[0])
    rejected = order_total - order_valid
    reject_rate = (rejected / order_total) if order_total else 0.0

    # 剔除原因分布：给质量报告和 README 提供"少了多少、为什么少"的依据
    reasons = con.execute(
        f"SELECT coalesce(reject_reason,'kept') AS reason, count(*) AS n "
        f"FROM dwd_order_detail WHERE dt BETWEEN DATE '{start}' AND DATE '{end}' "
        f"GROUP BY 1 ORDER BY n DESC"
    ).fetchall()
    reason_txt = " ".join(f"{r[0]}={r[1]}" for r in reasons if r[0] != "kept")

    return {
        "in_rows": order_total + log_total,
        "out_rows": order_valid + log_total,
        "dropped_rows": rejected,
        "reject_rate": reject_rate,
        "message": f"order_valid={order_valid:,}/{order_total:,} "
                   f"log={log_total:,} reject_rate={reject_rate:.2%}"
                   + (f" [{reason_txt}]" if reason_txt else ""),
    }
