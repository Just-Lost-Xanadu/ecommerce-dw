"""DWS 层：轻度汇总（用户 × 天的流量与交易）。

DWS 是"半成品"层——不做指标口径，只把明细按最常用的维度预聚合。
好处：多个指标共享同一份中间结果，且明细再大也不会让 ADS 变慢。
"""

from __future__ import annotations

from datetime import date

import duckdb

from dw import paths


def run(
    con: duckdb.DuckDBPyConnection,
    dates: list[date],
    batch_id: str,
    window_start: date,
    window_end: date,
) -> dict:
    # 每条模板都是「CREATE OR REPLACE 重建 → ALTER ADD PRIMARY KEY」两步，
    # 主键是 DWS "同一用户同一天只有一行" 的结构保证（下游 ADS 直接按它汇总）。
    # 两步必须在**同一个事务**里：否则中间崩掉会留下一张没有主键的表，
    # 之后的 --limit 1 重跑会把重复行原样复制下去，而 run_steps 只记一条 failed，
    # 没人知道约束已经丢了。其它层（ODS/DIM/ADS）都有显式 BEGIN/COMMIT，这里补齐。
    con.execute("BEGIN TRANSACTION")
    try:
        for template in ("dws_traffic_user_1d", "dws_trade_user_1d"):
            for dt in dates:
                con.execute(
                    paths.render(
                        template,
                        batch_id=batch_id,
                        dt=dt.isoformat(),
                        window_start=window_start.isoformat(),
                        window_end=window_end.isoformat(),
                    )
                )
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise

    start, end = dates[0].isoformat(), dates[-1].isoformat()
    n_traffic = int(con.execute(
        f"SELECT count(*) FROM dws_traffic_user_1d "
        f"WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    ).fetchone()[0])
    n_trade = int(con.execute(
        f"SELECT count(*) FROM dws_trade_user_1d "
        f"WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    ).fetchone()[0])

    # 倾斜证据：单用户单日最大事件数 vs 中位数（长尾在 DWS 这一层就已经可见）
    skew = con.execute(
        f"SELECT max(event_cnt), median(event_cnt) FROM dws_traffic_user_1d "
        f"WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    ).fetchone()
    skew_txt = ""
    if skew and skew[0]:
        skew_txt = f" max_user_events={int(skew[0])} median={float(skew[1] or 0):.0f}"

    return {
        "in_rows": n_traffic + n_trade,
        "out_rows": n_traffic + n_trade,
        "message": f"traffic={n_traffic:,} trade={n_trade:,}{skew_txt}",
    }
