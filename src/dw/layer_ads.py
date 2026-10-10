"""ADS 层：应用数据层（对外指标）。

三张表都是**全量重算**：行数很小（天 × 维度），重算成本可忽略，
换来的是"所有日期的数字都基于同一套口径"——这部分区刷新的部分结果更不容易出现口径漂移。
"""

from __future__ import annotations

from datetime import date

import duckdb

from dw import paths

TEMPLATES = ("ads_daily_metrics", "ads_gmv_category_1d", "ads_funnel_1d")


def run(
    con: duckdb.DuckDBPyConnection,
    dates: list[date],
    batch_id: str,
    window_start: date,
    window_end: date,
) -> dict:
    for template in TEMPLATES:
        con.execute(
            paths.render(
                template,
                batch_id=batch_id,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
            )
        )

    start, end = window_start.isoformat(), window_end.isoformat()
    n_daily = int(con.execute(
        f"SELECT count(*) FROM ads_daily_metrics "
        f"WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    ).fetchone()[0])
    n_cat = int(con.execute(
        f"SELECT count(*) FROM ads_gmv_category_1d "
        f"WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    ).fetchone()[0])
    n_funnel = int(con.execute(
        f"SELECT count(*) FROM ads_funnel_1d "
        f"WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    ).fetchone()[0])

    total_gmv = con.execute(
        f"SELECT coalesce(sum(gmv), 0) FROM ads_daily_metrics "
        f"WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    ).fetchone()[0]

    return {
        "in_rows": n_daily + n_cat + n_funnel,
        "out_rows": n_daily + n_cat + n_funnel,
        "total_gmv": float(total_gmv),
        "message": f"daily={n_daily} category={n_cat} funnel={n_funnel} "
                   f"gmv={float(total_gmv):,.2f}",
    }
