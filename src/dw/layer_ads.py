"""ADS 层：应用数据层（对外指标）。

三张表都按**统计窗口覆盖写**（DELETE 窗口内行 + INSERT）：行数很小（天 × 维度），
重算成本可忽略。窗口外历史由全窗口跑批的 prune 负责清理，`--limit 1` 补数时保留。
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
        # 删除窗口内旧行与写入新行必须在同一事务里：否则 INSERT 失败时
        # 旧指标已经被删掉，看板会直接少一天。
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(
                paths.render(
                    template,
                    batch_id=batch_id,
                    window_start=window_start.isoformat(),
                    window_end=window_end.isoformat(),
                )
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise

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
