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
    # 三张指标表**整层一个事务**：旧实现是每张表各自 BEGIN/COMMIT，于是
    # ads_daily_metrics 提交成功、ads_gmv_category_1d 失败时，对外就是"总 GMV 更新了、
    # 品类 GMV 还是上一批"。两个看板数字互相打架，比整层失败更难发现（各步状态都是 ok）。
    # 行数本来就小（天 × 维度），整层回滚的成本可以忽略。
    con.execute("BEGIN TRANSACTION")
    try:
        for template in TEMPLATES:
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
