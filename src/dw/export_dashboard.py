"""看板导出：把 ADS 层的指标渲染成一张**可离线打开的 HTML**。

为什么不用 Superset / Tableau：
  1. 核实过的 11 份 2027 届数据岗 JD 里，BI 工具只被 2 家点名；
  2. 面试时你需要的是一个"打开就能看"的产物，而不是"你得先装个 BI 平台"；
  3. 自渲染 HTML 顺带证明了"我懂前端能出图"，且零依赖。
"""

from __future__ import annotations

import html
from datetime import date
from pathlib import Path

import duckdb

from dw import config, paths

_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>电商离线数仓 · 运营日报</title>
<style>
  :root {{ color-scheme: light; }}
  body {{ font-family: -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif;
         margin: 0; padding: 28px 32px; background:#f6f7f9; color:#1c1f23; }}
  h1 {{ font-size: 21px; margin: 0 0 4px; }}
  .meta {{ color:#6b7280; font-size: 13px; margin-bottom: 22px; }}
  .meta code {{ background:#eceef2; padding:1px 5px; border-radius:4px; }}
  .cards {{ display:flex; gap:14px; flex-wrap:wrap; margin-bottom:24px; }}
  .card {{ background:#fff; border:1px solid #e3e6ea; border-radius:10px;
           padding:14px 18px; min-width:150px; }}
  .card .k {{ font-size:12px; color:#6b7280; margin-bottom:6px; }}
  .card .v {{ font-size:22px; font-weight:650; letter-spacing:-.3px; }}
  table {{ border-collapse: collapse; width:100%; background:#fff;
           border:1px solid #e3e6ea; border-radius:10px; overflow:hidden;
           margin-bottom:26px; font-size:13px; }}
  caption {{ text-align:left; font-weight:650; padding:12px 4px 8px; font-size:14px; }}
  th, td {{ padding:8px 12px; text-align:right; border-bottom:1px solid #eef0f3; }}
  th {{ background:#fafbfc; color:#4b5563; font-weight:600; text-align:right; }}
  th:first-child, td:first-child {{ text-align:left; }}
  tbody tr:hover {{ background:#fafbfc; }}
  tbody tr:last-child td {{ border-bottom:none; }}
  .hot {{ color:#b42318; font-weight:600; }}
  footer {{ color:#9aa1ab; font-size:12px; margin-top:8px; }}
</style>
</head>
<body>
<h1>电商用户行为与订单离线数仓 · 运营日报</h1>
<div class="meta">
  统计窗口 <code>{window_start} ~ {window_end}</code> ·
  批次 <code>{batch_id}</code> ·
  引擎 DuckDB ·
  生成时间 <code>{generated_at}</code>
</div>
{cards}
{daily}
{category}
{funnel}
<footer>本页由 <code>python -m dw.cli dashboard</code> 离线渲染，不依赖任何 BI 平台。</footer>
</body>
</html>
"""


def _fmt_num(v: object) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        return f"{v:,.4f}".rstrip("0").rstrip(".")
    if isinstance(v, int):
        return f"{v:,}"
    return html.escape(str(v))


def _table(
    con: duckdb.DuckDBPyConnection, title: str, sql: str,
    highlight: str | None = None,
) -> str:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = []
    for r in rows:
        tds = []
        for c, v in zip(cols, r, strict=True):
            cls = ' class="hot"' if highlight and c == highlight and v not in (None, 0) else ""
            tds.append(f"<td{cls}>{_fmt_num(v)}</td>")
        body.append("<tr>" + "".join(tds) + "</tr>")
    return (
        f'<table><caption>{html.escape(title)}</caption>'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"
    )


def export(
    con: duckdb.DuckDBPyConnection,
    window_start: date,
    window_end: date,
    batch_id: str,
    out_path: Path | None = None,
) -> Path:
    start, end = window_start.isoformat(), window_end.isoformat()

    # ---- 顶部卡片：窗口内汇总 ----
    summary = con.execute(f"""
        SELECT
            coalesce(sum(gmv), 0)                                  AS gmv,
            coalesce(sum(order_cnt), 0)                            AS orders,
            coalesce(round(avg(dau)), 0)                           AS avg_dau,
            coalesce(round(avg(pay_conversion) * 100, 2), 0)       AS conv_pct,
            coalesce(round(sum(gmv) / nullif(sum(order_cnt), 0), 2), 0) AS aov
        FROM ads_daily_metrics
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
    """).fetchone()
    cards = (
        '<div class="cards">'
        f'<div class="card"><div class="k">窗口 GMV（净，已去重去退款）</div><div class="v">¥{summary[0]:,.0f}</div></div>'
        f'<div class="card"><div class="k">有效订单数</div><div class="v">{summary[1]:,.0f}</div></div>'
        f'<div class="card"><div class="k">日均 DAU</div><div class="v">{summary[2]:,.0f}</div></div>'
        f'<div class="card"><div class="k">支付转化率（平均）</div><div class="v">{summary[3]:.2f}%</div></div>'
        f'<div class="card"><div class="k">客单价 AOV</div><div class="v">¥{summary[4]:,.2f}</div></div>'
        "</div>"
    )

    daily = _table(
        con, "每日核心指标",
        f"""
        SELECT dt AS 日期, dau AS DAU, new_users AS 新增用户, pv AS PV,
               visit_users AS 浏览用户, order_cnt AS 订单数, gmv AS GMV,
               pay_users AS 支付用户, aov AS 客单价,
               round(pay_conversion * 100, 2) AS "支付转化率%",
               round(top1_sku_share * 100, 1) AS "Top1商品占比%"
        FROM ads_daily_metrics
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
        ORDER BY dt
        """,
        highlight="Top1商品占比%",
    )

    category = _table(
        con, "品类 GMV（关注「未知类目」——那是孤儿商品的可见出口）",
        f"""
        SELECT category AS 类目, sum(order_cnt) AS 订单数, sum(sku_cnt) AS 商品件数,
               round(sum(gmv), 0) AS GMV, sum(user_cnt) AS 用户数
        FROM ads_gmv_category_1d
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
        GROUP BY category ORDER BY GMV DESC
        """,
        highlight="类目",
    )

    funnel = _table(
        con, "转化漏斗（人数口径，同日行为）",
        f"""
        SELECT dt AS 日期,
               max(user_cnt) FILTER (WHERE stage = 'view') AS 浏览,
               max(user_cnt) FILTER (WHERE stage = 'cart') AS 加购,
               max(user_cnt) FILTER (WHERE stage = 'buy')  AS 下单,
               round(max(conversion_rate) FILTER (WHERE stage = 'cart') * 100, 2) AS "浏览→加购%",
               round(max(conversion_rate) FILTER (WHERE stage = 'buy') * 100, 2)  AS "加购→下单%"
        FROM ads_funnel_1d
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
        GROUP BY dt ORDER BY dt
        """,
    )

    out = Path(out_path or (config.DASHBOARD_DIR / "report.html"))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        _TEMPLATE.format(
            window_start=start, window_end=end, batch_id=batch_id,
            generated_at=date.today().isoformat(),
            cards=cards, daily=daily, category=category, funnel=funnel,
        ),
        encoding="utf-8",
    )
    return out


def main() -> None:
    from dw import db, paths

    baseline = paths.load_baseline()
    start, end = paths.default_window(baseline)
    with db.warehouse() as con:
        out = export(con, start, end, batch_id="dashboard-only")
    print(f"[dashboard] 已生成：{out}")


if __name__ == "__main__":
    main()
