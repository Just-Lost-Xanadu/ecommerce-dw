"""对账与倾斜量化：把两个技术难点的**证据**算出来。

两个检查项：
1. **对账（数据不准）**：报表口径 GMV 与"精确业务口径"基准的差异率。
   同时给出**清洗前**（ODS 原始口径，含重复单与退款单）与**清洗后**（DWD 收口口径）两个数字，
   这样"差异率从 X% 降到 Y%"就是可复现的实测结论，而不是编出来的。
2. **倾斜量化**：热销 SKU 的订单集中度与 DWS 聚合的分组规模分布——
   用来在跑 Spark 对照实验之前，先证明"这份数据确实有明显的 key 倾斜"。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import duckdb


@dataclass
class ReconResult:
    baseline_gmv: float
    raw_gmv: float          # 源数据直接汇总（含重复单、含退款）
    dwd_gmv: float          # DWD 收口后（去重 + 有效 + 已支付）
    raw_diff_pct: float
    dwd_diff_pct: float
    baseline_orders: int
    raw_orders: int
    dwd_orders: int

    def format(self) -> str:
        return "\n".join([
            "对账结果（基准 = 造数时的精确业务口径：唯一订单且已支付）",
            f"  基准 GMV            {self.baseline_gmv:,.2f}  ({self.baseline_orders:,} 单)",
            (
                f"  清洗前（源表直算）   {self.raw_gmv:,.2f}  "
                f"({self.raw_orders:,} 行)"
                f"   差异 {self.raw_diff_pct:+.3f}%"
            ),
            (
                f"  清洗后（DWD 收口）   {self.dwd_gmv:,.2f}  "
                f"({self.dwd_orders:,} 单)"
                f"   差异 {self.dwd_diff_pct:+.3f}%"
            ),
            f"  → 收口把差异率从 {abs(self.raw_diff_pct):.3f}% 降到 {abs(self.dwd_diff_pct):.3f}%",
        ])


def reconcile(
    con: duckdb.DuckDBPyConnection, baseline_gmv: float, baseline_orders: int,
    window_start: date, window_end: date,
) -> ReconResult:
    start, end = window_start.isoformat(), window_end.isoformat()

    raw = con.execute(f"""
        SELECT coalesce(sum(amount), 0), count(*)
        FROM ods_order_info
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
    """).fetchone()

    dwd = con.execute(f"""
        SELECT coalesce(sum(amount), 0), count(*)
        FROM dwd_order_detail
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
          AND is_valid AND is_paid
    """).fetchone()

    def pct(v: float) -> float:
        return ((v - baseline_gmv) / baseline_gmv * 100) if baseline_gmv else 0.0

    return ReconResult(
        baseline_gmv=round(baseline_gmv, 2),
        raw_gmv=round(float(raw[0]), 2),
        dwd_gmv=round(float(dwd[0]), 2),
        raw_diff_pct=round(pct(float(raw[0])), 4),
        dwd_diff_pct=round(pct(float(dwd[0])), 4),
        baseline_orders=baseline_orders,
        raw_orders=int(raw[1]),
        dwd_orders=int(dwd[1]),
    )


@dataclass
class SkewResult:
    total_rows: int
    hot_sku: str
    hot_sku_rows: int
    hot_sku_share: float
    n_groups: int
    max_group_rows: int
    p50_group_rows: int
    max_over_p50: float

    def format(self) -> str:
        return "\n".join([
            "倾斜量化（按 user_id 聚合时的分组规模分布）",
            f"  参与聚合的订单行数    {self.total_rows:,}",
            (
                f"  热销 SKU {self.hot_sku}   {self.hot_sku_rows:,} 行"
                f"（占比 {self.hot_sku_share:.2%}）"
            ),
            f"  聚合分组数            {self.n_groups:,}",
            f"  最大分组行数          {self.max_group_rows:,}",
            f"  分组中位数            {self.p50_group_rows:,}",
            (
                f"  最大 / 中位数         {self.max_over_p50:.1f}x"
                "   ← 倍数越大，单个 task 越可能成为长尾"
            ),
            "  → 这就是 Spark 里某个 task 明显比同 stage 其他 task 慢的成因；",
            "    对照实验见 spark/skew_experiment.py",
        ])


def skew_stats(
    con: duckdb.DuckDBPyConnection, hot_sku: str,
    window_start: date, window_end: date,
) -> SkewResult:
    start, end = window_start.isoformat(), window_end.isoformat()

    total, hot, groups, mx, p50 = con.execute(f"""
        WITH base AS (
            SELECT user_id FROM dwd_order_detail
            WHERE dt BETWEEN DATE '{start}' AND DATE '{end}' AND is_valid AND is_paid
        ), g AS (
            SELECT user_id, count(*) AS n FROM base GROUP BY user_id
        )
        SELECT
            (SELECT count(*) FROM base),
            (SELECT count(*) FROM dwd_order_detail
             WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
               AND is_valid AND is_paid AND sku_id = '{hot_sku}'),
            (SELECT count(*) FROM g),
            (SELECT coalesce(max(n), 0) FROM g),
            (SELECT coalesce(cast(median(n) AS BIGINT), 0) FROM g)
    """).fetchone()

    total_i = int(total or 0)
    return SkewResult(
        total_rows=total_i,
        hot_sku=hot_sku,
        hot_sku_rows=int(hot or 0),
        hot_sku_share=(int(hot or 0) / total_i) if total_i else 0.0,
        n_groups=int(groups or 0),
        max_group_rows=int(mx or 0),
        p50_group_rows=int(p50 or 0),
        max_over_p50=(int(mx) / int(p50)) if p50 else 0.0,
    )
