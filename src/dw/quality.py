"""数据质量：规则定义、执行、报告与**下游阻断**。

这是"合格"与"不合格"最常被区分的分水岭——
只写 ETL 不做质量校验的项目，面试官会认为你只是会写 SQL。

三条设计原则：
1. **每条规则都有真靶子**：造数脚本注入了 9 类脏数据，规则必须能抓到它们。
   只写规则不验证规则，等于没写（`tests/test_quality.py` 把这件事锁住）。
2. **每条规则只报实际触发的量**：不要笼统地说"数据有问题"，
   要说清"哪条规则、实际值多少、阈值多少、影响多少行"。
3. **阻断要分级**：主键重复、参照完整性断裂这类问题必须阻断下游；
   非空率轻微不达标可以先放行并告警——否则一有小瑕疵整条链路就停，
   运维会很快学会忽略告警，质量门禁就形同虚设。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import duckdb

from dw import config, paths


@dataclass(frozen=True)
class RuleResult:
    layer: str
    rule: str
    table: str
    passed: bool
    actual: float
    threshold: float
    affected_rows: int
    blocking: bool
    detail: str
    lower_is_better: bool = False   # 有些指标是"越小越好"（剔行率、孤儿率），展示方向要反向

    @property
    def status_cn(self) -> str:
        return "通过" if self.passed else "失败"

    @property
    def direction(self) -> str:
        return "≤" if self.lower_is_better else "≥"


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[float, int]:
    """执行"返回 (比率, 影响行数)"的查询，缺失时按 (1.0, 0) 处理（无数据 = 无违规）。"""
    row = con.execute(sql).fetchone()
    if not row or row[0] is None:
        return 1.0, 0
    return float(row[0]), int(row[1] or 0)


def _source_days_ahead(settle: date) -> int:
    """数一数源分区文件里有多少个日期超出了结算日。

    在**文件系统层面**检查，而不是查数仓：这些数据根本没被加载进来，
    数仓里查不到（见规则 11 的说明）。
    """
    prefix = "order_info"
    pattern = f"{prefix}_*.csv"
    ahead = 0
    for path in paths.raw_dir().glob(pattern):
        stamp = path.stem[len(prefix) + 1:]
        try:
            if date.fromisoformat(stamp) > settle:
                ahead += 1
        except ValueError:
            continue
    return ahead


def run_quality(
    con: duckdb.DuckDBPyConnection,
    dates: list[date],
) -> list[RuleResult]:
    """在 ODS/DWD/DIM/DWS 上执行全部质量规则。

    规则覆盖四类：**完整性、唯一性、参照完整性、值域/一致性/时效性**。
    每条规则返回 (合规率, 影响行数)，与阈值比较得出是否通过。
    """
    start, end = dates[0].isoformat(), dates[-1].isoformat()
    th = config.QUALITY_THRESHOLDS
    window_start = dates[0].isoformat()
    user_src = paths.raw_csv(config.SRC_USER_INFO).as_posix()
    results: list[RuleResult] = []

    def add(
        layer: str, rule: str, table: str, metric: tuple[float, int],
        threshold: float, blocking: bool, detail: str,
        lower_is_better: bool = False,
    ) -> None:
        actual, affected = metric
        passed = (actual <= threshold + 1e-9) if lower_is_better else (actual >= threshold - 1e-9)
        results.append(
            RuleResult(
                layer=layer, rule=rule, table=table,
                passed=passed,
                actual=round(actual, 6), threshold=threshold,
                affected_rows=affected, blocking=blocking, detail=detail,
                lower_is_better=lower_is_better,
            )
        )

    # ---- 1. 唯一性：订单号不重复（DWD 已去重，这里校验结论）----
    add(
        "dwd", "unique_order_id", "dwd_order_detail",
        _scalar(con, f"""
            WITH d AS (
                SELECT count(*) AS n, count(DISTINCT order_id) AS u
                FROM dwd_order_detail
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
            )
            SELECT CASE WHEN n > 0 THEN u * 1.0 / n ELSE 1.0 END, n - u FROM d
        """),
        th["unique_rate"], True,
        "订单号必须唯一（DWD 已按 order_id 去重，此处验证去重结论）",
    )

    # ---- 2. 非空率：交易事实的 user_id ----
    add(
        "dwd", "not_null_user_id", "dwd_order_detail",
        _scalar(con, f"""
            WITH d AS (
                SELECT count(*) AS n,
                       count(*) FILTER (WHERE user_id IS NOT NULL) AS ok
                FROM dwd_order_detail
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
            )
            SELECT CASE WHEN n > 0 THEN ok * 1.0 / n ELSE 1.0 END, n - ok FROM d
        """),
        th["not_null_rate"], True,
        "有效订单必须有 user_id，否则无法归属到人",
    )

    # ---- 3. 值域：有效订单金额必须为正 ----
    add(
        "dwd", "positive_amount", "dwd_order_detail",
        _scalar(con, f"""
            WITH d AS (
                SELECT count(*) AS n,
                       count(*) FILTER (WHERE amount > 0) AS ok
                FROM dwd_order_detail
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}' AND is_valid
            )
            SELECT CASE WHEN n > 0 THEN ok * 1.0 / n ELSE 1.0 END, n - ok FROM d
        """),
        th["value_range_rate"], True,
        "进入指标的订单金额必须为正数",
    )

    # ---- 4. 参照完整性（DWD 层）：有效订单的孤儿商品率 ----
    add(
        "dwd", "orphan_sku_rate_dwd", "dwd_order_detail",
        _scalar(con, f"""
            WITH d AS (
                SELECT count(*) AS n,
                       count(*) FILTER (WHERE s.sku_id IS NULL) AS orphan
                FROM dwd_order_detail o
                LEFT JOIN dim_sku s ON s.sku_id = o.sku_id
                WHERE o.dt BETWEEN DATE '{start}' AND DATE '{end}' AND o.is_valid
            )
            SELECT CASE WHEN n > 0 THEN orphan * 1.0 / n ELSE 0.0 END, orphan FROM d
        """),
        th["referential_dwd_rate"], False,
        "孤儿商品在 ADS 归入「未知类目」，不会让 GMV 静默消失——"
        "所以这是**可控的告警项而非阻断项**，限额监控即可",
        lower_is_better=True,
    )

    # ---- 5. 参照完整性（ODS 层）：源数据本身的孤儿率（对比用）----
    add(
        "ods", "orphan_sku_rate_ods", "ods_order_info",
        _scalar(con, f"""
            WITH d AS (
                SELECT count(*) AS n,
                       count(*) FILTER (WHERE s.sku_id IS NULL) AS orphan
                FROM ods_order_info o
                LEFT JOIN dim_sku s ON s.sku_id = o.sku_id
                WHERE o.dt BETWEEN DATE '{start}' AND DATE '{end}'
            )
            SELECT CASE WHEN n > 0 THEN orphan * 1.0 / n ELSE 0.0 END, orphan FROM d
        """),
        th["referential_ods_rate"], False,
        "源侧孤儿率用于对比：与 DWD 侧的差值说明清洗挡住了多少",
        lower_is_better=True,
    )

    # ---- 6. 跨表一致性：同一 user_id 的手机号必须唯一 ----
    add(
        "dim", "consistent_user_phone", "dim_user",
        _scalar(con, f"""
            WITH d AS (
                SELECT count(*) AS n, count(*) FILTER (WHERE pn = 1) AS ok
                FROM (
                    SELECT user_id, count(DISTINCT phone) AS pn
                    FROM read_csv('{user_src}', header=true,
                                  all_varchar=true, sample_size=-1)
                    WHERE user_id IS NOT NULL
                    GROUP BY user_id
                ) t
            )
            SELECT CASE WHEN n > 0 THEN ok * 1.0 / n ELSE 1.0 END, n - ok FROM d
        """),
        th["consistency_rate"], False,
        "源系统里同一用户多个手机号：保留一条并标记 is_conflict，不静默取第一条",
    )

    # ---- 7. 时效性：已加载分区里的事件日期必须落在统计窗口内 ----
    # 说明：造数时注入的"迟到数据"（日期落在窗口之外）**不会**被 ODS 加载进来——
    # 因为 ODS 是按 `dt` 分区加载的，那些行属于窗口外的分区。
    # 所以这条规则校验的是另一个更实际的问题：**加载进来的分区里有没有日期错位**
    # （分区标着 09-25、事件日期却是 10-03，这类错位在真实系统里很常见）。
    # 要监控"被挡在窗口外的迟到数据量"，看造数脚本的 late_arrival 注入数与对账报告。
    add(
        "ods", "event_date_in_window", "ods_user_log",
        _scalar(con, f"""
            WITH d AS (
                SELECT
                    count(*) AS total,
                    count(*) FILTER (
                        WHERE log_date < DATE '{window_start}' OR log_date > DATE '{end}'
                    ) AS out_of_window
                FROM ods_user_log
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
            )
            SELECT CASE WHEN total > 0 THEN 1.0 - out_of_window * 1.0 / total ELSE 1.0 END,
                   out_of_window FROM d
        """),
        th["timeliness_rate"], False,
        "分区日期与事件日期必须一致；不一致说明上游写入时错分了分区",
    )

    # ---- 8. 值域：行为事件的枚举合法 ----
    add(
        "dwd", "valid_event_enum", "dwd_page_log",
        _scalar(con, f"""
            WITH d AS (
                SELECT count(*) AS n,
                       count(*) FILTER (WHERE event IN ('pv','cart','fav','buy')) AS ok
                FROM dwd_page_log
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
            )
            SELECT CASE WHEN n > 0 THEN ok * 1.0 / n ELSE 1.0 END, n - ok FROM d
        """),
        th["value_range_rate"], True,
        "漏斗依赖 event 取值，出现新枚举必须先纳入口径",
    )

    # ---- 9. 波动监控：DWS 交易汇总的日环比 ----
    add(
        "dws", "gmv_day_over_day", "dws_trade_user_1d",
        _scalar(con, f"""
            WITH daily AS (
                SELECT dt, sum(pay_amount) AS gmv
                FROM dws_trade_user_1d
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
                GROUP BY dt
            ), pairs AS (
                SELECT dt, gmv, lag(gmv) OVER (ORDER BY dt) AS prev FROM daily
            ), bad AS (
                SELECT count(*) AS n
                FROM pairs
                WHERE prev IS NOT NULL AND prev > 0
                  AND abs(gmv - prev) * 1.0 / prev > {th['gmv_volatility']}
            ), tot AS (SELECT count(*) AS n FROM pairs WHERE prev IS NOT NULL)
            SELECT CASE WHEN (SELECT n FROM tot) > 0
                        THEN 1.0 - (SELECT n FROM bad) * 1.0 / (SELECT n FROM tot)
                        ELSE 1.0 END,
                   (SELECT n FROM bad)
        """),
        # 波动规则本身是"越小越好"的监控项，这里用"合规率"表达：日环比超限的天数占比要低
        1.0 - th["gmv_volatility"], False,
        "GMV 单日环比超过 ±50% 需要人工确认（大促、事故、口径变更）",
    )

    # ---- 10. 剔行率：DWD 把多少比例的数据判为无效（越小越好）----
    add(
        "dwd", "reject_rate_max", "dwd_order_detail",
        _scalar(con, f"""
            WITH d AS (
                SELECT count(*) AS n,
                       count(*) FILTER (WHERE is_valid) AS ok
                FROM dwd_order_detail
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
            )
            SELECT CASE WHEN n > 0 THEN (n - ok) * 1.0 / n ELSE 0.0 END, n - ok FROM d
        """),
        th["dwd_reject_rate_max"], False,
        f"DWD 剔行率上限 {th['dwd_reject_rate_max']:.0%}；超限说明源数据质量崩了，需要人工介入",
        lower_is_better=True,
    )

    # ---- 11. 时效性（源文件级）：源侧不得出现超出处理边界的日期 ----
    #
    # 这条规则补的是一个**真实的盲区**，值得单独说明：
    #   "未来日期"这类脏数据落在处理窗口之外，于是它的分区文件根本不会被加载进数仓——
    #   也就是说，**数仓内部的任何一条规则都不可能看到它**。
    #   如果只在数仓里做校验，这类问题会永远沉默，直到某天有人手工把窗口往前挪，
    #   未来数据一次性涌进指标表。
    #
    #   正确做法是把校验点前移到**源文件边界**：不检查"表里有什么"，
    #   而是检查"送进来的数据里有没有超出今天的东西"。
    #   这也是真实数据平台的做法（数据接入前的边界校验），
    #   因为很多问题在入口挡住比在仓库里发现便宜得多。
    ahead = _source_days_ahead(dates[-1])
    results.append(
        RuleResult(
            layer="ods",
            rule="source_days_within_boundary",
            table="data/raw/by_dt",
            passed=ahead <= th["max_source_days_ahead"],
            actual=float(ahead),
            threshold=float(th["max_source_days_ahead"]),
            affected_rows=ahead,
            blocking=False,
            detail="源分区文件里超出结算日的天数；必须在接入前处理，"
                   "否则这些数据会在某次窗口调整时突然涌进指标表",
            lower_is_better=True,
        )
    )

    return results


def blocking_failures(results: list[RuleResult]) -> list[RuleResult]:
    """返回需要阻断下游的失败规则。"""
    return [r for r in results if r.blocking and not r.passed]


def format_report(results: list[RuleResult]) -> str:
    """终端可读的质量报告。

    注意"实际"和"阈值"两列的语义随方向变化：
    合规率类规则是"实际 ≥ 阈值算通过"，比率类规则是"实际 ≤ 阈值算通过"，
    所以这里显式打出方向符，避免读的人搞反（这个坑自己也踩过一次）。
    """
    lines = [
        f"{'规则':<24}{'层':<5}{'结果':<5}{'实际':>10}{'方向':>4}{'阈值':>9}"
        f"{'影响行数':>10}  阻断",
        "-" * 86,
    ]
    for r in results:
        lines.append(
            f"{r.rule:<24}{r.layer:<5}{r.status_cn:<5}"
            f"{r.actual:>10.4f}{r.direction:>4}{r.threshold:>9.4f}"
            f"{r.affected_rows:>10,}"
            f"  {'是' if r.blocking else '否'}"
        )
    return "\n".join(lines)
