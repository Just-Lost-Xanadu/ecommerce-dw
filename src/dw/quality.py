"""数据质量：规则定义、执行、报告与**下游阻断**。

这是"合格"与"不合格"最常被区分的分水岭——
只写 ETL 不做质量校验的项目，面试官会认为你只是会写 SQL。

三条设计原则：
1. **每条规则都有真靶子**：造数脚本注入了 9 类脏数据，规则必须能抓到它们。
   只写规则不验证规则，等于没写（`tests/test_quality.py` 把这件事锁住）。
   ⚠️ 覆盖缺口如实说明：**late_arrival（迟到日志）目前没有任何规则负责**——
   它们的日期落在统计窗口之外，ODS 按窗口加载，根本进不了数仓，
   仓库内的规则看不到；只在源文件层面由测试守着。
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
    skipped: bool = False           # 该规则对本窗口不适用（如只有 1 天，算不了日环比）

    @property
    def status_cn(self) -> str:
        if self.skipped:
            return "不适用"
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

    # ---- 3. 值域：不允许出现「既被判为有效、金额又非正」的行 ----
    #
    # 旧口径是 `count(*) FILTER (WHERE amount > 0) / count(*) ... AND is_valid`，
    # 而 DWD 的 is_valid 定义就是 `reject_reason IS NULL`，reject_reason 里又含
    # `WHEN amount <= 0 THEN 'amount_not_positive'` —— **is_valid 已经蕴含
    # amount > 0**，所以那个比值恒等于 1.0。一条永远不会失败的阻断级规则，
    # 等于没有这条规则（这正是本项目最该避免的那类假安全感）。
    #
    # 新口径直接检查 DWD 的**判定结论**与金额是否自洽：
    #     合规行 = NOT (is_valid AND 金额非正)
    # 这样一旦 sql/dwd_order_detail.sql 的 CASE 分支被改坏，它立刻变红：
    #   · 把 `amount <= 0` 写成 `amount < 0` → amount = 0 的行混成有效行；
    #   · 整个金额分支被删掉 → 负数金额的行变成有效行进入 GMV。
    # 注意它**不把「被正确剔除的负数行」算成违规**——那些行 is_valid = false，
    # 属于清洗正常的产物，不是这条规则要报的问题。
    add(
        "dwd", "positive_amount", "dwd_order_detail",
        _scalar(con, f"""
            WITH d AS (
                SELECT count(*) AS n,
                       count(*) FILTER (
                           WHERE NOT (is_valid AND (amount IS NULL OR amount <= 0))
                       ) AS ok
                FROM dwd_order_detail
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
            )
            SELECT CASE WHEN n > 0 THEN ok * 1.0 / n ELSE 1.0 END, n - ok FROM d
        """),
        th["value_range_rate"], True,
        "没有任何一行可以既被判为有效、金额又非正（is_valid 与 amount 必须自洽）。旧口径在 is_valid 行上算 amount > 0，是恒真表达式，已改为一致性校验",
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
        f"孤儿商品在 ADS 归入「未知类目」，不会让 GMV 静默消失——"
        f"所以这是**可控的告警项而非阻断项**；孤儿率上限 "
        f"{th['referential_dwd_rate']:.2%}（越小越好）",
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
        f"源侧孤儿率上限 {th['referential_ods_rate']:.2%}（越小越好）。"
        "⚠️ 注意：孤儿行在 DWD **不会**被剔除（它的 is_valid 仍为 true，"
        "只是关联不上维表），所以本规则的孤儿数与 orphan_sku_rate_dwd "
        "**当前恒等**，两者的差值暂无信息量；等 DWD 开始剔除孤儿行，"
        "它才会变得有意义",
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
    #
    # ⚠️ **这条规则在当前数据模型下恒真**，如实说明原因：
    #   sql/ods_user_log.sql 的加载语句是
    #       WHERE TRY_CAST(log_date AS DATE) = DATE {dt}
    #   且 dt := TRY_CAST(log_date AS DATE) —— 也就是说 **分区键 dt 就是由
    #   log_date 派生出来的**，两者在结构上不可能不一致；再按 `dt BETWEEN 窗口`
    #   过滤，log_date 必然落在窗口内，比值恒为 1.0（实测影响行数 0）。
    #
    #   保留它的理由：它是一个**接入层变更后的守卫**。真实数仓里 ODS 常常
    #   按「文件所在的分区目录名」落 dt、事件日期另取一列，那时两者就可能错位
    #   （分区标着 09-25、事件日期却是 10-03），这条规则会立刻生效。
    #   要让它今天就有意义，ODS 必须同时保留「分区日期」和「事件日期」两个字段。
    #
    #   另外，迟到数据（late_arrival 注入的 600 行）落在窗口**之外**的分区文件里，
    #   ODS 按窗口加载，它们根本进不来——这是同一类"校验点选错位置"的盲区，
    #   由 tests/test_quality.py::test_late_arrival_stays_out_of_window 在
    #   源文件层面守着。
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
        "ODS 的 dt 现在落文件分区日期，log_date 才是业务日期；"
        "本规则检查分区内是否混入窗口外的业务日期（迟到/错放）",
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

    # ---- 9. 波动监控：逐日 GMV 的环比 ----
    #
    # 口径：actual = **超限的天数**（单日 GMV 环比超过 ±gmv_volatility），
    #       threshold = 允许的超限天数上限 gmv_volatility_days_max（默认 0），
    #       方向「越小越好」。默认语义就是注释里那句话：
    #       **任何一天环比超过 ±50% 都要人工确认**。
    #
    # 旧写法有两个问题：
    #   ① 把 actual 写成合规率 `1 - bad/tot`、阈值写成 `1 - 0.5 = 0.5`，
    #      含义变成"超限天数不超过一半就算通过"，比注释松得多；
    #   ② 窗口只有 1 天时（`run --limit 1`）没有可比的"前一天"，tot = 0，
    #      旧写法直接返回 1.0「通过」——一条在日批模式下完全失效的监控。
    #      现在这种情况显式标为 **skipped（不适用）**，不再伪装成通过。
    #
    # 还有一个**结构性问题**顺手修掉了：质量门禁跑在 dws **之前**
    # （config.LAYERS 里 quality 在 dws 前，这是刻意的——脏数据不该流进指标层），
    # 所以这条规则**不可能**读到本轮刚算出来的 dws_trade_user_1d：
    # 首次跑批时它读的是空表（旧写法恒返回 1.0 通过），
    # 后续跑批时读的是**上一轮的旧数据**——一个永远慢一拍、
    # 却看起来一直在工作的监控。改成直接读 DWD 明细后，
    # 它算的就是本轮的真实数字（与 dws_trade_user_1d 的 pay_amount 同口径，
    # 只是没做逐用户的 ROUND）。
    bad_days, comparable_pairs = con.execute(f"""
        WITH days AS (
            -- 从日历表补齐窗口内每一天：旧写法只取"有 DWD 行的日期"，
            -- 缺分区/某天 0 单时那一天会从 pairs 里消失，GMV 从正常掉到 0
            -- 反而不会被发现。
            SELECT date_key AS dt
            FROM dim_date
            WHERE date_key BETWEEN DATE '{start}' AND DATE '{end}'
        ), daily AS (
            SELECT dt, sum(amount) AS gmv
            FROM dwd_order_detail
            WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
              AND is_valid AND is_paid
            GROUP BY dt
        ), filled AS (
            SELECT d.dt, coalesce(x.gmv, 0) AS gmv
            FROM days d LEFT JOIN daily x USING (dt)
        ), pairs AS (
            SELECT dt, gmv, lag(gmv) OVER (ORDER BY dt) AS prev FROM filled
        )
        SELECT
            count(*) FILTER (
                WHERE prev IS NOT NULL AND (
                    (prev = 0 AND gmv > 0)
                    OR (prev > 0
                        AND abs(gmv - prev) * 1.0 / prev > {th['gmv_volatility']})
                )
            ) AS bad_days,
            count(*) FILTER (WHERE prev IS NOT NULL) AS comparable_pairs
        FROM pairs
    """).fetchone()
    bad_days = int(bad_days or 0)
    comparable_pairs = int(comparable_pairs or 0)
    if comparable_pairs == 0:
        results.append(
            RuleResult(
                layer="dwd", rule="gmv_day_over_day", table="dwd_order_detail",
                passed=True, actual=0.0,
                threshold=float(th["gmv_volatility_days_max"]),
                affected_rows=0, blocking=False,
                detail="窗口内没有可比的前一日（只有 1 天或数据不足），本规则不适用",
                lower_is_better=True, skipped=True,
            )
        )
    else:
        results.append(
            RuleResult(
                layer="dwd", rule="gmv_day_over_day", table="dwd_order_detail",
                passed=bad_days <= th["gmv_volatility_days_max"],
                actual=float(bad_days),
                threshold=float(th["gmv_volatility_days_max"]),
                affected_rows=bad_days, blocking=False,
                detail=f"GMV 单日环比超过 ±{th['gmv_volatility']:.0%} 的天数"
                       f"（共 {comparable_pairs} 个可比日）；"
                       "超限即需人工确认（大促、事故、口径变更）",
                lower_is_better=True,
            )
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
    # 结算日以造数时写入的 _baseline.json 为准，而不是"本轮窗口最后一天"：
    # 回补一个较早的窗口时，窗口之后、但尚未到真正结算日的分区是合法数据，
    # 用 dates[-1] 会把它们误报成"超前"。
    try:
        settle = date.fromisoformat(paths.load_baseline().get("settle_date") or "")
    except (OSError, ValueError, TypeError):
        settle = dates[-1]
    ahead = _source_days_ahead(settle)
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

    # ---- 12. 漏斗单调性：后一阶段的人数不得超过前一阶段 ----
    #
    # 为什么需要它：漏斗的"加购→下单"转化率**可能大于 100%**。
    # 实测本项目的造数数据（2026-10-21）：浏览 19,730 → 加购 10,355 →
    # **下单 10,428**，下单人数比加购还多。根因是造数时每个事件独立随机抽取
    # （gen_source.FUNNEL_EVENTS），用户可以只 buy 不 cart，不构成行为序列。
    #
    # 这条规则**在当前数据上就会失败**，这正是它的价值——它是一条"活靶子"，
    # 证明规则真的会红（对比那几条恒真的规则，它们永远给不出这个信号）。
    # 它是告警级（blocking=False）：漏斗不单调说明口径或造数需要修，
    # 但不该让整条链路停下来（否则就变成"门禁天天拦"，告警会被无视）。
    add(
        "dwd", "funnel_stage_monotonic", "dwd_page_log",
        _scalar(con, f"""
            WITH per_day AS (
                SELECT
                    dt,
                    count(DISTINCT user_id) AS view_users,
                    count(DISTINCT user_id) FILTER (WHERE event = 'cart') AS cart_users,
                    count(DISTINCT user_id) FILTER (WHERE event = 'buy')  AS buy_users
                FROM dwd_page_log
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
                GROUP BY dt
            )
            SELECT
                CASE WHEN count(*) > 0
                     THEN count(*) FILTER (WHERE cart_users <= view_users
                                             AND buy_users <= cart_users)
                          * 1.0 / count(*)
                     ELSE 1.0 END,
                count(*) FILTER (WHERE cart_users > view_users
                                   OR buy_users > cart_users)
            FROM per_day
        """),
        1.0, False,
        "漏斗各阶段人数必须单调不增（浏览 ≥ 加购 ≥ 下单）。"
        "当前造数数据里会出现 buy > cart（转化率 > 100%），"
        "说明事件是独立随机的、不构成行为序列——这是数据口径问题，不是链路 bug",
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
        (
            f"{'规则':<30}{'层':<6}{'结果':<7}{'实际':>10}{'方向':>4}{'阈值':>10}"
            f"{'影响行数':>10}  阻断"
        ),
        "-" * 96,
    ]
    for r in results:
        lines.append(
            f"{r.rule:<30}{r.layer:<6}{r.status_cn:<7}"
            f"{r.actual:>10.4f}{r.direction:>4}{r.threshold:>10.4f}"
            f"{r.affected_rows:>10,}"
            f"  {'是' if r.blocking else '否'}"
        )
    return "\n".join(lines)
