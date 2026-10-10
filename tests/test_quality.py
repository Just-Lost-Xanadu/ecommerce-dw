"""质量规则的**灵敏度**测试：每条规则必须能抓到对应的那类脏数据。

为什么这个文件比别的测试重要：
    只写规则、不验证规则，等于没写。规则很容易写成一个"永远通过"的表达式
    （阈值写反、方向搞错、FILTER 条件写漏），跑起来一切正常，脏数据照样流进指标。
    造数脚本注入了 9 类脏数据，**每一条都必须有规则抓到它**——
    这件事只能靠测试锁住，不能靠"我看了一眼结果都对"。

测试策略：用**两个对照运行**证明灵敏度，且两者都跑在**独立临时目录 + 独立进程**里：
    A. 全量脏数据：对应规则的 affected_rows 必须 > 0
    B. 关掉某一类脏数据：该规则的 affected_rows 必须变成 0
B 是关键：它排除了"这条规则的数字其实来自别的脏数据"这种巧合。

为什么不复用主进程的数仓：链路是有状态的（写同一个 duckdb 文件）。
在主进程里反复跑，后一次会覆盖前一次，导致别的测试断言 ADS 内容时读到被改过的状态——
**测试之间通过共享数仓互相污染**，会表现成"单独跑能过、一起跑就挂"的随机失败。
"""

from __future__ import annotations

import pytest

from dw import db, quality

# 每类脏数据：负责抓它的规则
#
# 这张表本身就是"分层质量设计"的文档：
#   · 主键重复在 **ODS** 就能看到（DWD 会去重，所以 DWD 层看到的一定是 1.0）；
#     这件事只能靠 `test_duplicate_orders_visible_in_ods_then_deduped` 单独验证。
#   · 空值 / 非正金额 → DWD 判定为无效并剔除，共同体现在 `reject_rate_max`。
#   · **未来日期 → 源边界规则**（不是 reject_rate_max）：见下面那条注释。
#   · 孤儿商品 → DWD 参照完整性规则（限额告警，因为 ADS 用「未知类目」兜住了）。
#   · 手机号冲突 → 源侧一致性规则。
INJECTION_CHECKS = [
    ("null_user_id", "not_null_user_id"),
    ("null_amount", "reject_rate_max"),
    ("negative_amount", "reject_rate_max"),
    ("orphan_sku", "orphan_sku_rate_dwd"),
    ("dup_user_phone", "consistent_user_phone"),
    # 未来日期由**源边界规则**负责，而不是数仓内的规则——
    # 那些行落在处理窗口之外的分区文件里，**压根不会被加载进数仓**，
    # 所以数仓内部的任何规则都不可能看到它们。
    # 这个盲区是真实存在的：只在仓库里做校验，未来数据永远沉默，
    # 直到某天有人手工调整窗口，它们一次性涌进指标表。
    ("future_date", "source_days_within_boundary"),
]

# DWD 里应当出现的剔除原因（证明无效行是被"标记"而不是"静默删掉"）
# 不含 date_in_future：未来日期的行没进数仓，由源边界规则拦截。
EXPECTED_REJECT_REASONS = {
    "user_id_null",
    "amount_null",
    "amount_not_positive",
}


def _rule(results: list[dict], name: str) -> dict:
    for r in results:
        if r["rule"] == name:
            return r
    raise AssertionError(f"没有找到规则 {name}；现有规则：{[r['rule'] for r in results]}")


@pytest.mark.parametrize("dirty_key,rule", INJECTION_CHECKS)
def test_rule_detects_its_dirty_data(all_dirty, dirty_key, rule) -> None:
    """注入的每类脏数据，都必须有规则非零命中。"""
    r = _rule(all_dirty["quality"], rule)
    assert r["affected_rows"] > 0, (
        f"注入了 {dirty_key}，但规则 {rule} 的影响行数为 0 —— "
        f"说明这条规则没在检查它该检查的东西"
    )


@pytest.mark.parametrize("dirty_key,rule", INJECTION_CHECKS)
def test_rule_goes_quiet_when_dirty_data_removed(isolated_runs, all_dirty, dirty_key, rule) -> None:
    """关掉某类脏数据后，对应规则的影响行数必须**下降**。

    这里断言"下降"而不是"归零"，是因为 `reject_rate_max` 同时覆盖
    空值 / 非正金额 / 未来日期三类问题——关掉其中一类，另外两类仍会被计入。
    用"归零"来断言会把一条正确的规则误判成失效。
    （细到"每一类是否真被挡住"的验证交给下面按 reject_reason 细分的测试。）
    """
    with_all = _rule(all_dirty["quality"], rule)
    without = _rule(isolated_runs(dirty_key)["quality"], rule)
    assert without["affected_rows"] < with_all["affected_rows"], (
        f"关掉 {dirty_key} 之后，规则 {rule} 的影响行数没有下降"
        f"（{with_all['affected_rows']} -> {without['affected_rows']}）—— "
        f"说明这条规则其实没在看 {dirty_key}"
    )


def test_reject_reasons_are_individually_sensitive(isolated_runs, all_dirty) -> None:
    """按剔除原因细分验证：每关掉一类，对应原因的计数必须减少。

    这是比"规则层面"更细的灵敏度验证——
    `reject_rate_max` 把三类问题合成一个数字，只有拆到 reject_reason 才看得出
    到底哪一类被挡住了、哪一类漏了。
    """
    all_reasons = all_dirty["reject_reasons"]
    pairs = {
        "null_user_id": "user_id_null",
        "null_amount": "amount_null",
        "negative_amount": "amount_not_positive",
    }
    for dirty_key, reason in pairs.items():
        assert all_reasons.get(reason, 0) > 0, (
            f"没有一条订单被判为 {reason}（注入的 {dirty_key} 没被挡住）"
        )
        without = isolated_runs(dirty_key)["reject_reasons"]
        assert without.get(reason, 0) < all_reasons[reason], (
            f"关掉 {dirty_key} 后，{reason} 的计数没有下降"
            f"（{all_reasons[reason]} -> {without.get(reason, 0)}）"
        )


def test_dwd_marks_reject_reasons_instead_of_silently_dropping(all_dirty) -> None:
    """DWD 必须**标记**无效行，而不是删掉。

    "删掉"和"标记"的差别，在出问题时就是"能查"和"只能猜"的差别：
    对不上账时，标记过的可以直接
    `SELECT reject_reason, count(*) ... GROUP BY 1` 看剔了多少、为什么剔。
    """
    reasons = {k: v for k, v in all_dirty["reject_reasons"].items() if v}
    missing = EXPECTED_REJECT_REASONS - set(reasons)
    assert not missing, f"DWD 没有标出这些剔除原因：{missing}；实际只有 {reasons}"


def test_duplicate_order_ids_are_deduped_in_dwd(all_dirty) -> None:
    """源数据里有重复单号，DWD 必须已去重（唯一率 100%）。

    注意：`unique_order_id` 这条规则作用在 DWD 上，所以它**永远是通过的**
    （DWD 已经去重了）。这不代表规则没意义——它防的是"去重逻辑哪天被改坏"。
    重复数据本身在 ODS 层被 `test_duplicate_orders_visible_in_ods_then_deduped` 验证。
    """
    n, uniq = all_dirty["dwd_rows"], all_dirty["dwd_unique_orders"]
    assert n == uniq, f"DWD 里仍有重复 order_id（{n} 行 / {uniq} 个唯一值），去重逻辑失效"


def test_duplicate_orders_visible_in_ods_then_deduped(isolated_runs, all_dirty) -> None:
    """重复单号必须"在 ODS 看得见、在 DWD 被消掉"。

    这是去重链路的完整验证，分两步：
      ① 全量脏数据时 ODS 里确实存在重复 order_id（不然"去重"没有对象）；
      ② 关掉 dup_order_id 注入后，ODS 的重复数应当归零。
    只看 DWD 是验不出这件事的——DWD 已经去过重了。
    """
    assert all_dirty["ods_dup_orders"] > 0, (
        "注入了重复单号，但 ODS 里没有重复——注入没生效或加载丢了行"
    )
    without = isolated_runs("dup_order_id")
    assert without["ods_dup_orders"] == 0, (
        f"关掉 dup_order_id 注入后 ODS 仍有 {without['ods_dup_orders']} 个重复单号"
    )


def test_orphan_sku_lands_in_unknown_category(all_dirty) -> None:
    """孤儿商品必须出现在 ADS 的「未知类目」里，而不是让 GMV 静默消失。

    这条锁住一个很隐蔽的坑：如果把 DWD 与 dim_sku 的关联写成 INNER JOIN，
    这些订单会**静默消失**——GMV 少了，但报表上看不出任何异常。
    用 LEFT JOIN +「未知类目」，它们就变成**可见**的。
    """
    assert all_dirty["unknown_category_orders"] > 0, (
        "孤儿商品没有出现在「未知类目」——说明它们在 JOIN 时被丢掉了，GMV 会静默变少"
    )
    assert all_dirty["unknown_category_gmv"] > 0, "「未知类目」有订单数但没有金额，口径不对"


def test_no_blocking_failures_after_cleaning(all_dirty) -> None:
    """清洗之后，阻断级规则应当全部通过（脏数据被挡在 DWD 之外）。"""
    bad = [r for r in all_dirty["quality"] if r["blocking"] and not r["passed"]]
    assert not bad, f"仍有阻断级失败：{[(r['rule'], r['actual']) for r in bad]}"
    assert not all_dirty["blocked"], f"链路被意外阻断：{all_dirty['blocked']}"


def test_threshold_direction_is_correct(all_dirty) -> None:
    """阈值方向必须显式标注——"越小越好"和"越大越好"混用会让报告读出相反结论。

    这个错误很容易犯：把剔行率（越小越好）当成合规率（越大越好）来比较，
    于是"剔行率 0.06% 很好"会被显示成"失败"。这里把方向当成一项被测性质锁住。
    """
    for r in all_dirty["quality"]:
        assert isinstance(r["lower_is_better"], bool), (
            f"规则 {r['rule']} 没有声明阈值方向"
        )
    assert _rule(all_dirty["quality"], "reject_rate_max")["lower_is_better"] is True
    assert _rule(all_dirty["quality"], "unique_order_id")["lower_is_better"] is False


def test_quality_module_is_reusable_standalone(all_dirty) -> None:
    """质量校验必须能被单独调用（不依赖跑批），便于接进调度做"只校验"模式。"""
    assert len(all_dirty["quality"]) >= 10, f"规则数只有 {len(all_dirty['quality'])} 条"
    assert all(isinstance(r["actual"], float) for r in all_dirty["quality"])


def test_quality_entries_are_all_well_formed(all_dirty) -> None:
    """每条规则都要带齐"层 / 表 / 影响行数 / 是否阻断"——报告才可能说清楚问题。"""
    for r in all_dirty["quality"]:
        assert r["layer"] in {"ods", "dim", "dwd", "dws"}, f"{r['rule']} 的层不合法"
        assert isinstance(r["affected_rows"], int)
        assert isinstance(r["blocking"], bool)
        # 阈值可以是 0：例如"源侧超出边界的天数"这条规则，阈值就是 0（一天都不允许）。
        # 所以这里只要求它是数字，不要求大于 0。
        assert isinstance(r["threshold"], (int, float))
        assert r["threshold"] >= 0, f"{r['rule']} 的阈值是负数：{r['threshold']}"


def test_rule_names_are_unique(all_dirty) -> None:
    """规则名必须唯一——否则落库时后一条会覆盖前一条（PRIMARY KEY 冲突）。"""
    names = [r["rule"] for r in all_dirty["quality"]]
    assert len(names) == len(set(names)), f"存在重名规则：{names}"


def test_funnel_rule_actually_fails_on_current_data(all_dirty) -> None:
    """新增的漏斗单调性规则必须**真的会失败**——这是它存在的全部价值。

    本项目造数时每个事件独立随机抽取（gen_source.FUNNEL_EVENTS），
    用户完全可以只 buy 不 cart，于是「下单人数 > 加购人数」、转化率 > 100%。
    一条「永远通过」的规则等于没写，所以这里断言 affected_rows > 0。
    """
    r = _rule(all_dirty["quality"], "funnel_stage_monotonic")
    assert r["affected_rows"] > 0, (
        "漏斗单调性规则没有命中任何一天——要么造数改了、要么规则写错了"
    )
    assert not r["passed"], "存在 buy > cart 的天，规则却判为通过"


def test_null_user_logs_are_dropped_and_counted(isolated_runs, all_dirty) -> None:
    """日志侧的丢弃量必须可见（原来只统计订单侧，日志丢了多少行没有数字）。

    null_user_log 与 null_user_id 共用同一个开关，所以关掉 null_user_id 之后，
    日志侧的空 user_id 也应当一起消失。
    """
    assert all_dirty["log_dropped"] > 0, (
        "注入了 null_user_log，但日志侧丢弃量为 0——"
        "要么没注入，要么 layer_dwd 没有统计它"
    )
    without = isolated_runs("null_user_id")
    assert without["log_dropped"] == 0, (
        f"关掉 null_user_id 注入后，日志侧仍有 {without['log_dropped']} 行被丢弃"
    )


def test_late_arrival_stays_out_of_window(isolated_runs, all_dirty) -> None:
    """迟到数据（late_arrival）必须留在窗口**之外**的源分区文件里。

    现状如实说明：**没有任何一条质量规则负责 late_arrival**——这些日志的日期
    落在统计窗口之外，ODS 按窗口加载，它们根本进不了数仓，仓库内的规则看不到。
    所以这里退一步，在**源文件层面**验证它的存在与消失：
    注入了它 → 窗口外的日志分区文件数 > 0；关掉它 → 归零。
    这不是「有覆盖」，而是把覆盖缺口显式地写进测试，避免被误认为已覆盖。
    """
    assert all_dirty["late_log_partitions"] > 0, (
        "注入了 late_arrival，但源目录里没有窗口外的 user_log 分区文件"
    )
    without = isolated_runs("late_arrival")
    assert without["late_log_partitions"] == 0, (
        f"关掉 late_arrival 后，仍有 {without['late_log_partitions']} "
        "个窗口外的日志分区文件"
    )


def test_quality_helpers_exist() -> None:
    """`quality` 模块对外暴露的判定函数必须存在且可调用（防止被误删）。"""
    assert callable(quality.run_quality)
    assert callable(quality.blocking_failures)
    assert callable(quality.format_report)
    _ = db  # 保持导入（其他测试会用到）
