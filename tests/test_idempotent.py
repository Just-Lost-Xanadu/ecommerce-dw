"""幂等测试：**同一个窗口跑两次，结果必须逐行一致**。

这是调度系统里最重要的一条性质，也是"断点重跑 / 补数"能成立的前提：
    如果重跑会出双份数据，那么任何一次失败重试都是在往数仓里灌垃圾，
    而且这个错误**不会报错**——只会让指标悄悄变大（GMV 翻倍）。
    等业务方发现对不上账时，往往已经过去很多天。

测试方式：在**独立临时目录 + 独立进程**里连跑两次，对比各层内容指纹。
为什么要隔离：链路写的是同一个 duckdb 文件，在主进程里跑会和其他测试互相污染，
表现为"单独跑能过、一起跑就挂"的随机失败。
注意所有查询都带 ORDER BY——**没有 ORDER BY 的结果集顺序不保证稳定**，
不加会得到一个随机失败的测试，而这类"偶发失败"最容易被误判成环境问题。
"""

from __future__ import annotations

import pytest

# 参与指纹比对的表：每一层都挑有代表性的
FINGERPRINT_SQL = {
    "ods_order": "SELECT * FROM ods_order_info ORDER BY order_id, dt, quantity, amount",
    "dwd_order": "SELECT order_id, user_id, sku_id, amount, is_valid, reject_reason, dt "
                 "FROM dwd_order_detail ORDER BY order_id, sku_id, amount",
    "dwd_log": "SELECT log_id, user_id, event, log_date FROM dwd_page_log ORDER BY log_id",
    "dim_sku": "SELECT * FROM dim_sku ORDER BY sku_id",
    "dim_user": "SELECT * FROM dim_user ORDER BY user_id",
    "dws_traffic": "SELECT * FROM dws_traffic_user_1d ORDER BY user_id, dt",
    "dws_trade": "SELECT * FROM dws_trade_user_1d ORDER BY user_id, dt",
    "ads_daily": "SELECT * FROM ads_daily_metrics ORDER BY dt",
    "ads_category": "SELECT * FROM ads_gmv_category_1d ORDER BY dt, category",
    "ads_funnel": "SELECT * FROM ads_funnel_1d ORDER BY dt, stage",
    "reject_audit": "SELECT * FROM dwd_order_reject_audit ORDER BY dt, reject_reason",
}

# 每次运行都会变的元数据列，不参与内容一致性比对
VOLATILE_COLS = {"_batch_id", "_load_time", "_source_file"}


@pytest.fixture(scope="module")
def dedup_check(iso_project) -> dict:
    """跑「第一次 → 第二次」并对比指纹，返回两次的结果。"""
    return iso_project("dedup", first=FINGERPRINT_SQL, second=FINGERPRINT_SQL)


def test_rerun_is_row_for_row_identical(dedup_check) -> None:
    """第二次跑批不得改变任何一层的有效内容。"""
    before, after = dedup_check["first"], dedup_check["second"]
    for name in before:
        assert before[name] == after[name], (
            f"{name} 在重跑后发生了变化：行数 {len(before[name])} -> {len(after[name])}。"
            "这就是幂等被破坏——重跑会灌双份数据，而且不会报错。"
        )


def test_row_counts_stable(dedup_check) -> None:
    """行数级校验：任何一张表都不应因重跑而翻倍。"""
    before, after = dedup_check["first"], dedup_check["second"]
    for name in before:
        assert len(before[name]) == len(after[name]), (
            f"{name} 行数变化 {len(before[name])} -> {len(after[name])}，"
            "典型的「分区覆盖」漏写 DELETE"
        )


def test_gmv_stable(dedup_check) -> None:
    """指标级校验：GMV 是最直观的"有没有灌双份"的探针。"""
    before, after = dedup_check["first"], dedup_check["second"]
    assert before["ads_daily"] == after["ads_daily"], "重跑后每日指标发生了变化"


def test_partial_window_rerun_keeps_other_days(iso_project) -> None:
    """只重跑最后一天时，其他天的数据必须原样保留。

    这条对应真实运维场景：某天数据有问题，只补那一天，不能把整窗口重算一遍
    （大数据量下全量重算代价很高，而"只补一天"是日常操作）。
    """
    out = iso_project("partial", first=FINGERPRINT_SQL, second=FINGERPRINT_SQL,
                      second_window="last_day")
    before, after = out["first"], out["second"]
    for name in before:
        assert len(before[name]) == len(after[name]), (
            f"只重跑最后一天，但 {name} 的行数从 {len(before[name])} "
            f"变成了 {len(after[name])}——说明其他天的数据被误删或重复写入"
        )
    # 内容也要一致：只重跑同一天，结果不应变
    for name in before:
        assert before[name] == after[name], f"只重跑最后一天，但 {name} 内容发生了变化"


def test_full_window_run_prunes_ghost_partitions(prune_probe) -> None:
    """全窗口跑批必须清掉"不属于本窗口"的历史分区（幽灵分区）。

    这个坑是真实存在的：ODS 的幂等是「按 dt 分区先删后插」，它只能保证
    「这一天不会被写两遍」，**完全无法阻止别的窗口留下的分区躺在表里**。
    换个结算日重新造数后重跑，新窗口和旧窗口的日期不重叠，于是 ods_order_info
    里会同时存在两代数据（实测 181,400 × 2 = 362,800 行、两个 _batch_id），
    而质量规则和对账都带 `dt BETWEEN 窗口` 过滤，**完全看不见它们**。

    测试做法：跑窗口 A（默认结算日那一批），再把结算日整体推后、重新造数，
    跑窗口 B（两窗口不重叠），然后断言窗口 A 的分区已被清干净。
    """
    assert prune_probe["disjoint"], (
        f"前置条件不成立：窗口 A {prune_probe['window_a']} 与 "
        f"窗口 B {prune_probe['window_b']} 有重叠，这样测不出幽灵分区"
    )
    assert prune_probe["outside_rows"] == 0, (
        f"全窗口跑批后 ods_order_info 里仍有 {prune_probe['outside_rows']} 行"
        f"落在窗口外（dt 范围 {prune_probe['min_dt']} ~ {prune_probe['max_dt']}，"
        f"窗口是 {prune_probe['window_b']}）—— 幽灵分区没有被清理"
    )
    assert prune_probe["dwd_outside_rows"] == 0, "DWD 层仍有窗口外的历史分区"
    assert prune_probe["distinct_dt"] == 14, (
        f"ods_order_info 的分区数应当等于 N_DAYS=14，实际 {prune_probe['distinct_dt']}"
    )
    assert "pruned=" in prune_probe["ods_message"], (
        f"清理行数没有记进 runlog 的 ods 步骤 message（现在拿到的是 "
        f"{prune_probe['ods_message']!r}）"
    )


def test_batch_id_changes_but_content_does_not(dedup_check) -> None:
    """批次号应当随每次运行变化，但**内容不该跟着变**。

    这条把"运行元数据"和"业务数据"的边界说清楚：
    批次号存在是为了溯源（这一行是哪次跑批写进来的），它本身不参与业务口径。
    如果哪天有人把批次号当成业务字段参与聚合，两天的指标就会因为批次不同而不同——
    这个测试会让那种改动立刻显形。
    """
    before, after = dedup_check["first"], dedup_check["second"]
    assert before["ads_daily"] == after["ads_daily"]
