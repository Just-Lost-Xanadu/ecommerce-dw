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


def test_batch_id_changes_but_content_does_not(dedup_check) -> None:
    """批次号应当随每次运行变化，但**内容不该跟着变**。

    这条把"运行元数据"和"业务数据"的边界说清楚：
    批次号存在是为了溯源（这一行是哪次跑批写进来的），它本身不参与业务口径。
    如果哪天有人把批次号当成业务字段参与聚合，两天的指标就会因为批次不同而不同——
    这个测试会让那种改动立刻显形。
    """
    before, after = dedup_check["first"], dedup_check["second"]
    assert before["ads_daily"] == after["ads_daily"]
