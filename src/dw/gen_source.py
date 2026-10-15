"""源数据生成器：造出带**已知脏数据**的电商行为日志与订单数据。

为什么要自己造数而不是下载数据集：
1. **你知道脏在哪**。质量规则才有真靶子——每条规则都能指出它抓到了哪一类注入数据，
   而不是写一堆规则然后发现全是通过；
2. **可复现**。同一个 seed 永远产出同一份数据，性能对比才有意义；
3. **可控的倾斜**。可以精确指定"某个 SKU 占 40% 订单"，用来复现 DWS 聚合时的长尾 task。

产出的 4 个 CSV（data/raw/）：
    user_log.csv    行为日志（pv / cart / fav / buy）
    order_info.csv  订单表（含重复订单、退款单、脏行）
    sku_info.csv    商品维度源
    user_info.csv   用户维度源（含同一 user_id 两个手机号）

同时产出一份 **口径基准**（data/raw/_baseline.json）：用"精确业务口径"从干净数据算出的
GMV，供 ADS 对账使用。这是"数据不准 → 定位 → 修复"那条技术难点的靶子。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from dw import config
from dw.config import (
    HOT_CATEGORY,
    HOT_SKU_ID,
    HOT_SKU_SHARE,
    N_DAYS,
    N_LOGS,
    N_ORDERS,
    N_SKUS,
    N_USERS,
)

# 类目与价格区间（价格固定，避免"商品改价导致历史订单口径漂移"这种额外复杂度）
CATEGORIES: dict[str, tuple[float, float]] = {
    "数码": (300, 6000),
    "家电": (200, 4000),
    "服饰": (30, 500),
    "食品": (10, 200),
    "图书": (15, 120),
    "美妆": (40, 800),
}
CITIES = ["北京", "上海", "广州", "深圳", "成都", "重庆", "杭州", "武汉", "西安", "南京"]
FUNNEL_EVENTS = ["pv", "pv", "pv", "cart", "fav", "buy"]  # 权重：浏览多，下单少

# 结算日：整批数据的"今天"。统计窗口是 [结算日-N+1, 结算日]，
# 窗口外的未来日期视为脏数据、窗口外的历史日期视为迟到数据。
SETTLE_DATE = "2026-10-05"


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


# --------------------------------------------------------------------------
# 维度：商品
# --------------------------------------------------------------------------
def gen_sku(rng: np.random.Generator) -> pd.DataFrame:
    cats = list(CATEGORIES)
    # 保证热销 SKU 一定落在 HOT_CATEGORY 上，否则倾斜实验的口径对不上。
    n_sku = _n(N_SKUS)
    cat_of = [HOT_CATEGORY] + [cats[i % len(cats)] for i in range(n_sku - 1)]
    prices = [
        round(float(rng.uniform(*CATEGORIES[c])), 2) for c in cat_of
    ]
    sku_ids = [HOT_SKU_ID] + [f"SKU{i:05d}" for i in range(2, n_sku + 1)]
    return pd.DataFrame(
        {
            "sku_id": sku_ids,
            "sku_name": [f"{c}商品{i:04d}" for i, c in enumerate(cat_of, start=1)],
            "category": cat_of,
            "brand": [f"品牌{rng.integers(1, 21):02d}" for _ in range(n_sku)],
            "price": prices,
        }
    )


# --------------------------------------------------------------------------
# 维度：用户（含脏数据 1 类）
# --------------------------------------------------------------------------
def gen_user(rng: np.random.Generator, dirty: dict[str, bool]) -> tuple[pd.DataFrame, int]:
    n_user = _n(N_USERS)
    user_ids = [f"U{i:06d}" for i in range(1, n_user + 1)]
    reg_offset = rng.integers(0, 400, n_user)
    df = pd.DataFrame(
        {
            "user_id": user_ids,
            "phone": [f"13{rng.integers(10**8, 10**9 - 1)}" for _ in range(n_user)],
            "city": rng.choice(CITIES, n_user),
            "reg_date": pd.Timestamp("2026-01-01") + pd.to_timedelta(reg_offset, unit="D"),
            "age": rng.integers(18, 61, n_user),
        }
    )

    injected = 0
    if dirty.get("dup_user_phone"):
        # 脏数据：同一 user_id 出现两个不同手机号（跨表/表内一致性问题）
        n = _dirty_n(n_user, 500)
        idx = rng.choice(df.index, n, replace=False)
        clone = df.loc[idx].copy()
        clone["phone"] = [f"15{rng.integers(10**8, 10**9 - 1)}" for _ in range(n)]
        df = pd.concat([df, clone], ignore_index=True)
        injected = n
    return df, injected


# --------------------------------------------------------------------------
# 事实：订单（含脏数据 5 类）
# --------------------------------------------------------------------------
def gen_order(
    rng: np.random.Generator, sku: pd.DataFrame, user: pd.DataFrame, dirty: dict[str, bool]
) -> tuple[pd.DataFrame, dict]:
    # 未来日期注入会把基线整体往前推（见下），这里先算偏移量，再确定"结算日"。
    # 顺序很重要：先定偏移、再定 today、最后才注入脏数据。
    # 反过来做（先注入未来日期、再按数据上界确定窗口）会形成循环依赖：
    # 窗口跟着未来日期一起漂移，于是"未来日期"永远落在窗口内，规则命中 0。
    future_offsets = rng.integers(1, 30, 64)
    max_future = int(future_offsets.max()) if dirty.get("future_date") else 0
    base_settle = pd.Timestamp(SETTLE_DATE).normalize()
    today = base_settle + pd.to_timedelta(max_future, unit="D")

    n_order = _n(N_ORDERS)
    day_offsets = rng.integers(0, N_DAYS, n_order)
    order_dates = today - pd.to_timedelta(day_offsets, unit="D")

    sku_ids = sku["sku_id"].to_numpy()
    price_map = dict(zip(sku["sku_id"], sku["price"], strict=True))

    # ---- 倾斜：热销 SKU 占 HOT_SKU_SHARE，其余均匀分布 ----
    n_hot = int(n_order * HOT_SKU_SHARE)
    hot = np.full(n_hot, HOT_SKU_ID, dtype=object)
    rest = rng.choice(sku_ids, n_order - n_hot)
    chosen = np.concatenate([hot, rest])
    rng.shuffle(chosen)

    user_ids = rng.choice(user["user_id"].unique(), n_order)
    quantities = rng.integers(1, 4, n_order)

    df = pd.DataFrame(
        {
            "order_id": [f"O{i:08d}" for i in range(1, n_order + 1)],
            "user_id": user_ids,
            "sku_id": chosen,
            "quantity": quantities,
            "unit_price": [price_map[s] for s in chosen],
            "order_date": order_dates,
            "order_status": "paid",
            "channel": rng.choice(["app", "h5", "mini"], n_order, p=[0.6, 0.25, 0.15]),
        }
    )
    df["amount"] = (df["quantity"] * df["unit_price"]).round(2)

    # 退款单（≈3%）：状态为 refunded，金额不计入净 GMV
    n_refund = max(_DIRTY_MIN, int(n_order * 0.03))
    refund_idx = rng.choice(df.index, n_refund, replace=False)
    df.loc[refund_idx, "order_status"] = "refunded"

    # 精确业务口径的"干净"基准先留空，等脏数据注入完再算（见函数末尾）
    stats: dict[str, int] = {"refund_rows": n_refund}
    extra: list[pd.DataFrame] = []

    # ---- 脏数据注入 --------------------------------------------------------
    #
    # 设计要点（这几条都是踩坑之后定下来的，不是一开始就想清楚的）：
    #
    # ① **每一类脏数据用独立的 order_id 前缀**，绝不"克隆原行"。
    #    最开始的做法是随机抽若干行做克隆再改字段，结果：
    #      · 克隆体与原行**同号**，DWD 按 order_id 去重时会留下一行、丢掉另一行，
    #        脏数据被静默吃掉，质量规则报 0 行；
    #      · 不同类脏数据还会抽到同一行，互相覆盖（注入了 7 行只报出 1 行）。
    #    改成"每类一批新单号"之后，每一条脏数据都**保证能到达 DWD 并被判定**——
    #    测试要验的是"规则能不能抓到它"，那就必须确保它一定存在。
    #
    # ② **未来日期的基准日要先抬高**：注入的未来日期必须落在统计窗口之外才叫"未来"，
    #    而统计窗口是 [最大业务日期-N+1, 最大业务日期]。所以基线日期要整体挪到
    #    "未来日期最大值"之上，否则那些行反而会落进窗口里被当成正常数据。
    #    这个顺序错了会导致"注入了未来日期，但 DWD 一条都没判出来"。
    #
    # ③ 脏行的 user_id / sku_id / 单价仍然取自正常池子，只有出问题的那一列是脏的——
    #    这样每类脏数据都是**单因素**的，规则命中数才能干净地归因。
    _next_dirty_id = 1

    def _new_id() -> str:
        nonlocal _next_dirty_id
        oid = f"O9{_next_dirty_id:07d}"
        _next_dirty_id += 1
        return oid

    def _dirty_block(n: int) -> pd.DataFrame:
        """生成 n 行"结构正常"的订单骨架（单号互不重复），供各类脏数据各自改写。"""
        k = max(0, min(n, n_order))
        if k == 0:
            return df.iloc[0:0].copy()
        picked = rng.choice(sku_ids, k)
        qty = rng.integers(1, 4, k)
        blk = pd.DataFrame(
            {
                "order_id": [_new_id() for _ in range(k)],
                "user_id": rng.choice(user["user_id"].unique(), k),
                "sku_id": picked,
                "quantity": qty,
                "unit_price": [price_map[s] for s in picked],
                "order_date": today - pd.to_timedelta(rng.integers(0, N_DAYS, k), unit="D"),
                "order_status": "paid",
                "channel": rng.choice(["app", "h5", "mini"], k, p=[0.6, 0.25, 0.15]),
            }
        )
        blk["amount"] = (blk["quantity"] * blk["unit_price"]).round(2)
        return blk

    # 未来日期的最大偏移已在函数开头用于确定基线日期，这里不再重复计算

    # 脏 1：重复 order_id —— 复制一批**已有**订单（同号），这是唯一需要同号的一类
    dup_blk = None
    if dirty.get("dup_order_id"):
        n_dup = _dirty_n(n_order, 300)
        idx = rng.choice(df.index, min(n_dup, len(df)), replace=False)
        dup_blk = df.loc[idx].copy()
        extra.append(dup_blk)
        stats["dup_order_id"] = len(dup_blk)

    # 脏 2：user_id 为空
    if dirty.get("null_user_id"):
        blk = _dirty_block(_dirty_n(n_order, 900))
        blk["user_id"] = None
        extra.append(blk)
        stats["null_user_id"] = len(blk)

    # 脏 3：金额为空
    if dirty.get("null_amount"):
        blk = _dirty_block(_dirty_n(n_order, 1200))
        blk["amount"] = np.nan
        extra.append(blk)
        stats["null_amount"] = len(blk)

    # 脏 4：SKU 在商品表中不存在（参照完整性断裂）
    if dirty.get("orphan_sku"):
        blk = _dirty_block(_dirty_n(n_order, 500))
        blk["sku_id"] = [f"SKU9{i:04d}" for i in range(len(blk))]
        # 单价保留原值：孤儿商品要的是"关联不上"，不是"金额也乱"
        extra.append(blk)
        stats["orphan_sku"] = len(blk)

    # 脏 5：金额为负
    if dirty.get("negative_amount"):
        blk = _dirty_block(_dirty_n(n_order, 2000))
        blk["amount"] = -blk["amount"].abs()
        extra.append(blk)
        stats["negative_amount"] = len(blk)

    # 脏 6：订单日期在未来（严格大于统计窗口上界）
    if dirty.get("future_date"):
        blk = _dirty_block(_dirty_n(n_order, 2500))
        blk["order_date"] = today + pd.to_timedelta(
            rng.choice(future_offsets, len(blk)), unit="D"
        )
        extra.append(blk)
        stats["future_date"] = len(blk)

    # 定位重复块在 concat 之后的起始行，并**先把"原始单号 → 原始日期"的映射取好**。
    # 为什么要提前取：concat 之后 df 里同时有克隆体和原始行，
    # `drop_duplicates(keep="first")` 挑到哪一条取决于拼接顺序；从拼接前的 df 取映射，
    # 语义才明确是"克隆体对齐到它所克隆的那一行"。
    dup_start: int | None = None
    base_date = None
    if dup_blk is not None and len(dup_blk):
        base_date = df.drop_duplicates(subset=["order_id"], keep="first").set_index(
            "order_id"
        )["order_date"]
        dup_start = len(df)

    if extra:
        df = pd.concat([df, *extra], ignore_index=True)

    # ---- 重复行必须与它克隆的原始行落在**同一个日期分区** ----
    #
    # 为什么必须这么做：DWD 的去重是**按分区**做的
    # （`row_number() OVER (PARTITION BY order_id ...) WHERE dt = 某天`）。
    # 如果克隆体被扔到另一个日期分区，两条同号订单会分别在两个分区里各自成为"第一名"，
    # **永远不会被去重掉**，唯一性规则看到的永远是 1.0——
    # 数据里明明有重复主键，质量报告却是全绿。
    #
    # 这个现象在真实系统里同样存在，而且是分区表去重最经典的坑：
    # **去重范围必须覆盖可能重复的分区，否则跨分区重复无法被发现**。
    #
    # ⚠️ 这里**不能**假设"克隆体位于 df 尾部"：dup_blk 是 extra 的**第一块**，
    # 后面还跟着 null_user_id / null_amount / orphan_sku / negative_amount / future_date
    # 五块脏数据。原先按 `len(df) - len(dup_blk)` 取尾部，实际落到的是**最后一块**（未来日期），
    # 那批行的单号在映射里只会映射回自己，看着"没出错"，但真正的重复行一行都没被校对过——
    # 一旦脏数据块的顺序变化或克隆体重用了已有单号，尾部行会被静默改到别的日期分区。
    # 所以按拼接时的真实偏移定位。
    if dup_start is not None:
        rows = df.index[dup_start : dup_start + len(dup_blk)]
        df.loc[rows, "order_date"] = df.loc[rows, "order_id"].map(base_date).values

    df = df.sample(frac=1.0, random_state=20261005).reset_index(drop=True)

    # ---- 对账基准：与 DWD 完全同口径 ----
    #
    # 基准 = "唯一订单 且 已支付 且 金额有效 且 **user_id 非空**"，
    # 并且**去重时的取舍规则要和 DWD 一致**：
    # DWD 用 `(user_id IS NULL) DESC, (amount IS NULL) DESC` 把"有问题的行"排在前面当证据，
    # 所以基准也必须用同样的规则挑那一行。否则两边各自留了不同的行，差异率会凭空出现
    # （实测踩过：基准用 keep='first'、DWD 优先留脏行，结果差 0.076%，看起来像链路有 bug）。
    #
    # ⚠️ `user_id` 非空这一条**曾经漏掉过**，而且漏得很典型：
    #   DWD 的 `is_valid` 把 user_id 为空判为无效（无法归属到人），但基准原先只过滤了
    #   paid / amount 非空 / amount > 0，**没有**过滤 user_id 非空。于是注入的 200 行
    #   "user_id 为空"的脏数据全部留在基准里，而 DWD 把它们剔掉了——
    #   对账残差 493,474.47 里有 341,784.97（**69.3%**）来自这个口径差，
    #   却被 README 一句"残余来自窗口边界"全部归因掉了。
    #   这正是本文件上面那句"基准算错比代码写错更难发现"的第二次应验：
    #   两边的数字都"看起来对"，差异也"看起来有合理解释"。
    #
    # **基准算错比代码写错更难发现，因为两边都"看起来对"。**
    ordered = (
        df.assign(
            _prio_user=(df["user_id"].isna()).astype(int),
            _prio_amt=(df["amount"].isna()).astype(int),
        )
        .sort_values(
            by=["order_id", "_prio_user", "_prio_amt", "amount"],
            ascending=[True, False, False, False],
            kind="stable",
        )
        .drop_duplicates(subset=["order_id"], keep="first")
    )
    dwd_aligned = (
        ordered.query("order_status == 'paid'")
        .query("amount == amount")
        .query("amount > 0")
        .query("user_id == user_id")          # 与 DWD 的 is_valid 对齐：user_id 为空即无效
    )

    stats["clean_orders"] = len(dwd_aligned)
    # 与 DWS/ADS 一样收敛到分：不然对账时会出现"差 1e-9 元"这种噪声差异，
    # 让人误以为链路有问题（实测踩过：0.079% 的差异率全部来自这种末位误差）。
    stats["clean_gmv"] = round(float(dwd_aligned["amount"].sum()), 2)
    stats["settle_date"] = today.strftime("%Y-%m-%d")
    # 源表直算口径：不去重、不过滤退款与无效金额——用来做"清洗前后"的对照
    raw = df.query("amount == amount")
    stats["raw_orders"] = len(df)
    stats["raw_gmv"] = float(raw["amount"].sum())
    stats["refund_gmv"] = float(df.loc[df["order_status"] == "refunded", "amount"].sum())
    stats["raw_rows"] = len(df)
    return df, stats


# --------------------------------------------------------------------------
# 事实：行为日志（含脏数据 2 类）
# --------------------------------------------------------------------------
def gen_user_log(
    rng: np.random.Generator, sku: pd.DataFrame, user: pd.DataFrame,
    dirty: dict[str, bool], settle_date: str | None = None,
) -> tuple[pd.DataFrame, dict]:
    # 结算日必须与订单侧保持一致。
    # 订单侧因为要注入"未来日期"，会把基线整体前推（见 gen_order），
    # 如果日志这里还用常量 SETTLE_DATE，两边就会错位——
    # 表现是统计窗口落在日志数据的时间范围之外，**ODS 读不到任何日志**、
    # DWS 流量汇总整层为空，而其他层却完全正常（很隐蔽的一类错位）。
    today = pd.Timestamp(settle_date or SETTLE_DATE).normalize()
    n_logs = _n(N_LOGS)
    user_ids = user["user_id"].unique()
    sku_ids = sku["sku_id"].to_numpy()

    df = pd.DataFrame(
        {
            "log_id": [f"L{i:09d}" for i in range(1, n_logs + 1)],
            "user_id": rng.choice(user_ids, n_logs),
            "sku_id": rng.choice(sku_ids, n_logs),
            "event": rng.choice(FUNNEL_EVENTS, n_logs),
            "log_date": today - pd.to_timedelta(rng.integers(0, N_DAYS, n_logs), unit="D"),
            "stay_seconds": rng.integers(1, 300, n_logs),
        }
    )

    stats: dict[str, int] = {"raw_rows": len(df)}
    extra: list[pd.DataFrame] = []

    # 脏 7：user_id 为空的日志
    if dirty.get("null_user_id"):
        n = _dirty_n(n_logs, 3000)
        blk = df.sample(n, random_state=7).copy()
        blk["log_id"] = [f"L9{i:08d}" for i in range(n)]
        blk["user_id"] = None
        extra.append(blk)
        stats["null_user_log"] = n

    # 脏 8：混入"昨天"的迟到数据（分区日期与业务日期不一致）
    if dirty.get("late_arrival"):
        n = _dirty_n(n_logs, 2000)
        blk = df.sample(n, random_state=11).copy()
        blk["log_id"] = [f"L8{i:08d}" for i in range(n)]
        blk["log_date"] = today - pd.to_timedelta(N_DAYS + 1, unit="D")  # 落在窗口之外
        extra.append(blk)
        stats["late_arrival"] = n

    if extra:
        df = pd.concat([df, *extra], ignore_index=True)
    df = df.sample(frac=1.0, random_state=20261005).reset_index(drop=True)
    stats["raw_rows"] = len(df)
    return df, stats


# --------------------------------------------------------------------------
# 入口
# --------------------------------------------------------------------------
BY_DT_DIRNAME = "by_dt"


def _write_partitions(df: pd.DataFrame, out: Path, prefix: str, date_col: str) -> int:
    """按业务日期把数据切成 `by_dt/{prefix}_{YYYY-MM-DD}.csv`。

    为什么要额外切一份：
      ODS 是**按分区加载**的。全量 CSV 一次性读进来再按 dt 过滤，等于每次跑批
      都要扫全表；切成日期文件之后，加载某一天只读那一天的文件——
      这就是 Hive 分区的直观版本，也是"为什么分区能省 IO"最直接的体现。

    ⚠️ 必须先清掉**本前缀**的旧文件再写。只清同名前缀是有意为之：
      改了 seed / 数据规模 / 结算日之后，**新数据的日期范围会和旧的错开**，
      旧日期的分区文件会残留下来。而 ODS 是按"窗口内有哪些日期文件"加载的，
      残留文件会以"幽灵分区"的形式混进数仓——实测表现为对账差 0.09%（约 1.2 万行）
      且找不到来源，因为那些行来自上一次造数的数据。

      但**不能把整个目录删掉**：订单和日志是分两次调用这个函数的，
      用 rmtree 清目录会把前一次刚写好的另一类分区一起删掉——
      症状是"订单分区文件一个都没有、日志却有"，而且不报任何错。
    """
    pdir = out / BY_DT_DIRNAME
    pdir.mkdir(parents=True, exist_ok=True)
    for old in pdir.glob(f"{prefix}_*.csv"):
        old.unlink()
    dates = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
    n = 0
    for d, part in df.groupby(dates, sort=True):
        part.to_csv(pdir / f"{prefix}_{d}.csv", index=False, encoding="utf-8")
        n += 1
    return n


# 数据规模缩放系数：测试里设成 0.02 就能在 1~2 秒内跑完全链路。
# 用"缩放系数"而不是直接改模块常量，是为了避免 `global N_USERS` 这类
# 在函数内重新绑定的写法（那样会遮蔽模块级名字，读代码时很容易看漏）。
_SCALE = 1.0


def use_scale(scale: float) -> None:
    """设置全局缩放系数（测试与小样本演示用）。"""
    global _SCALE
    _SCALE = max(0.0001, float(scale))


def _n(base: int) -> int:
    """按缩放系数计算实际条数，并保证不少于 1（否则分组会落空）。"""
    return max(1, int(base * _SCALE))


# 每类脏数据的注入比例：按"总量 / 分母"抽样，但**保证至少有 2 行**。
#
# 为什么要设下限：早先写的是 `max(1, n // 300)`，"至少 1 行"看起来够，
# 但测试把数据缩到几千行时 `n//300` 等于 0，`max(1, 0)` = 1 ——
# 只注入 1 行的话，"影响行数 > 0" 这类断言虽然能过，
# 但只要有任何一环把这一行去重/合并掉，规则就再也抓不到了（测试随机失败）。
# 更根本的问题：**注入量应该由"要验证什么"决定，而不是由整除的巧合决定**。
# 所以这里改成比例 + 硬下限，让每种脏数据在各种规模下都稳定存在。
_DIRTY_MIN = 2


def _dirty_n(total: int, denom: int) -> int:
    """按比例计算脏数据注入行数，保证至少 _DIRTY_MIN 行、且不超过总量。"""
    return max(_DIRTY_MIN, min(total, total // denom))


def generate(seed: int = 20261005, scale: float = 1.0) -> dict:
    """生成全部源数据，返回统计信息（含对账基准）。

    输出目录**只有一处真相**：`config.RAW_DIR`（可用 DW_DATA_DIR / DW_RAW_DIR 覆盖）。
    这里曾经有个 `out_dir` 参数，它只影响"写"，而 run/quality/recon 读的仍是
    config.RAW_DIR——于是 `gen --out X` 会把数据写到 X 而后面的命令根本读不到，
    命令不报错、结果却用旧的源数据算，属于最难发现的那种错。
    """
    use_scale(scale)
    out = Path(config.RAW_DIR)
    out.mkdir(parents=True, exist_ok=True)
    rng = _rng(seed)
    dirty = config.DIRTY

    sku = gen_sku(rng)
    user, n_dup_phone = gen_user(rng, dirty)
    order, order_stats = gen_order(rng, sku, user, dirty)
    log, log_stats = gen_user_log(
        rng, sku, user, dirty, settle_date=order_stats["settle_date"]
    )

    sku.to_csv(out / config.SRC_SKU, index=False, encoding="utf-8")
    user.to_csv(out / config.SRC_USER_INFO, index=False, encoding="utf-8")
    order.to_csv(out / config.SRC_ORDER, index=False, encoding="utf-8")
    log.to_csv(out / config.SRC_USER_LOG, index=False, encoding="utf-8")

    n_order_parts = _write_partitions(order, out, "order_info", "order_date")
    n_log_parts = _write_partitions(log, out, "user_log", "log_date")

    stats = {
        "seed": seed,
        "generated_at": "2026-10-05",
        # 结算日由 gen_order 决定（未来日期注入会把基线整体前推），不能写死常量
        "settle_date": order_stats["settle_date"],
        "rows": {
            "sku": len(sku),
            "user": len(user),
            "order": len(order),
            "user_log": len(log),
        },
        "partitions": {
            "order_dt_files": n_order_parts,
            "log_dt_files": n_log_parts,
        },
        "injected": {
            "dup_user_phone": n_dup_phone,
            **{k: v for k, v in order_stats.items() if k.startswith(("dup_", "null_", "orphan", "negative", "future"))},
            **{k: v for k, v in log_stats.items() if k in {"null_user_log", "late_arrival"}},
        },
        # 对账基准：两个口径都给。收口口径与 DWD 完全同规则，源口径用于对照。
        "baseline": {
            "clean_orders": order_stats["clean_orders"],
            "clean_gmv": round(order_stats["clean_gmv"], 2),
            "raw_orders": order_stats["raw_orders"],
            "raw_gmv": round(order_stats["raw_gmv"], 2),
            "refund_rows": order_stats["refund_rows"],
            "refund_gmv": round(order_stats["refund_gmv"], 2),
        },
        "skew": {
            "hot_sku_id": HOT_SKU_ID,
            "hot_share_target": HOT_SKU_SHARE,
        },
    }
    (out / "_baseline.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="生成电商源数据（含已知脏数据注入）")
    ap.add_argument("--seed", type=int, default=20261005, help="随机种子（默认固定，保证可复现）")
    ap.add_argument("--scale", type=float, default=1.0, help="数据量缩放系数")
    args = ap.parse_args()
    stats = generate(args.seed, args.scale)
    print("[gen] 源数据已生成：")
    for k, v in stats["rows"].items():
        print(f"  {k:<10} {v:>10,} 行")
    print("[gen] 注入的脏数据：")
    for k, v in stats["injected"].items():
        print(f"  {k:<18} {v:>8,} 行")
    print(f"[gen] 对账基准 clean_gmv = {stats['baseline']['clean_gmv']:,.2f}"
          f"（退款 {stats['baseline']['refund_rows']:,} 单，不计入）")


if __name__ == "__main__":
    main()
