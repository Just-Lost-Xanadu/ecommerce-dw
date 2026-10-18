"""全局配置：路径、常量、各层定义、质量阈值。

原则：**所有"魔法数字"集中在这一个文件里**，不在业务代码里散落。
这样调参、复现、写文档都只改一处。
"""

from __future__ import annotations

import os
from pathlib import Path

# ---- 路径 ----------------------------------------------------------------
# 支持用环境变量覆盖，好处有两个：
#   ① 测试可以指向临时目录，不污染真实数仓（`tests/` 就是这么用的）；
#   ② 部署时可以挂到数据盘，不用改代码。
ROOT = Path(os.getenv("DW_ROOT", str(Path(__file__).resolve().parents[2])))
DATA_DIR = Path(os.getenv("DW_DATA_DIR", str(ROOT / "data")))
RAW_DIR = Path(os.getenv("DW_RAW_DIR", str(DATA_DIR / "raw")))
WAREHOUSE_DB = Path(os.getenv("DW_WAREHOUSE", str(DATA_DIR / "warehouse.duckdb")))
META_DB = Path(os.getenv("DW_META_DB", str(DATA_DIR / "meta.sqlite")))
DASHBOARD_DIR = Path(os.getenv("DW_DASHBOARD_DIR", str(ROOT / "dashboard" / "out")))

# ---- 数仓分层 -------------------------------------------------------------
# 这是 **runner 真实执行的顺序**：`runner.run_pipeline()` 的主循环就是遍历
# 这个元组（见 runner.py 里 `for step in config.LAYERS`），所以它不再是
# "写着好看的文档"——改这里会真的改变执行顺序。
#   · dim 必须在 dwd 之前：dwd 要 LEFT JOIN dim_sku，维表必须先存在；
#   · quality 是**门禁**，放在 dws 之前：质量不合格就不该让脏数据流进指标层，
#     它失败时会把它后面的所有步骤记为 blocked 并中断（force=True 时例外）；
#   · dashboard 不是数仓分层，所以不在这个元组里。
LAYERS: tuple[str, ...] = ("ods", "dim", "dwd", "quality", "dws", "ads")

# ---- 数据规模 -------------------------------------------------------------
N_USERS = 20_000
N_SKUS = 800
N_LOGS = 1_200_000
N_ORDERS = 180_000

# 统计窗口：最近 N 天（含当日）
N_DAYS = 14

# ---- 数据倾斜设置 ---------------------------------------------------------
# "数码"类目下的一个热销 SKU 占据约 40% 的订单——用来在 DWS 聚合时制造真实的长尾 task。
HOT_CATEGORY = "数码"
HOT_SKU_ID = "SKU00001"
HOT_SKU_SHARE = 0.40

# ---- 脏数据注入开关 -------------------------------------------------------
# 这里只有 **8 个开关**，但 `python -m dw.cli gen` 会打印 **9 类**注入——
# 因为订单侧的空 user_id（null_user_id）与日志侧的空 user_id
# （null_user_log）是两个独立的注入块，却共用 `null_user_id` 这一个开关。
# 做实验时可单独关掉某一类，观察质量报告的变化。
#
# 覆盖情况（如实说明，不假装全覆盖）：
#   · 有规则/断言负责：dup_order_id、null_user_id（订单侧）、null_amount、
#     negative_amount、orphan_sku、dup_user_phone、future_date（源边界规则）、
#     null_user_log（被 dwd_page_log 丢弃，计入 layer_dwd 的 dropped_rows）；
#   · **暂无规则负责：late_arrival**。这 600 行日志的日期落在统计窗口之外，
#     ODS 按窗口加载，它们根本进不了数仓，仓库内的规则看不到——这正是
#     「校验点必须前移到接入边界」的另一面。目前只在源文件层面守：
#     tests/test_quality.py::test_late_arrival_stays_out_of_window。
DIRTY: dict[str, bool] = {
    "dup_order_id": True,      # 同一个 order_id 出现两次
    "null_user_id": True,      # user_id 为空
    "null_amount": True,       # 金额为空
    "orphan_sku": True,        # 订单里的 SKU 在商品表中不存在
    "negative_amount": True,   # 金额为负
    "future_date": True,       # 操作日期在未来
    "dup_user_phone": True,    # 同一 user_id 对应两个手机号
    "late_arrival": True,      # 混入昨天的数据（分区日期与业务日期不一致）
}

# ---- 质量阈值 -------------------------------------------------------------
# 每条阈值都是"这条规则在这个项目里的容错上限"，超过即失败。
#
# 阈值设计原则——**区分"必须为零"和"必须可控"**：
#   必须为零（阻断级）：主键唯一、进入指标的金额为正、枚举合法。
#     这些一旦出问题，指标就是**错的**，不是"少一点"。
#   必须可控（告警级）：孤儿商品率、用户手机号冲突率、迟到数据率、剔行率。
#     这些在真实系统里天然存在，做法是**限量 + 可见**，而不是要求为 0。
#     如果把它们也设成阻断级，运维会很快学会忽略告警——质量门禁就失效了。
QUALITY_THRESHOLDS: dict[str, float] = {
    # 非空率下限。取值说明：造数注入的 user_id 为空约占千分之一
    # （约 200 / 18 万），所以阈值必须比它松，否则**每次跑批都会被自己的测试数据阻断**。
    # 真实系统里这个阈值来自"业务能容忍多少脏数据"的约定，不是拍脑袋定的数字。
    "not_null_rate": 0.995,
    "unique_rate": 1.0,            # 唯一率下限（主键必须 100%）
    # 下面两条的 actual 都是**孤儿率**（orphan/n），方向是「越小越好」，
    # 所以阈值必须是**上限**。早期版本填了 0.995 / 0.98 这两个「合规率下界」
    # 的数值，于是通过条件变成 `孤儿率 ≤ 99.5%`——哪怕 99% 的订单都关联不上
    # 商品，规则依然是绿的。一条永远不会失败的质量规则等于没有规则。
    "referential_dwd_rate": 0.005,  # DWD 侧孤儿商品率上限 0.5%（超了说明上游口径崩了）
    "referential_ods_rate": 0.02,   # ODS 侧孤儿率上限 2%（源侧参照完整性，放宽一档）
    "value_range_rate": 0.995,     # 值域合法率下限
    "consistency_rate": 0.99,      # 跨表一致性下限
    "timeliness_rate": 0.99,       # 时效性下限
    "max_source_days_ahead": 0,    # 源侧允许出现的"超出处理边界"的天数（0 = 一天都不允许）
    "gmv_volatility": 0.50,        # 单日 GMV 环比超过 ±50% 即视为"超限"
    "gmv_volatility_days_max": 0,  # 允许的"超限天数"上限（0 = 一天都不允许）
    "dwd_reject_rate_max": 0.05,   # DWD 剔行率上限（超了说明源数据质量崩了）
}

# ---- 运行环境 -------------------------------------------------------------
def env_int(name: str, default: int) -> int:
    """读可选的整型环境变量；非数字时报出变量名，不要抛裸 ValueError。"""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 不是整数：{raw!r}") from exc


# CSV 文件名（源数据）
SRC_USER_LOG = "user_log.csv"
SRC_ORDER = "order_info.csv"
SRC_SKU = "sku_info.csv"
SRC_USER_INFO = "user_info.csv"
