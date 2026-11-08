# 电商用户行为与订单离线数仓

> **测试口径（2026-11-08 复核）**：`pytest -q` → kbase-agent **40 passed**、ecommerce-dw **32 passed**、mcp-tools **30 passed**，合计 **102**。README 内的历史段落保留更早版本的 77/51/38、60/40/30、49/36/30。

> 把埋点日志与订单数据加工成五层数仓（ODS / DIM / DWD / DWS / ADS），产出可复算的运营指标，
> 并让**数据质量问题可见、可阻断、可解释**，让**重跑幂等、可验证**。

## 数据源（2026-11-08：切换为真实数据集）

主数据来自阿里天池 **UserBehavior**：`https://tianchi.aliyun.com/dataset/649`
（`Dataset/UserBehavior.csv` 或 `data/UserBehavior.csv`，约 1 亿行、98.8 万用户、417 万商品，时间窗 2017-11-25 ~ 2017-12-03）。
本仓库**不入库**这份 3.6GB 文件（见 `.gitignore`），下载后运行：

```bash
python -m dw.cli gen --source taobao     # 默认 auto：检测到 Dataset 就用它，否则回退 synthetic
```

- **采样**：用 `user_id % 20 == 0` 做确定性采样（约 4.9 万用户、491 万行为、9.9 万 buy 事件），
  保留采样用户的全部窗口内行为；采样规则写进 `_baseline.json`，同 CSV + 同 mod 可复算。
- **字段映射**：`behavior_type` 直接映射为 `pv/cart/fav/buy`；`buy` 事件生成订单，
  金额/商品价格/渠道/用户画像是**派生字段**（原始数据没有这些字段，README 不把它们说成原始值）。
- **脏数据仍保留**：在真实用户/商品上等比叠加 9 类可控脏数据（重复订单、空 user_id、
  空金额、负金额、孤儿 SKU、退款、未来日期、空 user 日志、迟到日志），用于质量门禁与对账。
- **合成造数器仍在**：`--source synthetic` 保留原 40% 热销 SKU 数据，用于"单键长尾"倾斜压测；
  默认不再使用它。

一条命令从零跑完，全程离线，不依赖任何云服务或集群。

### 快速开始（照这个顺序敲，可以一次跑通）

```bash
# 1. 建虚拟环境并装依赖
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt

# 2. 【必须】把项目装成包，否则 python -m dw.cli 会报 No module named dw
#    原因：代码在 src/dw/ 下，Python 默认的模块搜索路径里没有 src。
.venv\Scripts\python.exe -m pip install -e .
#    Windows 下不想安装也可以，改成把 src 加进 PYTHONPATH（当前会话有效）：
#      cmd:        set PYTHONPATH=src
#      PowerShell: $env:PYTHONPATH = "src"

# 3. 跑数
.venv\Scripts\python.exe -m dw.cli gen --source taobao   # 天池真实数据（Dataset/ 或 data/ 下的 UserBehavior.csv）
#   没有 Dataset 时用：python -m dw.cli gen               # auto 回退 synthetic
.venv\Scripts\python.exe -m dw.cli run      # 跑完整链路：ODS → DIM → DWD → 质量门禁 → DWS → ADS → 看板
.venv\Scripts\python.exe -m dw.cli recon    # 对账与倾斜量化（两个技术难点的实测证据）
```

> 装好 `-e .` 之后（或激活了虚拟环境），上面每条都可以简写成 `python -m dw.cli gen`。
> 注意 **`pytest -q` 不需要第 2 步**——`pyproject.toml` 里有 `pythonpath = ["src"]`，
> pytest 自己会把 `src` 加进路径。所以会出现「测试全绿、命令行却说找不到 dw 包」的现象，
> 这是本项目最容易让人怀疑人生的第一个坎，原因就在这里。

---

## 一、这个项目解决什么问题

运营每天手工导 Excel 算 GMV 和转化率，口径不一致、数字对不上账。
本项目把这条链路做成可调度的批处理管道：

- **统一口径**：所有指标只在 ADS 层定义，明细口径（去重、退款、有效性）统一收口在 DWD；
- **质量可控**：12 条质量规则（4 条阻断级 + 8 条告警级）+ 严重问题**阻断下游**；
- **可解释**：被剔除的数据**标记原因而不删除**，所以"少了多少、为什么少"随时可查；
- **可重跑**：任何一层重跑都不产生重复数据，且有测试锁住这条性质。

---

## 二、架构

```
数据源                数仓分层                          应用
埋点日志 ──┐
           ├─→ ODS ──→ DIM ──→ DWD ──→ 质量门禁 ──→ DWS ──→ ADS ──→ HTML 看板
订单数据 ──┘     原始    维度     明细     阻断        汇总     指标
                  │        │        │                  │        │
                  └────────┴────────┴──────────────────┴────────┘
                        运行链路记录 + 质量结果（SQLite，保留历史）
```

| 层 | 职责 | 表 |
|---|---|---|
| **ODS** | 原样落地，不做业务清洗，保留脏数据作为证据 | `ods_order_info`、`ods_user_log` |
| **DIM** | 维度表（星型模型的"维"侧） | `dim_sku`、`dim_user`、`dim_date` |
| **DWD** | 清洗 + 有效性判定 + 口径收口；粒度＝一订单一行商品 | `dwd_order_detail`、`dwd_page_log`、`dwd_order_reject_audit` |
| **DWS** | 轻度汇总（用户 × 天），供多指标共享 | `dws_traffic_user_1d`、`dws_trade_user_1d` |
| **ADS** | 对外指标层，每个指标带口径说明 | `ads_daily_metrics`、`ads_gmv_category_1d`、`ads_funnel_1d` |

**技术选型**：DuckDB（单文件、零部署的列式引擎）· SQL 优先 · 自写 runner 编排（可选 Airflow 调度层）· SQLite 存运行元数据 · 自渲染 HTML 看板。

---

## 三、为什么这么设计（关键设计取舍）

### 3.1 为什么要分层

**每一层只解决一类问题**：

- **ODS 原样落地**：清洗规则会变，源数据不会变。留着原样才能用新规则重跑；
  更重要的是——**脏数据是证据**，ODS 保留脏行，DWD 才标记剔除，这样"数据少了多少"可解释。
- **DWD 是口径收口点**：去重、退款、有效性全部在这里定死，上层只引用 DWD。
  反例：如果在每张 ADS 里各自过滤，改一次口径要改 N 处，迟早出现"这张表改对了、那张表忘了改"。
  这就是"口径不一致"最常见的来源。
- **DWS 是半成品**：留存、活跃、漏斗都基于"用户天粒度"，算一次存下来给多个指标共享。
- **ADS 只放有口径的指标**：口径变了只改一层。

### 3.2 为什么去重的排序规则要显式保护"有问题的行"

DWD 按 `order_id` 去重。最初写的是：

```sql
ORDER BY _load_time DESC, amount DESC NULLS LAST   -- ❌ 有问题的写法
```

同一批导入的重复行 `_load_time` 完全相同，于是排序落到第二键，
`NULLS LAST` 把"金额为空"的那一行排到后面，**去重之后脏数据被静默丢弃**：
DWD 报出 0 行金额为空，看起来像"源数据很干净"。

**危害不在于丢了一行数据，而在于让质量问题不可见**——质量报告全绿，但报告本身是假的。

现在的写法显式把有问题的行排在最前面（`(user_id IS NULL) DESC, (amount IS NULL) DESC`）：

> 去重的目标是"同一业务事实只留一份"，**不是"顺手把脏数据洗掉"**。

### 3.3 为什么未来日期要在"源文件边界"校验，而不是在数仓里

注入的"未来日期"数据落在**处理窗口之外**的分区里，因此**根本不会被加载进数仓**——
数仓内部的任何规则都不可能看到它。只在仓库里做校验，这类问题会永远沉默，
直到某天有人手工调整窗口，未来数据一次性涌进指标表。

所以规则 `source_days_within_boundary` 检查的是**送进来的分区文件里有没有超出结算日的日期**，
而不是"表里有什么"。把校验点前移到接入边界，比在下游发现问题便宜得多。

### 3.4 为什么阈值要区分"必须为零"和"必须可控"

| 类型 | 例子 | 处理 |
|---|---|---|
| **必须为零**（阻断级） | 主键重复、进入指标的金额为负、枚举非法 | 失败即**阻断下游**，脏数据流不进指标层 |
| **必须可控**（告警级） | 孤儿商品率、手机号冲突率、迟到数据率、剔行率 | 限量 + 可见，超限告警 |

如果把后者也设成阻断级，一有小瑕疵整条链路就停，运维会很快学会忽略告警——
**质量门禁就形同虚设**。所以阈值本身要按"业务能容忍多少"来定，而不是拍一个 100%。

### 3.5 为什么聚合要定精度

`DOUBLE` 累加的结果依赖求和顺序，而重跑时行的物理顺序会变。
实测出现过同一用户同一天算出 `8660.22` 与 `8660.220000000001` 两个值——
业务上完全等价，但让"重跑结果逐行一致"这条性质无法验证。

处理方式：**明细保真，汇总定精度**——DWD 保留原始精度，DWS/ADS 用 `ROUND` 收敛到分。

---

## 四、数据质量（12 条规则）

| 规则 | 层 | 类型 | 阻断 | 说明 |
|---|---|---|---|---|
| `unique_order_id` | DWD | 唯一性 | ✅ | 订单号必须唯一。**在单天分区内恒为通过**（DWD 已按 `order_id` 去重），它是防「去重逻辑被改坏」的回归护栏；跨天重复不在本规则视野内 |
| `not_null_user_id` | DWD | 完整性 | ✅ | 有效订单必须有 user_id（在 DWD 全量行上算非空率） |
| `positive_amount` | DWD | 值域 | ✅ | **没有任何一行可以「既被判为有效、金额又非正」**（is_valid 与 amount 必须自洽）。⚠️ 旧口径是「在 is_valid 行上算 amount > 0」，而 is_valid 已蕴含 amount > 0，属于恒真表达式，已修正 |
| `valid_event_enum` | DWD | 值域 | ✅ | 行为事件枚举合法（漏斗依赖它） |
| `orphan_sku_rate_dwd` | DWD | 参照完整性 | ❌ | 孤儿商品率**上限 0.5%**（ADS 用「未知类目」兜住，所以是可控告警） |
| `orphan_sku_rate_ods` | ODS | 参照完整性 | ❌ | 源侧孤儿率**上限 2%**。⚠️ 孤儿行在 DWD **不会**被剔除，所以本规则的孤儿数与 DWD 侧**当前恒等**，两者差值暂无信息量（等 DWD 开始剔孤儿行才有意义） |
| `consistent_user_phone` | DIM | 一致性 | ❌ | 源侧同一 `user_id` 是否出现多个手机号；真实数据 1.0000（无冲突），synthetic 会注入 40 个冲突用户 |
| `event_date_in_window` | ODS | 时效性 | ❌ | ODS 的 `dt` 落**文件分区日期**、`log_date` 才是业务日期；本规则检查分区内是否混入窗口外的业务日期（迟到/错放）。已有实际防护能力 |
| `source_days_within_boundary` | 源文件 | 时效性 | ❌ | 源分区文件不得出现超出结算日的日期（真实数据实测命中 **5 个超前分区**：注入的 `2017-12-04 ~ 12-08`） |
| `gmv_day_over_day` | DWD | 波动监控 | ❌ | **超限天数上限 0**：任何一天 GMV 日环比超过 ±50% 都要人工确认。读的是 `dwd_order_detail`（因为门禁跑在 dws **之前**，读 dws 会读到空表或上一轮的旧数据）。窗口只有 1 天时本规则标为「不适用（skipped）」，不再伪装成通过 |
| `reject_rate_max` | DWD | 健康度 | ❌ | 剔行率上限 5%，超限说明源数据质量崩了 |
| `funnel_stage_monotonic` | DWD | 一致性 | ❌ | 漏斗各阶段人数必须单调不增（浏览 ≥ 加购 ≥ 下单）。**天池真实数据上通过**（1.0000）；切到 `--source synthetic` 时会失败（造数事件独立随机），保留为可演示的「活靶子」 |

**大部分规则都有真靶子**：导入层 / synthetic 造数器注入 9 类脏数据，规则必须能抓到它们。
如实说明两条例外：`unique_order_id` 在 DWD 去重后（同一天分区内）恒为通过，是防回归护栏；
`source_days_within_boundary` 只扫描订单分区，未来日期的日志分区不在覆盖范围内。

### 4.0 两处如实说明（不藏的覆盖缺口）

1. **`late_arrival`（迟到 600 行日志）目前没有任何规则负责**。它们的日期落在统计窗口之外，
   ODS 按窗口加载，根本进不了数仓，仓库内的规则看不到——这正是「校验点必须前移到接入边界」的另一面。
   现在只在**源文件层面**守：`tests/test_quality.py::test_late_arrival_stays_out_of_window`。
2. **「9 类脏数据 / 8 个开关」（synthetic 专属）**：订单侧的空 `user_id`（`null_user_id`）与日志侧的空 `user_id`（`null_user_log`）
   是两个独立的注入块，却共用了 `null_user_id` 这一个开关 —— 所以 `config.DIRTY` 是 8 个键，而 synthetic 打印 9 行。
   天池导入层的注入量是独立计算的（见 `_baseline.json.injected`），不读 `config.DIRTY`。

```bash
python -m dw.cli quality    # 只看质量报告
```

### 4.1 门禁真的会阻断

把阈值调到不可能达到，下游会**真的不跑**：

```
[BLOCKED] 1 条阻断级规则未通过：unique_order_id(0.5 < 1.0)
         下游（dws/ads）已阻断，指标表未更新。
         处理方式：修源数据/修规则后重跑；或用 --force 强制出数（会在 trace 里留痕）。
```

`--force` 是应急通道（真实运维里"已知问题、先出数"），但**会把"绕过了哪几条阻断规则"写进 `run_steps.message`**；注意这一轮的 `quality` 步骤状态仍是 `ok`，`dw.cli runs` 的"异常步"不会单独标红。

---

## 五、指标口径

完整口径见 [`docs/指标口径.md`](docs/指标口径.md)，字段说明见 [`docs/数据字典.md`](docs/数据字典.md)。
每个指标的定义**直接写在 SQL 注释里**（口径必须和代码在一起，否则改代码的人不会同步改文档）。

| 指标 | 口径要点 |
|---|---|
| DAU | 当日产生过任意行为事件的去重用户数 |
| 新增用户 | 当日首次出现行为的用户（首次＝**仓库当前保留历史**中最早那天；空仓直接跑 `--limit 1` 会把当天所有人算成新用户，这是已知口径边界） |
| GMV | **净 GMV**：有效且已支付，已去重、不含退款 |
| 支付转化率 | `pay_users / visit_users`，**人数口径**，且是**同日**口径 |
| 客单价 AOV | `gmv / order_cnt` |
| Top1 商品占比 | 最大单商品 GMV 占当日 GMV 比例（用于观察数据分布是否倾斜） |

**已知口径局限**（写在 SQL 注释与文档里，不藏着）：
转化率是"当日转化"，用户今天浏览、明天下单不会被算进今天的转化；
漏斗的"下单"用的是行为日志的 `buy` 事件（行为口径），与订单侧的支付人数**不是一个口径，不可交叉相加**。

---

## 六、实测数据（可复算，2026-11-08）

默认数据源是**阿里天池 UserBehavior** 的确定性采样（`user_id % 20 == 0`）：
窗口 **2017-11-25 ~ 2017-12-03（9 天）**，窗口内 **99,660 订单源行 + 490 万行为日志 +
109 万商品 + 4.9 万用户**。源文件总行数会略高（含窗口外未来日期/迟到数据），两者不是同一口径。
旧的 synthetic 数据（18 万订单 / 120 万日志 / 40% 热销 SKU）仍可用 `--source synthetic` 复现。

### 6.1 对账：清洗把差异率从 3.406% 降到 0.000%

```bash
python -m dw.cli recon
```

| 口径 | GMV | 与基准差异 |
|---|---|---|
| 基准（唯一订单明细 `(order_id, sku_id)` + 已支付 + 金额有效 + **user_id 非空**） | 96,782,831.16 | — |
| **清洗前**（源表直算，含重复单/退款单/无效行） | 100,079,022.57 | **+3.406%** |
| **清洗后**（DWD 收口） | 96,782,831.16 | **+0.000%** |

真实数据本身没有订单表，`buy` 事件被映射成订单后，再叠加 9 类可控脏数据；
所以 3.406% 的差异可以逐项归因：**3% 退款单 + 0.3% 重复订单 + 空 user/空金额/负金额**
（基准与 DWD 都用同一套去重键与 `is_valid` 口径，因此收口后差异为 0）。
未来日期订单落在窗口外分区、ODS 不加载，因此不再制造"边界残差"。

> 历史故事保留：synthetic 模式下曾用"基准漏掉 `user_id` 非空"把 0.094% 修正到 0.029%，
> 那次教训（**一个正确的解释会盖住其他错误**）已经写进 `docs/指标口径.md` 的口径变更记录；
> 但对外引用当前数字时，以天池数据这一节为准。

### 6.2 倾斜：真实数据是长尾（193x），不是单键热点

```
倾斜量化（按 sku_id 聚合时的分组规模分布）
  参与聚合的订单行数    96,156
  热销 SKU 3122135     66 行（占比 0.07%）
  聚合分组数            69,684
  最大分组行数          193
  分组中位数            1
  最大 / 中位数         193.0x
```

真实天池数据里单个商品最多只占 0.07%，但 69,684 个 SKU 中有大量只出现 1 次的商品，
所以"最大 / 中位数"仍有 **193x**——这是**长尾型倾斜**，不是 synthetic 那种"一个 SKU 占 40%"
的**单键热点**。两者都会让 Spark 的某些 task 变慢，但治理方式不同。

**A 部分（DuckDB，零依赖）**：直接聚合 vs 加盐两阶段聚合，**结果逐行一致**
（69,684 个 SKU 分组，加盐后 stage1 分组数 ×8）。这是任何性能优化的前提。

**B 部分（真 Spark，`local[*]`，2026-11-08 实测）**：按 `sku_id` shuffle 后：

| shuffle key | 最大分区 | 最小分区 | 最大 / 最小 |
|---|---|---|---|
| `sku_id`（直接聚合） | **12,274** | 11,666 | **1.05x** |
| `sku_id + salt`（加盐后 stage1） | **12,166** | 11,833 | **1.03x** |

两条路径结果逐行一致。**加盐在这个真实样本上没有收益**，因为 6.9 万个 key 已经把
shuffle 摊得很均匀；这正是"salt 不是万能药"的证据：它只对**单键热点**有效。
要复现“单键热点 → 加盐削峰”的对照，请切回 synthetic：

```bash
python -m dw.cli gen --source synthetic
python spark/skew_experiment.py --spark   # synthetic 下实测 82,043 → 30,806（6.86x → 2.41x）
```

> 实现上有个踩过的坑：Spark 侧加盐用的是带种子的 `rand()`，**不是** `row_number() OVER (ORDER BY sku_id)`。
> 后者会触发 `No Partition Defined for Window operation! Moving all data to a single partition`。
> 另：Windows 下若项目路径含中文，`bin/spark-submit.cmd` 会把 classpath 传坏并报
> `Could not find or load main class org.apache.spark.deploy.SparkSubmit`；把 pyspark
> 以 junction 挂到纯 ASCII 路径并设 `SPARK_HOME` 可绕过（脚本头部有说明）。

**这个实验证明不了什么（同样要说清）**：9.6 万订单、490 万日志的量级远低于 Spark 的适用区间，
真跑起来 Spark 大概率**比 DuckDB 慢**——固定开销占了大头。价值不是"Spark 更快"，
而是"倾斜长什么样、什么时候加盐有用"，以及什么量级才值得从 DuckDB 换到 Spark。

### 6.3 各层产出

用 `python -m dw.cli tables` 实测（干净的单次全窗口跑批，窗口 2017-11-25 ~ 2017-12-03，9 天）：

| 层 | 表 | 行数 |
|---|---|---|
| ODS | `ods_order_info` | 99,660 |
| ODS | `ods_user_log` | 4,909,921 |
| DIM | `dim_sku` / `dim_user` / `dim_date` | 1,090,811 / 49,395 / 9 |
| DWD | `dwd_order_detail` | 99,363（其中有效 99,126，剔行率 0.24%） |
| DWD | `dwd_page_log` | 4,909,521 |
| DWD | `dwd_order_reject_audit` | 36 |
| DWS | `dws_traffic_user_1d` / `dws_trade_user_1d` | 344,618 / 64,426 |
| ADS | `ads_daily_metrics` / `ads_gmv_category_1d` / `ads_funnel_1d` | 9 / 17,724 / 27 |

质量报告 **11/12 通过**，唯一失败是告警级 `source_days_within_boundary`（注入的 5 个超前分区，不阻断下游）；
真实数据的漏斗单调性通过，不再是 synthetic 里的失败项。

> 关于这张表要提醒一句（也是这个项目踩过的真实坑）：**它必须是「一次干净跑批」的数字**。
> 如果表里同时躺着两代数据（例如换了结算日重新造数后重跑，而窗口不重叠），
> 行数会翻倍，而且**质量规则和对账都带 `dt BETWEEN 窗口` 过滤，完全看不见它们**。
> 现在 `runner` 在全窗口跑批时会自动清理窗口外的历史分区（`prune_out_of_window`，见第八节与 `dw.cli run --no-prune`），
> 清理行数会记进 `run_steps` 的 ods 步骤 message（形如 `pruned=362,800`），由 `tests/test_idempotent.py::test_full_window_run_prunes_ghost_partitions` 守住。

---

## 七、幂等与可重跑

每一层都是"按分区先删后插"或"全量重算"，所以**同一窗口重跑任意多次，结果逐行一致**。
这条性质由 3 个测试函数锁住（`tests/test_idempotent.py`，其中一个函数同时验证多项性质）：

- 重跑后各层指纹逐行相同
- 行数不翻倍（"分区覆盖漏写 DELETE"会立刻显形）
- GMV 不变（最直观的"有没有灌双份"探针）
- **只重跑最后一天时，其他天的数据原样保留**（对应真实运维的"补数"场景）

> 为什么这条很重要：如果重跑会灌双份数据，任何一次失败重试都是在往数仓里灌垃圾，
> 而且**不会报错**——只会让指标悄悄变大。

---

## 八、目录结构

```
ecommerce-dw/
├── src/dw/
│   ├── config.py             # 路径、规模、阈值、注入开关——所有可调项集中在此
│   ├── paths.py              # 路径解析与 SQL 模板渲染
│   ├── db.py                 # DuckDB（数仓）与 SQLite（运行元数据）连接
│   ├── runlog.py             # 运行链路记录（每步耗时/行数/状态）
│   ├── runner.py             # 编排：依赖顺序 + 质量门禁 + 阻断
│   ├── cli.py                # 命令行入口
│   ├── gen_source.py         # synthetic 造数 + 9 类脏数据注入
│   ├── import_taobao.py      # 天池 UserBehavior 导入/确定性采样/脏数据叠加
│   ├── layer_ods.py          # 各层执行器
│   ├── layer_dim.py
│   ├── layer_dwd.py
│   ├── layer_dws.py
│   ├── layer_ads.py
│   ├── quality.py            # 12 条质量规则 + 报告 + 阻断判定
│   ├── checks.py             # 对账与倾斜量化
│   └── export_dashboard.py   # 离线 HTML 看板
├── sql/                      # 全部建表与加工 SQL（含设计理由注释）
├── dags/                     # Airflow DAG（只做调度层，见 docs/airflow.md）
├── Dockerfile.airflow        # Airflow 镜像：Airflow 环境与数仓 venv 隔离
├── docker-compose.airflow.yml# Postgres + init + webserver + scheduler
├── spark/skew_experiment.py  # 倾斜对照实验（可选，见该文件说明）
├── Dataset/                  # 天池 CSV（3.6GB，本地，不入库；也可放 data/）
├── tests/                    # 32 项测试
├── docs/                     # 指标口径 / 数据字典
├── dashboard/out/report.html # 看板产物（可离线打开）
└── LICENSE                   # MIT
```

---

## 八点五、Airflow 调度（可选）

项目本身不依赖 Airflow；它只是把已有的 `dw.cli` 包成一条可调度、可重试、可补数的 DAG：

```bash
python -m dw.cli gen                       # 可选；DAG 的 ensure_data 会自动造数
$env:COMPOSE_BAKE="false"                  # 项目路径含中文时，Compose bake 会报错；关掉即可
docker compose -f docker-compose.airflow.yml up --build -d
# 或直接运行 .\start-airflow.ps1
# http://127.0.0.1:8081   admin / admin（演示值，生产必须改）
```

DAG：`ensure_data → run → quality → recon → dashboard`。默认 `run --limit 1`（T+1）；
在 Trigger DAG w/ config 里把 `full_refresh` 设为 `true` 才会跑完整窗口。

**边界**：不把五层拆成五个 task（会绕过质量门禁、且 DuckDB 单写者会锁库）；
`--limit 1` 的“最后一天”来自 `_baseline.json` 的 settle_date，不是 Airflow logical_date；
失败回调只写日志，没有接企业微信/钉钉。**首次部署建议先手动触发一次 `full_refresh=true`**：
日常 T+1 只重建一天，而 quality/recon 仍按完整窗口检查，空仓首日直接跑日批会把“只有一天数据”
误判成大幅缺失。详见 [`docs/airflow.md`](docs/airflow.md)。

---

## 九、测试

```bash
python -m pytest -q          # 32 passed
```

| 文件 | 覆盖 |
|---|---|
| `test_quality.py` | **每条规则对每类脏数据的灵敏度**（注入 → 命中；关掉注入 → 命中归零/下降）、剔除原因分布、阈值方向、规则名唯一；**漏斗单调性规则必须真的失败**、**日志侧丢弃量可见**、**迟到数据留在窗口外的源分区里（覆盖缺口的显式断言）** |
| `test_idempotent.py` | 重跑逐行一致、行数稳定、GMV 稳定、**只补一天不影响其他天**、**全窗口跑批会清理幽灵分区且把行数记进 trace** |
| `test_pipeline.py` | 分层完整、单日窗口的日环比规则标为 skipped、对账口径、倾斜真实存在、门禁确实阻断、`--force` 留痕、运行记录落库 |

**两个关键测试设计**：

1. **隔离运行**：需要反复改配置的测试跑在**独立临时目录 + 独立进程**里。
   链路写的是同一个 DuckDB 文件，在主进程里反复跑会让测试之间通过共享状态互相污染，
   表现为"单独跑能过、一起跑就挂"。这是最隐蔽的一类测试不稳定来源。
2. **灵敏度对照**：光验证"规则命中 > 0"不够——还得验证"关掉那类脏数据后命中降下来"，
   否则规则可能是在统计别的东西。

---

## 十、已知不足（如实说明）

- **单机 DuckDB，未做分布式（这是选型，不是缺陷）**。数据量在千万行级以内，DuckDB 的部署成本远低于起 Spark 集群；
  再往上（数十亿行、需要横向扩展）才需要换 Spark/Hive，并配套分区裁剪与倾斜处理。
  倾斜实验**已经在真 Spark（`local[*]`）上跑过**：天池真实样本是长尾（最大/中位 193x），
  但 shuffle 分区最大/最小只有 **1.05x**，加盐后 1.03x、结果逐行一致——说明加盐对"没有单键热点"的
  真实数据没有收益（见 6.2）。synthetic 的 40% 热销 SKU 才会复现 82,043 → 30,806（6.86x → 2.41x）。
  同时如实地记下：**这个量级下 Spark 只会比 DuckDB 慢**，固定开销占大头——
  所以结论是"知道什么时候该换、什么时候该加盐"，而不是"我用了 Spark"。
- **只有 T+1 批处理，没有实时链路**。Kafka + Flink 的实时 GMV 属于后续方向，未实现。
- **主线调度是自写 runner；另提供可选的 Airflow 调度层**（`dags/` + `docker-compose.airflow.yml`）。
  主链路仍不依赖 Airflow：`dw.cli run` 是一次性前台命令，`runner.py` 负责依赖顺序、质量门禁和幂等；
  Airflow 只在外层做定时、重试、补数入口和日志集中。**不把五层拆成五个 task**，因为那会绕过门禁，
  且 DuckDB 单写者并发会锁库。详见 [`docs/airflow.md`](docs/airflow.md)。
- **DWD 去重是"按天分区内、按 `(order_id, sku_id)`"（本轮已从 `order_id` 改正）**：
  当前天池导入与 synthetic 都是"一 buy 事件一订单"，两者结果相同；接入多商品订单后，
  按 `order_id` 去重会把同单多商品压成一行。
  **对账基准也已同步改为同一去重键**，订单数按 `count(DISTINCT order_id)` 统计，避免"基准按单、DWD 按明细行"的口径漂移。
  跨天重复的同一 `order_id` 也不会被当天分区内的去重消掉——需要全局去重表或唯一约束才能真正守住。
- **质量规则 12 条，未覆盖分布漂移与异常检测**。目前只有日环比波动这一条粗粒度监控；
  另外 `event_date_in_window` 现在检查的是"分区日期 vs 业务日期"，不再是恒真表达式；`source_days_within_boundary` 仅扫描订单分区，未来日期的日志分区仍不在它的覆盖范围内。
- **未做数据血缘可视化**，血缘只体现在 `_source_file` / `_batch_id` 两个元数据列上。
- **维度不做拉链表（SCD2）**，全量覆盖。需要历史状态回溯时见 [`docs/扩展方向.md`](docs/扩展方向.md)。
- **无鉴权、无多用户隔离**：本项目是单机演示，`data/` 目录即全部边界。
- **看板是静态 HTML**，无交互筛选。用它做"离线可交付的产物"，不替代真实 BI 平台。

---

## 十一、环境

Python 3.12（实测版本），依赖 `duckdb`、`pandas`、`numpy`（测试另需 `pytest`）。
`requirements.txt` 用 `==` 锁死实测通过的版本组合——README 印的“32 passed”只有在同一套依赖下才可复现。
真实数据模式另需把天池 `UserBehavior.csv` 放到 `Dataset/` 或 `data/`（约 3.6GB，不入库），
或用 `TAOBAO_DATASET` 指向文件路径。

```bash
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt   # Windows
.venv\Scripts\python.exe -m pip install -e ".[dev]"           # 必做：否则 python -m dw.cli 找不到 dw 包
```

> `numpy` 是 `gen_source.py` 直接 import 的依赖（造数用 `np.random.default_rng`），
> 所以它被显式写进了 `requirements.txt` / `pyproject.toml`——不能只靠 pandas 传递安装，那是隐式依赖。

环境变量（可选，都有默认值）：

| 变量 | 默认 | 用途 |
|---|---|---|
| `DW_ROOT` / `DW_DATA_DIR` | 项目根 / `data` | 路径覆盖（部署到数据盘、测试隔离） |
| `DW_WAREHOUSE` / `DW_META_DB` | `data/warehouse.duckdb` / `data/meta.sqlite` | 数仓与运行元数据文件 |

---

## 十二、命令速查

```bash
python -m dw.cli gen --source taobao # 天池真实数据（默认 mod=20，约 4.9 万用户）
python -m dw.cli gen --source taobao --user-mod 100  # 更小样本（约 1 万用户）
python -m dw.cli gen --source synthetic # 回退合成造数（含 40% 热销 SKU，供倾斜压测）
python -m dw.cli gen --scale 0.05    # synthetic 小样本（秒级，用于快速验证）
python -m dw.cli run                 # 全量跑批（含质量门禁与看板；会自动清理窗口外的历史分区）
python -m dw.cli run --limit 1       # 只跑窗口最后一天（模拟 T+1 日批；不清理窗口外分区）
python -m dw.cli run --force         # 忽略门禁强制出数（会留痕）
python -m dw.cli run --no-prune      # 全窗口跑批但**不**清理窗口外的历史分区
python -m dw.cli run --prune         # 强制清理窗口外的历史分区（补数后清幽灵分区用）
python -m dw.cli quality             # 只输出质量报告
python -m dw.cli recon               # 对账 + 倾斜量化
python -m dw.cli tables              # 列出数仓表与行数
python -m dw.cli runs                # 查看历史跑批记录
python -m dw.cli dashboard           # 只重新生成看板
python -m pytest -q                  # 跑测试
```

**关于 `--prune`（幽灵分区清理）**：ODS 的幂等是「按 `dt` 分区先删后插」，它只能保证
「这一天不会被写两遍」，**删不掉别的窗口留下的分区**。换了结算日重新造数后重跑，新旧窗口的日期
不重叠时，`ods_order_info` 里会同时留下两代数据；更麻烦的是质量规则与对账都带
`dt BETWEEN 窗口` 过滤，**完全看不见它们**（只有 `dw.cli tables` 的全表计数会暴露）。
所以全窗口跑批时会顺带删除窗口外（所有含 `dt` 列的表）的历史分区，行数记进 `run_steps`；
而 `--limit` 的补数场景默认**不**清理——窗口之外的那些天是合法数据。

---

## 许可证

MIT
