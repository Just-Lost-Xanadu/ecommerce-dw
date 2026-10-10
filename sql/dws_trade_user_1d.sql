-- ============================================================================
-- DWS：汇总数据层 —— 用户天粒度交易汇总
-- ============================================================================
-- 只统计**有效且已支付**的订单（is_valid AND is_paid）。
--
-- 这里是"对不上账"那个技术难点的落点：
--   报表 GMV 与业务库对不上时，根因通常在**这一层之前的口径**——
--   重复订单没去重、退款单没剔除、无效行没排除。
--   修复方式不是让每个上层各修一遍，而是**在 DWD 统一收口**，
--   也就是这一层只认 `is_valid AND is_paid` 这一个口径。
--
-- 为什么收口在 DWD 而不是 ADS：
--   如果在每张 ADS 里各自过滤，改一次口径要改 N 处，迟早出现"这张表算对了、
--   那张表忘了改"——这正是口径不一致最常见的来源。
--
-- 幂等写法同 dws_traffic_user_1d：重建 + 排除当前分区（避开 DuckDB 的 DELETE 开销）。
-- ============================================================================

CREATE TABLE IF NOT EXISTS dws_trade_user_1d (
    user_id       VARCHAR,
    dt            DATE,
    order_cnt     BIGINT,
    sku_cnt       BIGINT,
    pay_amount    DOUBLE,
    _batch_id     VARCHAR,
    PRIMARY KEY (user_id, dt)
);

CREATE OR REPLACE TABLE dws_trade_user_1d AS
WITH other_days AS (
    SELECT * FROM dws_trade_user_1d WHERE dt <> DATE {dt}
),
today AS (
    SELECT
        user_id,
        DATE {dt} AS dt,
        count(DISTINCT order_id) AS order_cnt,
        sum(quantity)            AS sku_cnt,
        -- 金额聚合必须**定精度**：DOUBLE 累加的结果依赖求和顺序，
        -- 而重跑时行的物理顺序会变（重建表 + UNION ALL 的两段顺序不同），
        -- 于是同一个用户同一天会算出 8660.22 和 8660.220000000001 两个值——
        -- 业务上完全等价，但让"重跑结果逐行一致"这条性质无法验证。
        -- 真实项目里金额一律用 DECIMAL 或统一 ROUND，这里用 ROUND 收敛到分。
        round(sum(amount), 2)    AS pay_amount,
        {batch_id} AS _batch_id
    FROM dwd_order_detail
    WHERE dt = DATE {dt}
      AND is_valid
      AND is_paid
    GROUP BY user_id
)
SELECT * FROM other_days
UNION ALL
SELECT * FROM today;

-- CREATE OR REPLACE 会丢掉约束，显式补回主键（同 dws_traffic_user_1d 的说明）
ALTER TABLE dws_trade_user_1d ADD PRIMARY KEY (user_id, dt);

-- 顺带说明：DWS 层用 ROUND 收敛金额，是为了让"重跑结果逐行一致"可以被验证。
-- 上游 DWD 保留原始精度（明细不该被改写），精度收敛只发生在**汇总层**——
-- 这也是真实数仓的常规做法：明细保真，汇总定精度。
