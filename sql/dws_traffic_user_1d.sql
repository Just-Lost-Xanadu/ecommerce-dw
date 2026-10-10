-- ============================================================================
-- DWS：汇总数据层 —— 用户天粒度流量汇总
-- ============================================================================
-- 轻量汇总（不是指标）：按 用户 × 天 聚合行为日志，给上层提供"半成品"。
--
-- 为什么要有 DWS 这一层，而不从 DWD 直接算指标：
--   1. **复用**：留存、活跃、漏斗都基于"用户天粒度"，算一次存下来，多个指标共享；
--   2. **性能**：DWD 是明细（千万级），ADS 每次从明细算会越来越慢；
--   3. **稳定**：指标口径变了只改 ADS，DWS 不变。
--   这是分层的核心价值——**每一层只解决一类问题**。
--
-- 幂等写法：**重建 + 排除当前分区**。
--   这一层是按天循环执行的，所以每次只重算"当天"，但要保留其他天。
--   写法是 `CREATE OR REPLACE TABLE ... AS (其他天 UNION 当天)`——
--   一次原子替换，既能重跑幂等，也**避开了 DuckDB 上逐行 DELETE 的高开销**
--   （实测：DELETE + INSERT 的写法在这一层要 43 秒，换成重建降到 1~2 秒级别）。
-- ============================================================================

CREATE TABLE IF NOT EXISTS dws_traffic_user_1d (
    user_id       VARCHAR,
    dt            DATE,
    pv            BIGINT,
    cart_cnt      BIGINT,
    fav_cnt       BIGINT,
    buy_cnt       BIGINT,
    stay_seconds  BIGINT,
    event_cnt     BIGINT,
    _batch_id     VARCHAR,
    PRIMARY KEY (user_id, dt)
);

CREATE OR REPLACE TABLE dws_traffic_user_1d AS
WITH other_days AS (
    -- 除当前分区外的历史数据：原样保留（这就是"分区覆盖"要保住的部分）
    SELECT * FROM dws_traffic_user_1d WHERE dt <> DATE {dt}
),
today AS (
    SELECT
        user_id,
        DATE {dt} AS dt,
        count(*) FILTER (WHERE event = 'pv')   AS pv,
        count(*) FILTER (WHERE event = 'cart') AS cart_cnt,
        count(*) FILTER (WHERE event = 'fav')  AS fav_cnt,
        count(*) FILTER (WHERE event = 'buy')  AS buy_cnt,
        sum(stay_seconds)                      AS stay_seconds,
        count(*)                               AS event_cnt,
        {batch_id} AS _batch_id
    FROM dwd_page_log
    WHERE dt = DATE {dt}
    GROUP BY user_id
)
SELECT * FROM other_days
UNION ALL
SELECT * FROM today;

-- CREATE OR REPLACE 会丢掉原表的约束，所以要显式补回主键。
-- 主键在这里不只是约束，它也让"同一用户同一天只有一行"成为**结构保证**，
-- 而不是靠上游 SQL 写对来保证。
ALTER TABLE dws_traffic_user_1d ADD PRIMARY KEY (user_id, dt);
