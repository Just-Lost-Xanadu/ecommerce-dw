-- ============================================================================
-- ADS：应用数据层 —— 转化漏斗
-- ============================================================================
-- 全量重算。**人数口径**（不是次数口径）：每个阶段的"人数"是该阶段行为发生过的去重用户数。
--
-- 口径必须写清楚（面试会追问）：
--   · 浏览 = 当日产生过任意行为事件的用户
--   · 加购 = 当日有 `cart` 事件的用户
--   · 下单 = 当日有 `buy` 事件的用户（**行为口径**）
--
--   ⚠️ 漏斗的"下单"用的是**行为日志**里的 buy 事件，不是订单表。
--   原因：漏斗要的是"用户在站内的行为序列"，用行为日志才自洽
--   （订单表的支付可能发生在浏览的第二天，混进来会让"同日漏斗"失真）。
--   订单侧的**真实支付人数与金额**在 `ads_daily_metrics` 里，两者不是一个口径，
--   **不要交叉相加**。这一点在 README 的"指标口径"里也写明了。
-- ============================================================================

CREATE TABLE IF NOT EXISTS ads_funnel_1d (
    dt                DATE,
    stage             VARCHAR,
    stage_order       INTEGER,
    user_cnt          BIGINT,
    conversion_rate   DOUBLE,   -- 相对上一阶段的转化率
    _batch_id         VARCHAR,
    PRIMARY KEY (dt, stage)
);

DELETE FROM ads_funnel_1d
WHERE dt BETWEEN DATE {window_start} AND DATE {window_end};

CREATE OR REPLACE TEMP TABLE _funnel AS
SELECT
    d.date_key AS dt,
    count(DISTINCT l.user_id)                                 AS view_users,
    count(DISTINCT l.user_id) FILTER (WHERE l.event = 'cart')  AS cart_users,
    count(DISTINCT l.user_id) FILTER (WHERE l.event = 'buy')   AS buy_users
FROM dim_date d
LEFT JOIN dwd_page_log l ON l.dt = d.date_key
WHERE d.date_key BETWEEN DATE {window_start} AND DATE {window_end}
GROUP BY d.date_key;

INSERT INTO ads_funnel_1d
SELECT dt, 'view', 1, view_users, 1.0, {batch_id} FROM _funnel
UNION ALL
SELECT
    dt, 'cart', 2, cart_users,
    CASE WHEN view_users > 0 THEN round(cart_users * 1.0 / view_users, 4) ELSE 0 END,
    {batch_id}
FROM _funnel
UNION ALL
SELECT
    dt, 'buy', 3, buy_users,
    CASE WHEN cart_users > 0 THEN round(buy_users * 1.0 / cart_users, 4) ELSE 0 END,
    {batch_id}
FROM _funnel;
