-- ============================================================================
-- ADS：应用数据层 —— 每日核心指标
-- ============================================================================
-- 全量重算（这张表只有几十行，重算成本可忽略），保证**任何一天的数字都基于同一套口径**。
--
-- 口径说明（每个指标都要能回答"为什么是这个数"）：
--   dau            当日产生过任意行为事件的去重用户数
--   new_users      当日首次出现行为的用户数。首次 = 在本统计窗口内最早出现的那天
--   pv             当日页面访问次数
--   visit_users    当日有 pv 的去重用户数（≠ dau：可能有只加购不浏览的用户）
--   order_cnt      当日有效且已支付订单数（已去重）
--   gmv            当日有效且已支付金额合计（**净 GMV：不含退款、不含重复单**）
--   pay_users      当日有效且已支付订单的去重用户数
--   aov            客单价 = gmv / order_cnt
--   pay_conversion 支付转化率 = pay_users / visit_users（人数口径，非次数口径）
--   dau_arpu       人均收入 = gmv / dau
--   top1_sku_share 最大单商品 GMV 占比（用来观察数据分布是否倾斜）
--
-- 已知局限（写在 README 里，不藏着）：
--   转化率是**同日**人数口径。用户今天浏览、明天下单，不会被算进今天的转化——
--   这是"当日转化"的口径，不是"用户级漏斗转化"，两者数字不同且都有用途。
-- ============================================================================

CREATE TABLE IF NOT EXISTS ads_daily_metrics (
    dt               DATE PRIMARY KEY,
    dau              BIGINT,
    new_users        BIGINT,
    pv               BIGINT,
    visit_users      BIGINT,
    order_cnt        BIGINT,
    gmv              DOUBLE,
    pay_users        BIGINT,
    aov              DOUBLE,
    pay_conversion   DOUBLE,
    dau_arpu         DOUBLE,
    top1_sku_share   DOUBLE,
    _batch_id        VARCHAR
);

-- ---- 用户首次出现日期（用于判定新用户）----
-- “首次”锚定在**仓库当前保留的历史**（DWS 全表）里，而不是只看本窗口：
-- 这样“只补最后一天”与“全窗口重跑”得到同一个 new_users（幂等测试锁着这条性质）。
-- 代价是空仓直接跑 --limit 1 时，当天所有人都会被算成新用户；README 已如实写明
-- 这个口径边界，不声称它是“生命周期首次”。
CREATE OR REPLACE TEMP TABLE _first_seen AS
SELECT user_id, min(dt) AS first_dt
FROM dws_traffic_user_1d
GROUP BY user_id;

-- ---- 行为侧指标 ----
CREATE OR REPLACE TEMP TABLE _traffic AS
SELECT
    d.date_key AS dt,
    count(DISTINCT t.user_id)                            AS dau,
    count(DISTINCT t.user_id) FILTER (
        WHERE f.first_dt = d.date_key
    )                                                    AS new_users,
    coalesce(sum(t.pv), 0)                                AS pv,
    count(DISTINCT t.user_id) FILTER (WHERE t.pv > 0)     AS visit_users
FROM dim_date d
LEFT JOIN dws_traffic_user_1d t ON t.dt = d.date_key
LEFT JOIN _first_seen f ON f.user_id = t.user_id
WHERE d.date_key BETWEEN DATE {window_start} AND DATE {window_end}
GROUP BY d.date_key;

-- ---- 交易侧指标 ----
CREATE OR REPLACE TEMP TABLE _trade AS
SELECT
    d.date_key AS dt,
    coalesce(sum(tr.order_cnt), 0)   AS order_cnt,
    coalesce(sum(tr.pay_amount), 0)  AS gmv,
    count(DISTINCT tr.user_id)       AS pay_users
FROM dim_date d
LEFT JOIN dws_trade_user_1d tr ON tr.dt = d.date_key
WHERE d.date_key BETWEEN DATE {window_start} AND DATE {window_end}
GROUP BY d.date_key;

-- ---- 单商品集中度（倾斜的量化证据）----
-- 注意：外层不能再引用 amount——它在子查询里已经被聚合成 sku_gmv 了。
-- 这个错的根因是"写了别名还去引用原列"，在 SQL 里很常见，DuckDB 会直接报 Binder Error。
CREATE OR REPLACE TEMP TABLE _sku_share AS
SELECT
    dt,
    CASE WHEN sum(sku_gmv) > 0
         THEN max(sku_gmv) / sum(sku_gmv)
         ELSE 0 END AS top1_sku_share
FROM (
    SELECT dt, sku_id, sum(amount) AS sku_gmv
    FROM dwd_order_detail
    WHERE is_valid AND is_paid
      AND dt BETWEEN DATE {window_start} AND DATE {window_end}
    GROUP BY dt, sku_id
) GROUP BY dt;

-- ---- 合并 ----
DELETE FROM ads_daily_metrics
WHERE dt BETWEEN DATE {window_start} AND DATE {window_end};

INSERT INTO ads_daily_metrics
SELECT
    tr.dt,
    tr.dau,
    tr.new_users,
    tr.pv,
    tr.visit_users,
    td.order_cnt,
    round(td.gmv, 2) AS gmv,
    td.pay_users,
    CASE WHEN td.order_cnt > 0 THEN round(td.gmv / td.order_cnt, 2) ELSE 0 END AS aov,
    CASE WHEN tr.visit_users > 0
         THEN round(td.pay_users * 1.0 / tr.visit_users, 4) ELSE 0 END AS pay_conversion,
    CASE WHEN tr.dau > 0 THEN round(td.gmv / tr.dau, 2) ELSE 0 END AS dau_arpu,
    round(coalesce(s.top1_sku_share, 0), 4) AS top1_sku_share,
    {batch_id} AS _batch_id
FROM _traffic tr
JOIN _trade td USING (dt)
LEFT JOIN _sku_share s USING (dt)
ORDER BY tr.dt;
