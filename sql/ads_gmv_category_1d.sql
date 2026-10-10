-- ============================================================================
-- ADS：应用数据层 —— 品类 GMV 报表
-- ============================================================================
-- 全量重算。按 天 × 一级类目 汇总成交金额。
--
-- 一个必须处理的细节：**孤儿商品**（订单里的 sku_id 在商品维表里不存在）。
--   现在用 LEFT JOIN + coalesce 归到「未知类目」，而不是让这些行消失。
--   如果直接 INNER JOIN，这些订单会**静默消失**，GMV 少了却查不出原因——
--   这是数据开发里最典型的"数字对不上"来源之一。
--   归到一个显式的「未知类目」后，它变成**可见**的：报表上直接能看到有多少金额
--   落在这个类目里，质量规则也会单独监控它的占比。
-- ============================================================================

CREATE TABLE IF NOT EXISTS ads_gmv_category_1d (
    dt           DATE,
    category     VARCHAR,
    order_cnt    BIGINT,
    sku_cnt      BIGINT,
    gmv          DOUBLE,
    user_cnt     BIGINT,
    _batch_id    VARCHAR,
    PRIMARY KEY (dt, category)
);

DELETE FROM ads_gmv_category_1d
WHERE dt BETWEEN DATE {window_start} AND DATE {window_end};

INSERT INTO ads_gmv_category_1d
SELECT
    o.dt,
    coalesce(s.category, '未知类目') AS category,
    count(DISTINCT o.order_id)       AS order_cnt,
    sum(o.quantity)                  AS sku_cnt,
    round(sum(o.amount), 2)          AS gmv,
    count(DISTINCT o.user_id)        AS user_cnt,
    {batch_id} AS _batch_id
FROM dwd_order_detail o
LEFT JOIN dim_sku s ON s.sku_id = o.sku_id
WHERE o.dt BETWEEN DATE {window_start} AND DATE {window_end}
  AND o.is_valid
  AND o.is_paid
GROUP BY o.dt, 2
ORDER BY o.dt, gmv DESC;
