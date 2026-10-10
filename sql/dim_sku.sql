-- ============================================================================
-- DIM：维度层 —— 商品维度
-- ============================================================================
-- 事实表通过 sku_id 关联到这里。**价格取自维度表，而不是订单里的 unit_price**——
-- 因为维度的价格是"当前标准价"，而订单里的价格是"成交价"，两者语义不同：
--   分析成交金额用订单里的 amount；
--   分析"按标准价算的品类结构"用维度价。
-- 混用会导致口径不一致，这是数据开发里很常见的一类错误。
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_sku (
    sku_id    VARCHAR PRIMARY KEY,
    sku_name  VARCHAR,
    category  VARCHAR,
    brand     VARCHAR,
    price     DOUBLE,
    _batch_id VARCHAR
);

INSERT INTO dim_sku
SELECT
    sku_id, sku_name, category, brand,
    TRY_CAST(price AS DOUBLE) AS price,
    {batch_id} AS _batch_id
FROM read_csv({src}, header=true, all_varchar=true, sample_size=-1)
WHERE sku_id IS NOT NULL;
