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

-- 去重：商品维度表的源是"商品档案 CSV"，同一个 sku_id 出现两行（改了价、改了类目）
-- 完全可能。dim_sku 的 sku_id 是 PRIMARY KEY，直接 INSERT 两行会撞主键、把整个 DIM
-- 事务回滚掉，报错还只是一句 Constraint Error。dim_user 早就按 user_id 取了 row_number，
-- 这里同口径：同一 sku_id 只留一行。排序键用商品自身属性（而不是"文件中第几行"，
-- CSV 扫描的行序不受保证），保证**同一份源文件两次跑批留下的是同一行**——
-- 去重一旦不确定，幂等测试就会偶发失败。
INSERT INTO dim_sku
SELECT sku_id, sku_name, category, brand, price, {batch_id} AS _batch_id
FROM (
    SELECT
        sku_id, sku_name, category, brand,
        TRY_CAST(price AS DOUBLE) AS price,
        row_number() OVER (
            PARTITION BY sku_id ORDER BY sku_name DESC, category DESC, brand DESC
        ) AS _rn
    FROM read_csv({src}, header=true, all_varchar=true, sample_size=-1)
    WHERE sku_id IS NOT NULL
)
WHERE _rn = 1;
