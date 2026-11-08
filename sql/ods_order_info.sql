-- ============================================================================
-- ODS：操作数据存储层（原始落地）
-- ============================================================================
-- 职责：把源数据**原样**搬进数仓，不做任何业务清洗。
--
-- 为什么必须原样：
--   1. 出了口径问题要能回溯到"源数据到底是什么样"，清洗后再查就查不到了；
--   2. 清洗规则会变，源数据不会变——留着原样才能重跑新的清洗逻辑；
--   3. 脏数据是**证据**。ODS 保留脏行，DWD 才标记剔除，这样"数据少了多少"是可解释的。
--
-- 分区：dt 落**文件分区日期**（不是 order_date），这样即使文件里混入错日期行也原样入仓；
--      按分区覆盖写是幂等的关键：重跑同一天只替换那一天，不会产生重复数据。
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods_order_info (
    order_id      VARCHAR,
    user_id       VARCHAR,
    sku_id        VARCHAR,
    quantity      INTEGER,
    unit_price    DOUBLE,
    amount        DOUBLE,
    order_date    DATE,
    order_status  VARCHAR,
    channel       VARCHAR,
    -- 元数据列：记录这行是什么时候、从哪个批次进来的（数据血缘的最小实现）
    _load_time    TIMESTAMP,
    _source_file  VARCHAR,
    _batch_id     VARCHAR,
    dt            DATE
);

-- 从 CSV 覆盖写入指定分区。
-- {src} 由 Python 注入（带引号的绝对路径），避免相对路径随启动目录变化。
INSERT INTO ods_order_info
SELECT
    order_id, user_id, sku_id,
    TRY_CAST(quantity AS INTEGER)  AS quantity,
    TRY_CAST(unit_price AS DOUBLE) AS unit_price,
    TRY_CAST(amount AS DOUBLE)     AS amount,
    TRY_CAST(order_date AS DATE)   AS order_date,
    order_status, channel,
    now()                          AS _load_time,
    {src_name}                     AS _source_file,
    {batch_id}                     AS _batch_id,
    DATE {dt}                      AS dt
FROM read_csv({src}, header=true, all_varchar=true, sample_size=-1);
