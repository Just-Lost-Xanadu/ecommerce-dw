-- ============================================================================
-- ODS：行为日志原始落地
-- ============================================================================
-- 与订单表同理：原样入仓 + 元数据列 + 按 dt 分区覆盖写。
-- 注意这里也**不过滤**"超出统计窗口的迟到数据"——那是 DWD 的职责。
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods_user_log (
    log_id        VARCHAR,
    user_id       VARCHAR,
    sku_id        VARCHAR,
    event         VARCHAR,
    log_date      DATE,
    stay_seconds  INTEGER,
    _load_time    TIMESTAMP,
    _source_file  VARCHAR,
    _batch_id     VARCHAR,
    dt            DATE
);

INSERT INTO ods_user_log
SELECT
    log_id, user_id, sku_id, event,
    TRY_CAST(log_date AS DATE)     AS log_date,
    TRY_CAST(stay_seconds AS INTEGER) AS stay_seconds,
    now()                          AS _load_time,
    {src_name}                     AS _source_file,
    {batch_id}                     AS _batch_id,
    TRY_CAST(log_date AS DATE)     AS dt
FROM read_csv({src}, header=true, all_varchar=true, sample_size=-1)
WHERE TRY_CAST(log_date AS DATE) = DATE {dt};
