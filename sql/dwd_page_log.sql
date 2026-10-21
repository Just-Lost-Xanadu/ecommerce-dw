-- ============================================================================
-- DWD：明细数据层 —— 页面访问日志
-- ============================================================================
-- 粒度：一行 = 一次行为事件（pv / cart / fav / buy）
--
-- 清洗内容：
--   ① 丢掉 user_id 为空的行为（无法归属到人，做不了 UV / 漏斗）
--   ② **丢掉超出统计窗口的迟到数据**：分区日期在 [窗口开始, 结算日] 之外的记录
--      —— 这类数据不是"错"，而是"来晚了"；在 T+1 的批处理里，它属于上一批该处理的范围。
--      质量规则会单独监控它，而不是静默丢掉（见 quality 的 timeliness 规则）。
--   ③ 保留原始 event 值，枚举合法性由质量规则校验（不在 DWD 里静默改写）
-- ============================================================================

CREATE TABLE IF NOT EXISTS dwd_page_log (
    log_id        VARCHAR,
    user_id       VARCHAR,
    sku_id        VARCHAR,
    event         VARCHAR,
    log_date      DATE,
    stay_seconds  INTEGER,
    _batch_id     VARCHAR,
    dt            DATE
);

DELETE FROM dwd_page_log WHERE dt = DATE {dt};

INSERT INTO dwd_page_log
SELECT
    log_id, user_id, sku_id, event,
    log_date, stay_seconds,
    {batch_id} AS _batch_id,
    DATE {dt}  AS dt
FROM ods_user_log
WHERE dt = DATE {dt}
  AND user_id IS NOT NULL
  AND log_date BETWEEN DATE {window_start} AND DATE {settle};
