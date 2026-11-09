-- ============================================================================
-- DWD：明细数据层 —— 页面访问日志
-- ============================================================================
-- 粒度：一行 = 一次行为事件（pv / cart / fav / buy）
--
-- 清洗内容：
--   ① 丢掉 user_id 为空的行为（无法归属到人，做不了 UV / 漏斗）
--   ② **丢掉超出统计窗口的迟到数据**：业务日期在 [窗口开始, 结算日] 之外的记录
--      —— 这类数据不是"错"，而是"来晚了"；在 T+1 的批处理里，它属于上一批该处理的范围。
--      注意：迟到数据若在窗口外的分区文件里，ODS 根本不会加载，quality 的
--      event_date_in_window 看不到它；这条只处理"已进入窗口分区、但业务日期不在窗口"的行。
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

-- 去重：一个 log_id 是**一次事件**的唯一标识，源日志重复上报（客户端重试、采集端
-- 至少一次投递）时会落两行。不去重的话 UV 不受影响（DISTINCT user_id），但
-- `ads_funnel_1d` 的加购/收藏/下单**次数**会翻倍——漏斗第二步就假了。
-- 保留规则定成确定序（先按 stay_seconds 大、再按 log_date 新），保证同一份源
-- 两次跑批留下同一行，否则幂等测试会偶发失败。ods_user_log 没去重是有意的：
-- ODS 是原始层，只做"这天不写两遍"，脏数据留给这里收口。
INSERT INTO dwd_page_log
SELECT log_id, user_id, sku_id, event, log_date, stay_seconds,
       {batch_id} AS _batch_id, DATE {dt} AS dt
FROM (
    SELECT
        log_id, user_id, sku_id, event, log_date, stay_seconds,
        row_number() OVER (
            PARTITION BY log_id
            ORDER BY stay_seconds DESC NULLS LAST, log_date DESC
        ) AS _rn
    FROM ods_user_log
    WHERE dt = DATE {dt}
      AND user_id IS NOT NULL
      AND log_date BETWEEN DATE {window_start} AND DATE {settle}
)
WHERE _rn = 1;
