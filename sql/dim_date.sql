-- ============================================================================
-- DIM：日期维度
-- ============================================================================
-- 预生成连续日期，供指标表做"补齐"使用。
--
-- 为什么需要一个日期维度：如果某天一个订单都没有，直接从事实表 group by 出来的
-- 指标表会**缺这一天**，前端报表就会出现断点，看起来像"数据丢了"。
-- 有了日期维度才能 LEFT JOIN 出"这一天确实没有数据（值为 0）"。
-- 这是数据开发里很常见的一个坑，也是"报表为什么断了一天"的标准答案。
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_key   DATE PRIMARY KEY,
    year       INTEGER,
    month      INTEGER,
    day        INTEGER,
    weekday    INTEGER,     -- 0=周一 ... 6=周日
    weekday_cn VARCHAR,
    is_weekend BOOLEAN
);

DELETE FROM dim_date
WHERE date_key BETWEEN DATE {window_start} AND DATE {window_end};

INSERT INTO dim_date
SELECT
    d::DATE AS date_key,
    year(d)  AS year,
    month(d) AS month,
    day(d)   AS day,
    (dayofweek(d) + 6) % 7 AS weekday,
    ['周一','周二','周三','周四','周五','周六','周日'][(dayofweek(d) + 6) % 7 + 1] AS weekday_cn,
    ((dayofweek(d) + 6) % 7) >= 5 AS is_weekend
FROM generate_series(DATE {window_start}, DATE {window_end}, INTERVAL 1 DAY) AS t(d);
