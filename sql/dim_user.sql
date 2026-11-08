-- ============================================================================
-- DIM：维度层 —— 用户维度
-- ============================================================================
-- 星型模型的"维"侧，事实表通过 user_id 关联到这里。
--
-- 全量覆盖写（本项目维度不做拉链表；SCD2 拉链表作为加分项见 docs/可选加分项.md）。
--
-- 一处真实的一致性处理：同一 user_id 若对应多个手机号，说明源系统有重复/冲突记录。
-- 处理方式：**保留一行，其余标记为冲突**（is_conflict=true），而不是随便取一条——
-- 随便取会让"这个用户是哪个手机号"变成不确定的，下游对账时会莫名其妙少数据。
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_user (
    user_id     VARCHAR PRIMARY KEY,
    phone       VARCHAR,
    city        VARCHAR,
    age         INTEGER,
    reg_date    DATE,
    is_conflict BOOLEAN,   -- true = 该 user_id 在源数据里存在多条冲突记录
    _batch_id   VARCHAR
);

INSERT INTO dim_user
WITH agg AS (
    SELECT
        user_id,
        count(*)              AS n_rows,
        count(DISTINCT phone) AS n_phones
    FROM read_csv({src}, header=true, all_varchar=true, sample_size=-1)
    WHERE user_id IS NOT NULL
    GROUP BY user_id
),
picked AS (
    SELECT
        s.user_id,
        s.phone, s.city,
        TRY_CAST(s.age AS INTEGER)   AS age,
        TRY_CAST(s.reg_date AS DATE) AS reg_date,
        (a.n_rows > 1 OR a.n_phones > 1) AS is_conflict,
        row_number() OVER (
            PARTITION BY s.user_id
            ORDER BY TRY_CAST(s.reg_date AS DATE) DESC, s.phone
        ) AS rn
    FROM read_csv({src}, header=true, all_varchar=true, sample_size=-1) s
    JOIN agg a USING (user_id)
)
SELECT
    user_id, phone, city, age, reg_date,
    is_conflict,
    {batch_id} AS _batch_id
FROM picked
WHERE rn = 1;
