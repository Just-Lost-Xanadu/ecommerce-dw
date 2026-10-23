-- ============================================================================
-- DWD：明细数据层 —— 订单明细（事实表）
-- ============================================================================
-- 粒度：**一行 = 一个订单里的一个商品**（order_id + sku_id 唯一）
--   （不是"一行一订单"。因为要按品类/商品分析，订单粒度做不到；
--     粒度必须在建表时定死，改粒度等于重建整条链）
--   ⚠️ 当前造数器一单一商品，所以 (order_id, sku_id) 与 order_id 等价；
--      但去重键按 (order_id, sku_id) 写，多商品订单才不会被压成一行。
--
-- 这一层做四件事：
--   ① 去重：同一 (order_id, sku_id) 只保留一行（优先保留脏行作为证据，见下）——解决重复订单
--   ② 有效性判定：无效行**不删除，标记 reject_reason**——这样"少了多少"可解释
--   ③ 类型与单位统一
--   ④ 写审计表：把被剔除的原因分布落下来，供质量报告使用
--
-- 为什么"标记"而不是"删除"：
--   如果直接删掉，报表和业务库对不上账时你只能猜"是不是清洗洗掉了"；
--   标记之后可以直接查 `WHERE reject_reason IS NOT NULL` 看剔了多少、为什么剔。
-- ============================================================================

CREATE TABLE IF NOT EXISTS dwd_order_detail (
    order_id      VARCHAR,
    user_id       VARCHAR,
    sku_id        VARCHAR,
    sku_name      VARCHAR,      -- 冗余商品名：报表展示用，避免每次回联维表
    quantity      INTEGER,
    unit_price    DOUBLE,
    amount        DOUBLE,
    order_date    DATE,
    order_status  VARCHAR,
    channel       VARCHAR,
    is_paid       BOOLEAN,      -- 是否已支付（退款单为 false）
    is_valid      BOOLEAN,      -- 是否通过有效性校验
    reject_reason VARCHAR,      -- 被标记为无效的原因（有效行 = NULL）
    _batch_id     VARCHAR,
    dt            DATE
);

CREATE TABLE IF NOT EXISTS dwd_order_reject_audit (
    dt            DATE,
    reject_reason VARCHAR,
    rows_rejected BIGINT,
    _batch_id     VARCHAR,
    PRIMARY KEY (dt, reject_reason)
);

-- ---- 订单明细 ----
-- 分区覆盖写：先删掉本分区，再插入。
-- 少了这句 DELETE，重跑就会**把同一批数据插第二遍**——
-- 主键唯一性规则会立刻抓到（唯一率掉到 0.5），这就是质量门禁存在的意义。
DELETE FROM dwd_order_detail WHERE dt = DATE {dt};

INSERT INTO dwd_order_detail
WITH ranked AS (
    -- 去重键：(order_id, sku_id) —— 与建表声明的粒度一致。
    --   **不能只用 order_id**：真实订单表里一个订单常有多个商品行，按 order_id 去重
    --   会让多商品订单只剩金额最大的一条，其余商品的 quantity/amount 静默消失，
    --   ADS 的 GMV / 类目占比 / sku_cnt 全部少算，而且没有任何规则会报红
    --   （它们被当成"重复订单"删掉了，看着像数据很干净）。
    --   当前造数器是一单一商品，两种写法结果相同；差别只在接入真实多商品订单那天。
    -- 排序规则如下：
    --   ① user_id IS NULL DESC —— 主键字段缺失的行**排在最前面，优先留下来当证据**
    --   ② amount IS NULL DESC —— 同理，金额缺失的行也要留下
    --   ③ _load_time DESC —— 常规去重语义：同等干净程度下保留更晚进仓的那份
    --   ④ amount DESC —— 其余情况按金额降序，保证结果确定（不依赖物理行序）
    --
    -- 第 ①② 条（把脏行排在前面）是这个项目踩过的一个真实坑，值得记下来：
    --   原来写的是 `ORDER BY _load_time DESC, amount DESC NULLS LAST`。
    --   而同一批导入的重复行 _load_time 完全相同（同一个事务里写进去的），
    --   于是排序落到第二键，`NULLS LAST` 把"金额为空"的那一行排到后面，
    --   去重之后**脏数据被静默丢弃**：DWD 报出 0 行金额为空，
    --   看起来像"源数据很干净"，实际上脏行被悄悄顶掉了。
    --   危害在于**让质量问题不可见**：质量报告全绿，但报告本身是假的。
    --   所以去重的排序必须显式保证"有问题的行不会被自动淘汰"——
    --   去重的目标是"同一业务事实只留一份"，不是"顺手把脏数据洗掉"。
    SELECT
        *,
        row_number() OVER (
            PARTITION BY order_id, sku_id
            ORDER BY
                (user_id IS NULL) DESC,
                (amount IS NULL) DESC,
                _load_time DESC,
                amount DESC
        ) AS rn
    FROM ods_order_info
    WHERE dt = DATE {dt}
),
deduped AS (
    SELECT * FROM ranked WHERE rn = 1
),
judged AS (
    SELECT
        order_id, user_id, sku_id, quantity, unit_price, amount,
        order_date, order_status, channel,
        (order_status = 'paid') AS is_paid,
        CASE
            WHEN user_id IS NULL         THEN 'user_id_null'
            WHEN amount IS NULL          THEN 'amount_null'
            WHEN amount <= 0             THEN 'amount_not_positive'
            WHEN order_date > DATE {settle} THEN 'date_in_future'
            ELSE NULL
        END AS reject_reason
    FROM deduped
)
SELECT
    j.order_id, j.user_id, j.sku_id,
    s.sku_name,                      -- 孤儿商品在这里为 NULL，是可见的（不是消失）
    j.quantity, j.unit_price, j.amount,
    j.order_date, j.order_status, j.channel,
    j.is_paid,
    (j.reject_reason IS NULL) AS is_valid,
    j.reject_reason,
    {batch_id} AS _batch_id,
    DATE {dt} AS dt
FROM judged j
LEFT JOIN dim_sku s ON s.sku_id = j.sku_id;

-- ---- 剔除审计（同一分区先清后写，保证重跑幂等）----
DELETE FROM dwd_order_reject_audit WHERE dt = DATE {dt};

INSERT INTO dwd_order_reject_audit
SELECT
    DATE {dt} AS dt,
    coalesce(reject_reason, 'kept') AS reject_reason,
    count(*) AS rows_rejected,
    {batch_id} AS _batch_id
FROM dwd_order_detail
WHERE dt = DATE {dt}
GROUP BY 2;
