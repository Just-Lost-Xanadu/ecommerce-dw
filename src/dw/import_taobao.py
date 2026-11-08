"""把阿里天池 UserBehavior.csv 导入成本项目的四张源表。

数据源：https://tianchi.aliyun.com/dataset/649
字段：user_id, item_id, category_id, behavior_type(pv/cart/fav/buy), timestamp(秒)

为什么需要导入层：
- 真实数据只有**行为日志**，没有订单金额、商品价格、用户画像与订单状态；
- 本项目下游已经按 ODS/DIM/DWD/DWS/ADS 固定了口径，直接改 SQL 会把"真实数据"
  和"数仓建模"耦死。导入层只做一件事：把真实行为映射成项目既有的源表 schema，
  并明确标注哪些字段是**派生**的（价格/渠道/年龄等），避免把模拟值伪装成原始值。

采样：
- 全量 1 亿行、987,994 用户，直接跑完整链路会让本地 DuckDB 演示失去意义；
- 用 `user_id % user_mod == 0` 做**确定性采样**（默认 mod=20，约 5 万用户），
  保留这些用户的**全部**行为事件，既缩小规模又不破坏用户内序列；
- 采样规则写进 `_baseline.json`，任何人可以用同一份 CSV + 同一个 mod 复算。
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from dw import config

TAOBAO_SETTLE_DATE = date(2017, 12, 3)
TAOBAO_DAYS = 9
TAOBAO_DEFAULT_USER_MOD = 20
CITIES = ["北京", "上海", "广州", "深圳", "成都", "重庆", "杭州", "武汉", "西安", "南京"]

_COLUMNS = {
    "user_id": "BIGINT",
    "item_id": "BIGINT",
    "category_id": "BIGINT",
    "behavior_type": "VARCHAR",
    "timestamp": "BIGINT",
}


def dataset_path() -> Path:
    """数据集路径：优先 TAOBAO_DATASET，其次项目根 Dataset/UserBehavior.csv。"""
    raw = os.getenv("TAOBAO_DATASET")
    if raw and raw.strip():
        p = Path(raw.strip()).expanduser()
        return p if p.is_absolute() else (config.ROOT / p).resolve()
    return (config.ROOT / "Dataset" / "UserBehavior.csv").resolve()


def _sql_literal(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


def _copy(con: duckdb.DuckDBPyConnection, sql: str, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY ({sql}) TO {_sql_literal(out)} "
        "(FORMAT CSV, HEADER true, DELIMITER ',', QUOTE '\"')"
    )


def import_taobao(
    csv_path: str | Path | None = None,
    user_mod: int = TAOBAO_DEFAULT_USER_MOD,
    verbose: bool = True,
) -> dict:
    """导入真实行为数据，返回与 `gen_source.generate()` 同形状的统计信息。"""
    if not isinstance(user_mod, int) or isinstance(user_mod, bool) or user_mod < 1:
        raise ValueError(f"user_mod 必须是 >=1 的整数，收到 {user_mod!r}")
    path = Path(csv_path).resolve() if csv_path else dataset_path()
    if not path.is_file():
        raise FileNotFoundError(
            f"找不到天池数据集 {path}。请下载 UserBehavior.csv 放到 Dataset/，"
            "或设置 TAOBAO_DATASET 指向文件。"
        )

    dates = [TAOBAO_SETTLE_DATE - timedelta(days=i) for i in range(TAOBAO_DAYS - 1, -1, -1)]
    start, end = dates[0].isoformat(), dates[-1].isoformat()
    out = Path(config.RAW_DIR)
    out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        # 只保留窗口内 + 采样用户；真实数据里窗口外的时间戳仅 1580 行左右，直接排除。
        con.execute(f"""
            CREATE TEMP TABLE base AS
            SELECT
                user_id, item_id, category_id, behavior_type,
                epoch_ms(timestamp * 1000) AS ts,
                epoch_ms(timestamp * 1000)::DATE AS dt
            FROM read_csv(
                {_sql_literal(path)}, header=false,
                columns={_COLUMNS!r}, sample_size=100000
            )
            WHERE user_id > 0 AND item_id > 0 AND category_id > 0
              AND behavior_type IN ('pv', 'cart', 'fav', 'buy')
              AND epoch_ms(timestamp * 1000)::DATE BETWEEN DATE '{start}' AND DATE '{end}'
              AND user_id % {int(user_mod)} = 0
        """)

        base_rows = int(con.execute("SELECT count(*) FROM base").fetchone()[0])
        if base_rows == 0:
            raise RuntimeError(
                f"采样后没有数据（user_mod={user_mod}）。请调小 user_mod 后重试。"
            )

        con.execute(f"""
            CREATE TEMP TABLE users AS
            SELECT
                CAST(user_id AS VARCHAR) AS user_id,
                '13' || lpad(CAST((user_id * 7919) % 1000000000 AS VARCHAR), 9, '0') AS phone,
                {CITIES!r}[1 + CAST(user_id % {len(CITIES)} AS INTEGER)] AS city,
                18 + CAST(user_id % 43 AS INTEGER) AS age,
                DATE '2017-01-01' + CAST(user_id % 330 AS INTEGER) AS reg_date
            FROM (SELECT DISTINCT user_id FROM base)
        """)

        con.execute("""
            CREATE TEMP TABLE items AS
            SELECT
                CAST(item_id AS VARCHAR) AS sku_id,
                '商品' || CAST(item_id AS VARCHAR) AS sku_name,
                '类目' || CAST(category_id AS VARCHAR) AS category,
                '品牌' || lpad(CAST(category_id % 100 AS VARCHAR), 2, '0') AS brand,
                round(
                    10 + CAST(item_id % 1990 AS DOUBLE)
                    + CAST(category_id % 100 AS DOUBLE) / 100.0,
                    2
                ) AS price
            -- 同一 item_id 在真实数据里可能对应多个 category_id（上游标注噪声）。
            -- 维度主键是 item_id，必须收敛成一行；取 min(category_id) 保证确定性。
            FROM (
                SELECT item_id, min(category_id) AS category_id
                FROM base GROUP BY item_id
            )
        """)

        con.execute("""
            CREATE TEMP TABLE log_rows AS
            SELECT
                'L' || lpad(
                    CAST(row_number() OVER (
                        ORDER BY ts, user_id, item_id, behavior_type
                    ) AS VARCHAR), 10, '0'
                ) AS log_id,
                CAST(user_id AS VARCHAR) AS user_id,
                CAST(item_id AS VARCHAR) AS sku_id,
                behavior_type AS event,
                dt AS log_date,
                CAST(hash(
                    CAST(user_id AS VARCHAR) || '|'
                    || CAST(item_id AS VARCHAR) || '|'
                    || CAST(ts AS VARCHAR)
                ) % 300 + 1 AS BIGINT) AS stay_seconds
            FROM base
        """)

        # ---- 订单：真实 buy 事件 + 可控脏数据 ---------------------------------
        # 天池原始数据只有行为、没有订单金额，且本身是"干净"的（buy 事件无重复主键、
        # 无退款状态）。为了让质量门禁 / 对账仍然有真实靶子，这里在**真实用户与商品**上
        # 叠加与 synthetic 造数器同口径的 8 类订单脏数据（重复、空用户、空金额、
        # 负金额、孤儿 SKU、退款、未来日期）与 2 类日志脏数据（空用户、迟到）。
        # 这些行的 user_id / item_id / category 都来自真实数据，只有"出问题的那一列"是
        # 人为注入的；注入量写进 _baseline.json，可复算、可关闭。
        con.execute("""
            CREATE TEMP TABLE base_orders AS
            WITH buys AS (
                SELECT
                    user_id, item_id, ts, dt,
                    row_number() OVER (ORDER BY ts, user_id, item_id) AS rn
                FROM base
                WHERE behavior_type = 'buy'
            )
            SELECT
                'B' || lpad(CAST(b.rn AS VARCHAR), 12, '0') AS order_id,
                CAST(b.user_id AS VARCHAR) AS user_id,
                CAST(b.item_id AS VARCHAR) AS sku_id,
                1 AS quantity,
                i.price AS unit_price,
                i.price AS amount,
                b.dt AS order_date,
                CASE WHEN hash('refund|' || CAST(b.rn AS VARCHAR)) % 100 < 3
                     THEN 'refunded' ELSE 'paid' END AS order_status,
                CASE CAST(hash(CAST(b.user_id AS VARCHAR) || '|' || CAST(b.item_id AS VARCHAR)) % 3 AS INTEGER)
                    WHEN 0 THEN 'app' WHEN 1 THEN 'h5' ELSE 'mini'
                END AS channel
            FROM buys b
            JOIN items i ON i.sku_id = CAST(b.item_id AS VARCHAR)
        """)

        base_order_rows = int(con.execute("SELECT count(*) FROM base_orders").fetchone()[0])

        # 注入量按真实 buy 事件数等比缩放，而不是写死固定条数：
        # 否则 --user-mod 调大（样本变小）时，固定 200 行空 user_id 会从 0.1% 变成 2%，
        # 直接把阻断级 not_null_user_id 打红；调小又会小到规则抓不到。
        def _dirty(rate: float) -> int:
            return max(2, min(base_order_rows, round(base_order_rows * rate)))

        n_dup = _dirty(0.003)       # 0.30% 重复订单
        n_null_user = _dirty(0.0011)  # 0.11% 空 user_id
        n_null_amount = _dirty(0.0008)
        n_negative = _dirty(0.0005)
        n_orphan = _dirty(0.002)
        n_future = _dirty(0.0004)

        con.execute(f"""
            CREATE TEMP TABLE order_rows AS
            WITH
            dup AS (
                SELECT * FROM base_orders
                WHERE order_status = 'paid'
                ORDER BY hash(order_id) LIMIT {n_dup}
            ),
            null_user AS (
                SELECT 'N' || lpad(CAST(row_number() OVER () AS VARCHAR), 8, '0') AS order_id,
                       CAST(NULL AS VARCHAR) AS user_id,
                       sku_id, quantity, unit_price, amount, order_date, order_status, channel
                FROM (SELECT * FROM base_orders ORDER BY hash(order_id) LIMIT {n_null_user})
            ),
            null_amount AS (
                SELECT 'A' || lpad(CAST(row_number() OVER () AS VARCHAR), 8, '0') AS order_id,
                       user_id, sku_id, quantity, unit_price,
                       CAST(NULL AS DOUBLE) AS amount,
                       order_date, order_status, channel
                FROM (SELECT * FROM base_orders ORDER BY hash(order_id) LIMIT {n_null_amount})
            ),
            negative_amount AS (
                SELECT 'G' || lpad(CAST(row_number() OVER () AS VARCHAR), 8, '0') AS order_id,
                       user_id, sku_id, quantity, unit_price,
                       -abs(amount) AS amount,
                       order_date, order_status, channel
                FROM (SELECT * FROM base_orders ORDER BY hash(order_id) LIMIT {n_negative})
            ),
            orphan_sku AS (
                SELECT 'K' || lpad(CAST(row_number() OVER () AS VARCHAR), 8, '0') AS order_id,
                       user_id, 'ITEM999999999' AS sku_id, quantity, unit_price, amount,
                       order_date, order_status, channel
                FROM (SELECT * FROM base_orders ORDER BY hash(order_id) LIMIT {n_orphan})
            ),
            future_date AS (
                SELECT 'F' || lpad(CAST(row_number() OVER () AS VARCHAR), 8, '0') AS order_id,
                       user_id, sku_id, quantity, unit_price, amount,
                       DATE '2017-12-03'
                           + CAST(row_number() OVER () % 5 + 1 AS INTEGER) AS order_date,
                       order_status, channel
                FROM (SELECT * FROM base_orders ORDER BY hash(order_id) LIMIT {n_future})
            )
            SELECT * FROM base_orders
            UNION ALL SELECT * FROM dup
            UNION ALL SELECT * FROM null_user
            UNION ALL SELECT * FROM null_amount
            UNION ALL SELECT * FROM negative_amount
            UNION ALL SELECT * FROM orphan_sku
            UNION ALL SELECT * FROM future_date
        """)

        # 日志脏数据：空 user_id（会被 DWD 丢弃）与窗口外迟到（ODS 不加载）。
        con.execute("""
            CREATE TEMP TABLE log_rows_dirty AS
            WITH null_log AS (
                SELECT 'L9' || lpad(CAST(row_number() OVER () AS VARCHAR), 8, '0') AS log_id,
                       CAST(NULL AS VARCHAR) AS user_id, sku_id, event, log_date, stay_seconds
                FROM (SELECT * FROM log_rows ORDER BY hash(log_id) LIMIT 400)
            ),
            late_log AS (
                SELECT 'L8' || lpad(CAST(row_number() OVER () AS VARCHAR), 8, '0') AS log_id,
                       user_id, sku_id, event,
                       DATE '2017-11-24' AS log_date,
                       stay_seconds
                FROM (SELECT * FROM log_rows ORDER BY hash(log_id) LIMIT 600)
            )
            SELECT * FROM log_rows
            UNION ALL SELECT * FROM null_log
            UNION ALL SELECT * FROM late_log
        """)
        con.execute("DROP TABLE log_rows")
        con.execute("ALTER TABLE log_rows_dirty RENAME TO log_rows")

        # 与 DWD 完全同口径的基准：窗口内、去重后、已支付、金额有效、user_id 非空。
        clean_valid_rows, clean_orders, clean_gmv = con.execute(f"""
            WITH dedup AS (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY order_id, sku_id
                        ORDER BY (user_id IS NULL) DESC, (amount IS NULL) DESC, amount DESC
                    ) AS rn
                FROM order_rows
                WHERE order_date BETWEEN DATE '{start}' AND DATE '{end}'
            )
            SELECT
                count(*) FILTER (WHERE order_status = 'paid' AND amount > 0 AND user_id IS NOT NULL),
                count(DISTINCT order_id) FILTER (WHERE order_status = 'paid' AND amount > 0 AND user_id IS NOT NULL),
                round(coalesce(sum(amount) FILTER (WHERE order_status = 'paid' AND amount > 0 AND user_id IS NOT NULL), 0), 2)
            FROM dedup WHERE rn = 1
        """).fetchone()
        user_rows, item_rows, log_rows = con.execute(
            "SELECT (SELECT count(*) FROM users), "
            "(SELECT count(*) FROM items), (SELECT count(*) FROM log_rows)"
        ).fetchone()
        hot_sku, hot_rows = con.execute(
            "SELECT sku_id, count(*) AS n FROM base_orders "
            "WHERE order_status = 'paid' GROUP BY 1 ORDER BY n DESC, sku_id LIMIT 1"
        ).fetchone()
        raw_rows, raw_gmv = con.execute(
            "SELECT count(*), round(coalesce(sum(amount), 0), 2) FROM order_rows"
        ).fetchone()
        refund_rows, refund_gmv = con.execute(
            "SELECT count(*), round(coalesce(sum(amount), 0), 2) "
            "FROM base_orders WHERE order_status = 'refunded'"
        ).fetchone()

        # 全量 CSV 与按 dt 分区文件。分区先清本前缀的旧文件，避免换采样规则后残留。
        pdir = out / "by_dt"
        pdir.mkdir(parents=True, exist_ok=True)
        partitions = {}
        for prefix, table, date_col in (
            ("order_info", "order_rows", "order_date"),
            ("user_log", "log_rows", "log_date"),
        ):
            for old in pdir.glob(f"{prefix}_*.csv"):
                old.unlink()
            _copy(con, f"SELECT * FROM {table}", out / f"{prefix}.csv")
            # 按**实际出现的日期**写分区：订单侧会因此多出 future_date 的 5 个分区，
            # 日志侧会多出 late_arrival 的 1 个；ODS 只按窗口加载，这正是区分
            # "源文件边界校验"和"仓内校验"的现场证据。
            part_dates = [
                row[0]
                for row in con.execute(
                    f"SELECT DISTINCT {date_col} FROM {table} ORDER BY 1"
                ).fetchall()
            ]
            for dt in part_dates:
                _copy(
                    con,
                    f"SELECT * FROM {table} WHERE {date_col} = DATE '{dt.isoformat()}'",
                    pdir / f"{prefix}_{dt.isoformat()}.csv",
                )
            partitions[f"{prefix}_dt_files"] = len(part_dates)
        _copy(con, "SELECT * FROM items", out / config.SRC_SKU)
        _copy(con, "SELECT * FROM users", out / config.SRC_USER_INFO)
    finally:
        con.close()

    stats = {
        "source": "taobao-userbehavior",
        "dataset": str(path),
        "user_mod": int(user_mod),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "settle_date": end,
        "window_days": TAOBAO_DAYS,
        "rows": {
            "sku": int(item_rows),
            "user": int(user_rows),
            "order": int(raw_rows),
            "user_log": int(log_rows),
            "base_events": int(base_rows),
            "buy_events": int(base_order_rows),
        },
        "partitions": {
            "order_dt_files": int(partitions["order_info_dt_files"]),
            "log_dt_files": int(partitions["user_log_dt_files"]),
        },
        "injected": {
            "dup_order_id": n_dup,
            "null_user_id": n_null_user,
            "null_amount": n_null_amount,
            "negative_amount": n_negative,
            "orphan_sku": n_orphan,
            "future_date": n_future,
            "null_user_log": 400,
            "late_arrival": 600,
            "refund_rate": 0.03,
        },
        "baseline": {
            "clean_rows": int(clean_valid_rows),
            "clean_orders": int(clean_orders),
            "clean_gmv": float(clean_gmv),
            "raw_orders": int(raw_rows),
            "raw_gmv": float(raw_gmv),
            "refund_rows": int(refund_rows),
            "refund_gmv": float(refund_gmv),
        },
        "skew": {
            "hot_sku_id": str(hot_sku),
            "hot_sku_order_rows": int(hot_rows),
        },
        "sampling": {
            "rule": f"user_id % {user_mod} == 0",
            "note": "保留采样用户的全部窗口内行为事件，不做行级随机丢弃",
        },
    }
    (out / "_baseline.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if verbose:
        print(
            f"[taobao] {path.name} -> {user_rows:,} 用户 / {item_rows:,} 商品 / "
            f"{log_rows:,} 行为 / {base_order_rows:,} 笔 buy 事件 "
            f"(user_mod={user_mod})"
        )
    return stats
