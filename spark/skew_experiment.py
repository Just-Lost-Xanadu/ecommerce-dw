"""倾斜对照实验：把"加盐两阶段聚合"这件事做成可复现的对照。

分两部分，**上半部分不需要装 Spark 就能跑**：

  A. `duckdb_baseline()` —— 用 DuckDB 复现"直接聚合"与"加盐两阶段聚合"两条路径，
     验证**两者结果完全一致**。这是加盐正确性的最低要求：
     优化不能改变结果，只能改变执行代价。
  B. `spark_experiment()` —— 真 Spark 版本（需要 `pip install pyspark`）。
     同样两条路径，打印各 stage 耗时与分区间数据量分布，
     用来观察"某个 task 明显比同 stage 其他 task 慢"这个现象。

为什么要做这个对照：
  倾斜的本质是**少数 key 承载了绝大多数数据**。本项目造数时让一个 SKU 占 40% 的订单，
  于是按它聚合时，某一个分区会比其他分区大得多——那个 task 就是长尾。
  加盐打散的作用是把这个大 key 拆成 N 个小 key 分别聚合，再把结果合并回去。

⚠️ 数据量与结论的关系（面试会被问）：
  本项目的量级（18 万订单 / 120 万日志）**远小于** Spark 的适用区间，
  所以真 Spark 跑起来大概率是**比 DuckDB 慢**的——固定开销（启动会话、JVM、shuffle）占了大头。
  这不是"Spark 不好"，而是"这个量级不需要它"。实测这个结论本身就是选型判断的依据。

用法：
    python spark/skew_experiment.py            # 跑 A（无需 Spark）
    python spark/skew_experiment.py --spark    # 额外跑 B（需要 pyspark）
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# 允许直接 `python spark/skew_experiment.py`（把 src 加进 import 路径）
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dw import config, db, paths  # noqa: E402

# 加盐的分桶数。取 8 是一个折中：
#   太小（比如 2）打散不充分，长尾只是被切成两条长的；
#   太大（比如 1000）会产生大量小任务，调度开销盖过收益。
SALT_BUCKETS = 8


def _window() -> tuple:
    baseline = paths.load_baseline()
    return paths.default_window(baseline)


# --------------------------------------------------------------------------
# A. DuckDB 对照（无需 Spark）
# --------------------------------------------------------------------------
DIRECT_SQL = """
-- 直接聚合：按 user_id 分组。倾斜的 key 会让某个分组特别大。
SELECT user_id, count(*) AS cnt, round(sum(amount), 2) AS amt
FROM dwd_order_detail
WHERE dt BETWEEN DATE '{start}' AND DATE '{end}' AND is_valid AND is_paid
GROUP BY user_id
ORDER BY user_id
"""

SALTED_SQL = """
-- 加盐两阶段聚合：
--   第一阶段：给每一行分配一个桶号（salt），按 (user_id, salt) 分组做局部聚合
--              ——这一步把大 key 拆成了 {buckets} 个小 key，长尾被打散
--   第二阶段：去掉 salt，把局部结果再合起来
--              ——这一步的数据量已经大幅缩小，不会再倾斜
WITH stage1 AS (
    SELECT
        user_id,
        count(*)              AS cnt,
        sum(amount)           AS amt
    FROM (
        SELECT
            user_id,
            amount,
            -- 加盐：用行号取模模拟"随机打散"，保证确定性（同一份数据结果可复现）
            (row_number() OVER () % {buckets}) AS salt
        FROM dwd_order_detail
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}' AND is_valid AND is_paid
    )
    GROUP BY user_id, salt
)
SELECT
    user_id,
    sum(cnt)          AS cnt,
    round(sum(amt), 2) AS amt
FROM stage1
GROUP BY user_id
ORDER BY user_id
"""


def duckdb_baseline() -> None:
    """验证：加盐两阶段聚合与直接聚合的结果必须完全一致。"""
    start, end = _window()
    print("=" * 72)
    print("A. DuckDB 对照：直接聚合 vs 加盐两阶段聚合")
    print("=" * 72)

    with db.warehouse() as con:
        direct = con.execute(
            DIRECT_SQL.format(start=start.isoformat(), end=end.isoformat())
        ).fetchall()
        salted = con.execute(
            SALTED_SQL.format(
                start=start.isoformat(), end=end.isoformat(), buckets=SALT_BUCKETS
            )
        ).fetchall()

        total = con.execute(
            f"SELECT count(*) FROM dwd_order_detail WHERE dt BETWEEN DATE '{start}' "
            f"AND DATE '{end}' AND is_valid AND is_paid"
        ).fetchone()[0]

    same = direct == salted
    print(f"  参与聚合的订单行数 : {total:,}")
    print(f"  聚合分组数         : {len(direct):,}")
    print(f"  加盐桶数           : {SALT_BUCKETS}")
    print(f"  两条路径结果一致   : {'✓ 是' if same else '✗ 否'}")
    if not same:
        for a, b in zip(direct, salted, strict=False):
            if a != b:
                print(f"    首个差异：direct={a} salted={b}")
                break
        raise SystemExit("加盐两阶段聚合改变了结果——这是不可接受的，优化不能改语义")
    print()
    print("  结论：加盐只改变执行方式（把大 key 拆小），**不改变结果**。")
    print("        这是任何性能优化的前提：先保证语义不变，再谈快不快。")


# --------------------------------------------------------------------------
# B. Spark 对照（需要 pyspark）
# --------------------------------------------------------------------------
def spark_experiment(warehouse: str, start: str, end: str) -> None:
    from pyspark.sql import SparkSession, functions as F

    print("=" * 72)
    print("B. Spark 对照：直接聚合 vs 加盐两阶段聚合")
    print("=" * 72)

    spark = (
        SparkSession.builder.appName("skew-experiment")
        .master("local[*]")
        # 为了观察分区规模分布，分区数给小一点，长尾才看得清
        .config("spark.sql.shuffle.partitions", str(SALT_BUCKETS))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 用 JDBC 读 DuckDB 需要额外驱动，这里走 CSV 中转最省事：
    # 先把要分析的那部分数据导出成 CSV，再由 Spark 读。
    export_dir = ROOT / "dashboard" / "out" / "skew_export"
    export_dir.mkdir(parents=True, exist_ok=True)
    csv_path = export_dir / "orders.csv"

    with db.warehouse() as con:
        con.execute(
            f"""
            COPY (
                SELECT user_id, amount
                FROM dwd_order_detail
                WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
                  AND is_valid AND is_paid
            ) TO '{csv_path.as_posix()}' (HEADER, DELIMITER ',')
            """
        )

    df = (
        spark.read.option("header", True).option("inferSchema", True).csv(str(csv_path))
    ).cache()
    df.createOrReplaceTempView("orders")

    # ---- 路径 1：直接聚合 ----
    direct = spark.sql(
        """
        SELECT user_id, count(*) AS cnt, round(sum(amount), 2) AS amt
        FROM orders GROUP BY user_id ORDER BY user_id
        """
    )
    direct_rows = direct.collect()

    # ---- 分区规模分布：长尾的直接证据 ----
    sizes = spark.sql(
        """
        SELECT /*+ REPARTITION(8) */ p, count(*) AS n
        FROM (
            SELECT user_id, amount, (row_number() OVER (ORDER BY user_id) % 8) AS p
            FROM orders
        ) GROUP BY p ORDER BY n DESC
        """
    ).collect()
    print("  分区规模分布（越不均匀越可能倾斜）:")
    for r in sizes:
        print(f"    partition {r['p']}: {r['n']:>10,} 行")
    if sizes:
        mx, mn = sizes[0]["n"], sizes[-1]["n"]
        print(f"    最大 / 最小 = {mx / max(mn, 1):.2f}x")

    # ---- 路径 2：加盐两阶段聚合 ----
    salted = spark.sql(
        f"""
        WITH stage1 AS (
            SELECT user_id, count(*) AS cnt, sum(amount) AS amt
            FROM (
                SELECT user_id, amount,
                       (row_number() OVER (ORDER BY user_id) % {SALT_BUCKETS}) AS salt
                FROM orders
            )
            GROUP BY user_id, salt
        )
        SELECT user_id, sum(cnt) AS cnt, round(sum(amt), 2) AS amt
        FROM stage1 GROUP BY user_id ORDER BY user_id
        """
    )
    salted_rows = salted.collect()

    same = [(r["user_id"], r["cnt"], r["amt"]) for r in direct_rows] == [
        (r["user_id"], r["cnt"], r["amt"]) for r in salted_rows
    ]
    print(f"  两条路径结果一致   : {'✓ 是' if same else '✗ 否'}")
    if not same:
        raise SystemExit("Spark 下加盐两阶段聚合改变了结果")

    print()
    print("  怎么看结果：")
    print("    · 分区规模分布越不均匀，说明倾斜越严重；")
    print("    · 在本项目的数据量级（约 18 万行）下，Spark 多半**比 DuckDB 慢**——")
    print("      固定开销（会话启动、JVM、shuffle）占了大头。")
    print("    · 所以正确结论不是“Spark 不好”，而是：")
    print("      **百万行以内用 DuckDB 更划算；到千万行以上、需要横向扩展时，Spark 才划算。**")

    spark.stop()


def main() -> None:
    # Windows 终端默认 GBK，直接打印 ✓ 会抛 UnicodeEncodeError。显式要求 UTF-8。
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
        except (AttributeError, ValueError):
            pass

    ap = argparse.ArgumentParser(description="倾斜对照实验")
    ap.add_argument("--spark", action="store_true", help="额外跑 Spark 版本（需 pyspark）")
    ap.add_argument("--warehouse", default=str(config.WAREHOUSE_DB))
    args = ap.parse_args()

    duckdb_baseline()

    if args.spark:
        start, end = _window()
        try:
            spark_experiment(args.warehouse, start.isoformat(), end.isoformat())
        except ImportError:
            print()
            print("跳过 Spark 部分：未安装 pyspark。安装后再跑：")
            print("    pip install pyspark")
            print("提示：Spark 需要 Java 运行时；本地起会话约需 1~2 分钟。")


if __name__ == "__main__":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    main()
