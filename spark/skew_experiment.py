r"""倾斜对照实验：把"加盐两阶段聚合"这件事做成可复现的对照。

分两部分，**上半部分不需要装 Spark 就能跑**：

  A. `duckdb_baseline()` —— 用 DuckDB 复现"直接聚合"与"加盐两阶段聚合"两条路径，
     验证**两者结果完全一致**。这是加盐正确性的最低要求：
     优化不能改变结果，只能改变执行代价。
  B. `spark_experiment()` —— 真 Spark 版本（需要 `pip install pyspark`）。
     同样两条路径，打印各 stage 耗时与分区间数据量分布，
     用来观察"某个 task 明显比同 stage 其他 task 慢"这个现象。

为什么要做这个对照：
  倾斜的本质是**少数 key 承载了绝大多数数据**。本项目造数时让一个 SKU 占 40% 的订单，
  于是**按 sku_id 聚合**时，这个 key 所在的分区会比其他分区大得多——那个 task 就是长尾。
  加盐打散的作用是把这个大 key 拆成 N 个小 key 分别聚合，再把结果合并回去。

  ⚠️ 分组键必须是 sku_id，不能是 user_id：本项目 2 万用户的订单量很均匀
  （`dw.cli recon` 实测 user_id 分组 max/中位数只有 2.7x，等于没有倾斜），
  拿它做加盐实验只能证明"两条路径结果一致"，证明不了"打散了长尾"。

⚠️ 数据量与结论的关系（面试会被问）：
  本项目的量级（18 万订单 / 120 万日志）**远小于** Spark 的适用区间，
  所以真 Spark 跑起来大概率是**比 DuckDB 慢**的——固定开销（启动会话、JVM、shuffle）占了大头。
  这不是"Spark 不好"，而是"这个量级不需要它"。实测这个结论本身就是选型判断的依据。

用法：
    python spark/skew_experiment.py            # 跑 A（无需 Spark）
    python spark/skew_experiment.py --spark    # 额外跑 B（需要 pyspark）

⚠️ Windows + 中文路径的坑（本项目实际踩到过，报错极具误导性）：
  Spark 的 `bin/spark-submit.cmd` 是个批处理脚本，**路径里有中文时 cmd.exe 会把 classpath 传坏**，
  于是 java 报 `Could not find or load main class org.apache.spark.deploy.SparkSubmit`
  —— 看着像"Spark 没装好"，其实装得完全没问题（同一份 jar 用 `java -cp` 直接跑是好的）。
  两个办法：① 把 pyspark 目录挂到纯 ASCII 路径上再设 `SPARK_HOME`：
      mklink /J C:\spark_ascii <venv>\Lib\site-packages\pyspark
      set SPARK_HOME=C:\spark_ascii
    ② 直接把项目放到纯英文路径下。
  另：pyspark 的 sdist 没有带上全部依赖，`pip install pyspark --no-deps` 之后会因缺 py4j 而在
  import 阶段失败 —— 用普通 `pip install pyspark` 即可。
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path

# 允许直接 `python spark/skew_experiment.py`（把 src 加进 import 路径）
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dw import config, db, paths

# 加盐的分桶数。取 8 是一个折中：
#   太小（比如 2）打散不充分，长尾只是被切成两条长的；
#   太大（比如 1000）会产生大量小任务，调度开销盖过收益。
SALT_BUCKETS = 8

# Spark 侧加盐用带种子的 rand()：既保证可复现，又是**分区内计算**。
# ⚠️ 第一版这里用的是 `row_number() OVER (ORDER BY sku_id)`，Spark 会直接告警
#    "No Partition Defined for Window operation! Moving all data to a single partition"——
#    为了给每行编号，它把全量数据先收进一个分区，等于在"治倾斜"的实验里自己造了个单点瓶颈。
#    DuckDB 侧没有这个问题（行号是流式的），所以两边实现不同，这是有意为之。
SALT_SEED = 20261010


def _window() -> tuple:
    baseline = paths.load_baseline()
    return paths.default_window(baseline)


# --------------------------------------------------------------------------
# A. DuckDB 对照（无需 Spark）
# --------------------------------------------------------------------------
DIRECT_SQL = """
-- 直接聚合：按 sku_id 分组。倾斜的 key 会让某个分组特别大。
SELECT sku_id, count(*) AS cnt, round(sum(amount), 2) AS amt
FROM dwd_order_detail
WHERE dt BETWEEN DATE '{start}' AND DATE '{end}' AND is_valid AND is_paid
GROUP BY sku_id
ORDER BY sku_id
"""

SALTED_SQL = """
-- 加盐两阶段聚合：
--   第一阶段：给每一行分配一个桶号（salt），按 (sku_id, salt) 分组做局部聚合
--              ——这一步把大 key 拆成了 {buckets} 个小 key，长尾被打散
--   第二阶段：去掉 salt，把局部结果再合起来
--              ——这一步的数据量已经大幅缩小，不会再倾斜
WITH stage1 AS (
    SELECT
        sku_id,
        count(*)              AS cnt,
        sum(amount)           AS amt
    FROM (
        SELECT
            sku_id,
            amount,
            -- 加盐：用行号取模模拟"随机打散"，保证确定性（同一份数据结果可复现）
            (row_number() OVER () % {buckets}) AS salt
        FROM dwd_order_detail
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}' AND is_valid AND is_paid
    )
    GROUP BY sku_id, salt
)
SELECT
    sku_id,
    sum(cnt)          AS cnt,
    round(sum(amt), 2) AS amt
FROM stage1
GROUP BY sku_id
ORDER BY sku_id
"""


def _same_rows(a: list, b: list, tol: float = 1e-6) -> bool:
    """逐行比较聚合结果：键必须完全一致，度量按**相对误差**比较。

    为什么不能直接 `a == b`：这里是 DOUBLE 的 sum，加盐把行打散后
    "相加顺序"变了，浮点末位可以差 1ulp（`0.1+0.2 != 0.3` 那个经典问题的放大版）。
    全等断言会偶发 SystemExit，把一次正确的优化判成"改了语义"。
    """
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b, strict=True):
        if ra[0] != rb[0]:
            return False
        for x, y in zip(ra[1:], rb[1:], strict=True):
            if x is None or y is None:
                if x != y:
                    return False
            elif abs(float(x) - float(y)) > tol * max(1.0, abs(float(x))):
                return False
    return True


def duckdb_baseline(warehouse: str | None = None) -> None:
    """验证：加盐两阶段聚合与直接聚合的结果必须完全一致。"""
    start, end = _window()
    print("=" * 72)
    print("A. DuckDB 对照：直接聚合 vs 加盐两阶段聚合")
    print("=" * 72)

    with db.warehouse(warehouse) as con:
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

    same = _same_rows(direct, salted)
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
    from pyspark.sql import SparkSession

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
    # 落 data/ 下（该目录本就整体不入库），避免在 dashboard/out/ 里留下未跟踪的中间产物。
    export_dir = config.DATA_DIR / "skew_export"
    export_dir.mkdir(parents=True, exist_ok=True)
    csv_path = export_dir / "orders.csv"

    with db.warehouse(warehouse) as con:
        con.execute(
            f"""
            COPY (
                SELECT sku_id, amount
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
        SELECT sku_id, count(*) AS cnt, round(sum(amount), 2) AS amt
        FROM orders GROUP BY sku_id ORDER BY sku_id
        """
    )
    direct_rows = direct.collect()

    # ---- 分区规模分布：长尾的直接证据 ----
    # 按 shuffle key 的哈希把行分到 8 个分区，看每个分区拿到多少行。
    # 直接按 sku_id 聚合时，热销 SKU 所在的那个分区会被灌进全量约 40% 的行。
    def _partition_sizes(shuffle_key: str) -> list:
        return spark.sql(
            f"""
            SELECT pmod(hash({shuffle_key}), {SALT_BUCKETS}) AS p, count(*) AS n
            FROM shuffled GROUP BY p ORDER BY n DESC
            """
        ).collect()

    # 直接聚合的 shuffle key 就是 sku_id
    spark.sql("SELECT sku_id FROM orders").createOrReplaceTempView("shuffled")
    direct_sizes = _partition_sizes("sku_id")
    print("  直接聚合（shuffle key = sku_id）的分区规模分布:")
    for r in direct_sizes:
        print(f"    partition {r['p']}: {r['n']:>10,} 行")
    if direct_sizes:
        mx, mn = direct_sizes[0]["n"], direct_sizes[-1]["n"]
        print(f"    最大 / 最小 = {mx / max(mn, 1):.2f}x")

    # 加盐后 stage1 的 shuffle key 是 (sku_id, salt)，大 key 被拆成 8 份
    spark.sql(
        f"""
        SELECT sku_id, cast(rand({SALT_SEED}) * {SALT_BUCKETS} AS INT) AS salt
        FROM orders
        """
    ).createOrReplaceTempView("shuffled")
    salted_sizes = _partition_sizes("concat(sku_id, '#', cast(salt AS string))")
    print("  加盐后 stage1（shuffle key = sku_id + salt）的分区规模分布:")
    for r in salted_sizes:
        print(f"    partition {r['p']}: {r['n']:>10,} 行")
    if salted_sizes:
        mx, mn = salted_sizes[0]["n"], salted_sizes[-1]["n"]
        print(f"    最大 / 最小 = {mx / max(mn, 1):.2f}x")

    # ---- 路径 2：加盐两阶段聚合 ----
    salted = spark.sql(
        f"""
        WITH stage1 AS (
            SELECT sku_id, count(*) AS cnt, sum(amount) AS amt
            FROM (
                SELECT sku_id, amount,
                       cast(rand({SALT_SEED}) * {SALT_BUCKETS} AS INT) AS salt
                FROM orders
            )
            GROUP BY sku_id, salt
        )
        SELECT sku_id, sum(cnt) AS cnt, round(sum(amt), 2) AS amt
        FROM stage1 GROUP BY sku_id ORDER BY sku_id
        """
    )
    salted_rows = salted.collect()

    same = _same_rows(
        [(r["sku_id"], r["cnt"], r["amt"]) for r in direct_rows],
        [(r["sku_id"], r["cnt"], r["amt"]) for r in salted_rows],
    )
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

    duckdb_baseline(args.warehouse)

    if args.spark:
        start, end = _window()
        # 只在"确实没装 pyspark"时给安装提示。原先这里直接 `except ImportError`，
        # 结果"装了 pyspark 但缺 py4j"也会被报成"未安装 pyspark"——提示指错了方向，
        # 得先怀疑自己没装，而不是去看真正的 ImportError。装了之后就不要再吞异常。
        if importlib.util.find_spec("pyspark") is None:
            print()
            print("跳过 Spark 部分：未安装 pyspark。安装后再跑：")
            print("    pip install pyspark")
            print("提示：Spark 需要 Java 运行时；本地起会话约需 1~2 分钟，sdist 约 430MB。")
        else:
            spark_experiment(args.warehouse, start.isoformat(), end.isoformat())


if __name__ == "__main__":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    main()
