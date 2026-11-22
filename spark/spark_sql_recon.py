r"""最小版 Spark SQL 主链路：ODS → DWD → DWS → ADS（交易指标）+ 与 DuckDB 对账。

目的：让"熟悉 Spark SQL"这句话有可运行的证据（而不是只做过倾斜 DataFrame 实验）。

范围（最小可验证）：
  · 输入：`data/raw/order_info.csv`（源 CSV，等价于订单侧 ODS）；
  · DWD：去重 + 有效性判定，逻辑与 `sql/dwd_order_detail.sql` 对齐；
  · DWS：用户 × 天的交易汇总，`round(sum(amount), 2)`；
  · ADS：每日 `gmv / order_cnt / pay_users`；
  · 对账：逐日与 DuckDB 的 `ads_daily_metrics` 比较，外加 DWD 行数一致性。

没做的（不在这份脚本里）：
  · 页面日志 / 流量指标 / 新用户 / 漏斗；
  · 分布式集群、Hive 元仓、YARN 调度；
  · 性能对比——这个量级下 Spark 固定开销占大头，跑通 + 对账一致才是本脚本要证明的事。

用法：
    python spark/spark_sql_recon.py

⚠️ Windows + 中文路径：`bin/spark-submit.cmd` 在中文路径下会把 classpath 传坏。
   本脚本用 `SparkSession.builder` 从 Python 直接拉起 JVM（不经过 spark-submit.cmd），
   和 `skew_experiment.py` 的 `--spark` 走同一条路径；若仍报
   `Could not find or load main class SparkSubmit`，按 `skew_experiment.py` 头部的
   junction + `SPARK_HOME` 办法处理。
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dw import config, paths


def _window() -> tuple[date, date]:
    return paths.default_window(paths.load_baseline())


def _prepare_spark_env() -> None:
    """Windows 中文路径下让 pyspark 能启动。

    `bin/spark-submit.cmd` 在中文路径下会把 classpath 传坏，报
    `ClassNotFoundException: SparkSubmit`。约定用一个纯 ASCII 的 junction
    （如 `C:\\spark_ascii`）指向 venv 里的 pyspark 包，存在就自动复用：
        mklink /J C:\\spark_ascii <venv>\\Lib\\site-packages\\pyspark
    """
    if not os.environ.get("PYSPARK_PYTHON"):
        os.environ["PYSPARK_PYTHON"] = sys.executable
    if os.environ.get("SPARK_HOME"):
        return
    try:
        import pyspark
        pdir = Path(pyspark.__file__).resolve().parent
    except Exception:
        return
    if not any(ord(ch) > 127 for ch in str(pdir)):
        return
    candidate = Path(r"C:\spark_ascii")
    if (candidate / "bin" / "spark-submit.cmd").is_file():
        os.environ["SPARK_HOME"] = str(candidate)


def _build_views(spark, start: str, end: str) -> None:
    """在 Spark 里用 SQL 重建 ODS→DWD→DWS 的临时视图。"""
    raw_path = paths.raw_csv(config.SRC_ORDER).as_posix()

    raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(raw_path)
    )
    raw.createOrReplaceTempView("raw")

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW ods AS
        SELECT
            order_id,
            user_id,
            sku_id,
            CAST(quantity AS INT)        AS quantity,
            CAST(unit_price AS DOUBLE)   AS unit_price,
            CAST(amount AS DOUBLE)       AS amount,
            to_date(nullif(trim(order_date), '')) AS order_date,
            order_status,
            channel,
            to_date(nullif(trim(order_date), '')) AS dt
        FROM raw
        WHERE to_date(nullif(trim(order_date), '')) BETWEEN DATE '{start}' AND DATE '{end}'
        """
    )

    # DWD：按 (dt, order_id, sku_id) 去重，脏行优先保留；然后判有效性。
    # 排序键与 sql/dwd_order_detail.sql 对齐（_load_time 在同一批里是常量，略去不影响）。
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW dwd AS
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY dt, order_id, sku_id
                    ORDER BY
                        (user_id IS NULL) DESC,
                        (amount IS NULL) DESC,
                        amount DESC,
                        order_status,
                        quantity,
                        order_date
                ) AS rn
            FROM ods
        ),
        deduped AS (
            SELECT * FROM ranked WHERE rn = 1
        ),
        judged AS (
            SELECT
                *,
                (order_status = 'paid') AS is_paid,
                CASE
                    WHEN order_id IS NULL   THEN 'order_id_null'
                    WHEN user_id IS NULL    THEN 'user_id_null'
                    WHEN order_date IS NULL THEN 'order_date_null'
                    WHEN amount IS NULL     THEN 'amount_null'
                    WHEN amount <= 0        THEN 'amount_not_positive'
                    WHEN order_date > DATE '{end}' THEN 'date_in_future'
                    ELSE NULL
                END AS reject_reason
            FROM deduped
        )
        SELECT *, (reject_reason IS NULL) AS is_valid
        FROM judged
        """
    )

    # DWS：用户 × 天，金额在汇总层定精度（与 sql/dws_trade_user_1d.sql 一致）。
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW dws AS
        SELECT
            user_id,
            dt,
            count(DISTINCT order_id) AS order_cnt,
            round(sum(amount), 2)    AS pay_amount
        FROM dwd
        WHERE is_valid AND is_paid
        GROUP BY user_id, dt
        """
    )

    # ADS：交易三指标。
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW ads AS
        SELECT
            dt,
            sum(order_cnt)              AS order_cnt,
            round(sum(pay_amount), 2)   AS gmv,
            count(DISTINCT user_id)     AS pay_users
        FROM dws
        GROUP BY dt
        ORDER BY dt
        """
    )


def _duckdb_reference(start: str, end: str) -> dict:
    import duckdb

    con = duckdb.connect(str(config.WAREHOUSE_DB), read_only=True)
    try:
        rows = con.execute(
            f"""
            SELECT dt, gmv, order_cnt, pay_users
            FROM ads_daily_metrics
            WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
            ORDER BY dt
            """
        ).fetchall()
        dwd = con.execute(
            f"""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE is_valid) AS valid
            FROM dwd_order_detail
            WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
            """
        ).fetchone()
    finally:
        con.close()
    return {
        "ads": {r[0]: (float(r[1]), int(r[2]), int(r[3])) for r in rows},
        "dwd_total": int(dwd[0]),
        "dwd_valid": int(dwd[1]),
    }


def reconcile(warehouse_note: str = "") -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
        except (AttributeError, ValueError):
            pass

    if importlib.util.find_spec("pyspark") is None:
        print("未安装 pyspark。先 `pip install pyspark`（需要 Java 运行时）。")
        return 2

    start, end = _window()
    start_s, end_s = start.isoformat(), end.isoformat()
    print("=" * 78)
    print(f"Spark SQL 主链路对账：{start_s} ~ {end_s}")
    print("=" * 78)

    _prepare_spark_env()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("spark-sql-recon")
        .master("local[*]")
        # 数据很小，8 个 shuffle 分区足够，避免无数空 task 拉长启动噪声。
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        _build_views(spark, start_s, end_s)

        dwd_counts = spark.sql(
            """
            SELECT count(*) AS total,
                   sum(CASE WHEN is_valid THEN 1 ELSE 0 END) AS valid
            FROM dwd
            """
        ).first()
        spark_rows = {
            r["dt"]: (float(r["gmv"]), int(r["order_cnt"]), int(r["pay_users"]))
            for r in spark.sql("SELECT * FROM ads").collect()
        }
    finally:
        spark.stop()

    ref = _duckdb_reference(start_s, end_s)

    print("DWD 行数：")
    print(f"  Spark  : total={int(dwd_counts['total']):,}  valid={int(dwd_counts['valid']):,}")
    print(f"  DuckDB : total={ref['dwd_total']:,}  valid={ref['dwd_valid']:,}")
    dwd_ok = (
        int(dwd_counts["total"]) == ref["dwd_total"]
        and int(dwd_counts["valid"]) == ref["dwd_valid"]
    )
    print(f"  一致   : {'✓ 是' if dwd_ok else '✗ 否'}")
    print()

    print(f"{'dt':<12} {'Spark GMV':>15} {'DuckDB GMV':>15} {'Δ':>6}  "
          f"{'ord':>7} {'pay':>7}  一致")
    ads_ok = True
    for dt in sorted(set(spark_rows) | set(ref["ads"])):
        s = spark_rows.get(dt)
        d = ref["ads"].get(dt)
        if s is None or d is None:
            print(f"{dt}  缺少一侧数据：spark={s}, duckdb={d}")
            ads_ok = False
            continue
        gmv_diff = abs(s[0] - d[0])
        row_ok = gmv_diff <= 0.01 and s[1] == d[1] and s[2] == d[2]
        ads_ok = ads_ok and row_ok
        print(f"{dt}  {s[0]:>15,.2f} {d[0]:>15,.2f} {gmv_diff:>6.2f}  "
              f"{s[1]:>7,} {s[2]:>7,}  {'✓' if row_ok else '✗'}")
    print()

    ok = dwd_ok and ads_ok
    print("结论：", "✓ Spark SQL 与 DuckDB 逐日一致" if ok else "✗ 存在差异，需排查")
    if warehouse_note:
        print(warehouse_note)
    return 0 if ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(description="Spark SQL 最小主链路对账")
    ap.parse_args()
    raise SystemExit(reconcile())


if __name__ == "__main__":
    main()
