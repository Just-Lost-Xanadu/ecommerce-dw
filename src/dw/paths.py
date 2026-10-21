"""路径与 SQL 模板的集中入口。

把"文件在哪、SQL 在哪"收在一处，业务代码里不出现 `../..` 这类相对路径——
换目录、打包、跑测试时都不会因为工作目录不同而找不到文件。
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from dw import config

SQL_DIR = config.ROOT / "sql"
BY_DT_DIRNAME = "by_dt"


# ---- 源数据 --------------------------------------------------------------
def raw_csv(name: str) -> Path:
    """全量源 CSV 的路径。"""
    return config.RAW_DIR / name


def raw_dir() -> Path:
    """按日期切分的源分区文件目录（data/raw/by_dt）。"""
    return config.RAW_DIR / BY_DT_DIRNAME


def dt_csv(prefix: str, dt: date | str) -> Path:
    """某一天的源 CSV 分区文件。

    例：dt_csv("order_info", "2026-10-01") -> data/raw/by_dt/order_info_2026-10-01.csv
    """
    key = dt.isoformat() if isinstance(dt, date) else str(dt)
    return config.RAW_DIR / BY_DT_DIRNAME / f"{prefix}_{key}.csv"


def raw_exists() -> bool:
    """源数据是否已生成。

    不能只看全量 CSV：ODS 真正读的是 `data/raw/by_dt/` 下的分区文件。只探测全量文件时，
    by_dt 被删空仍会判"数据已生成"，`run` 把每天当空分区、最后"成功"产出 0 行指标。
    """
    if not raw_csv(config.SRC_ORDER).is_file():
        return False
    by_dt = raw_dir()
    return bool(list(by_dt.glob("order_info_*.csv"))) and bool(
        list(by_dt.glob("user_log_*.csv"))
    )


def load_baseline() -> dict:
    """读造数时产出的对账基准（干净口径的 GMV 与订单数）。"""
    path = config.RAW_DIR / "_baseline.json"
    if not path.is_file():
        raise FileNotFoundError(
            f"找不到对账基准 {path}，请先运行：python -m dw.cli gen"
        )
    return json.loads(path.read_text(encoding="utf-8"))


# ---- 统计窗口 ------------------------------------------------------------
def default_window(baseline: dict | None = None) -> tuple[date, date]:
    """默认统计窗口：以数据里的最大业务日期为结算日，往前取 N_DAYS 天。

    为什么要"结算日"这个概念：T+1 批处理每天处理的是**截止到某一天**的数据。
    窗口的上界必须钉死，否则"未来日期"这类脏数据会被算进指标，
    而"迟到数据"也无从判断——两者都需要一个参照点。
    """
    base = baseline or load_baseline()
    settle = date.fromisoformat(base.get("settle_date") or "2026-10-05")
    return settle - timedelta(days=config.N_DAYS - 1), settle


def dates_between(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


# ---- SQL 模板 ------------------------------------------------------------
def sql(name: str) -> str:
    """读一个 SQL 模板。模板里用 `{var}` 占位，由调用方 render() 注入。"""
    path = SQL_DIR / f"{name}.sql"
    if not path.is_file():
        raise FileNotFoundError(f"SQL 模板不存在：{path}")
    return path.read_text(encoding="utf-8")


def render(name: str, **params: object) -> str:
    """读模板并注入参数。

    DuckDB 不支持把表名/日期字面量当绑定参数传，所以这里用显式占位符做字符串渲染。
    **所有注入值都来自本项目的内部代码（日期对象、配置常量、绝对路径），
    不来自外部输入**，因此不存在注入面。
    """
    body = sql(name)
    rendered: dict[str, str] = {}
    for key, value in params.items():
        if isinstance(value, date):
            rendered[key] = f"'{value.isoformat()}'"
        elif isinstance(value, str):
            escaped = value.replace("'", "''")
            rendered[key] = f"'{escaped}'"
        else:
            rendered[key] = str(value)
    return body.format(**rendered)


def _strip_leading_comments(chunk: str) -> str:
    """剥掉一段 SQL 开头的注释行（模板里 CREATE 前面通常有大段说明）。"""
    lines = chunk.splitlines()
    i = 0
    while i < len(lines) and (not lines[i].strip() or lines[i].lstrip().startswith("--")):
        i += 1
    return "\n".join(lines[i:]).strip()


def create_tables(con, *names: str) -> None:
    """只执行模板里的 CREATE TABLE 部分。

    为什么要单独做这一步：模板里 CREATE 和 INSERT 写在一起（放一起便于阅读），
    但建表时不该顺便把数据也插一遍——尤其是 INSERT 需要源文件路径的时候，
    拿空参数去执行会直接报 "No files found that match the pattern"。

    注意要先剥掉前导注释行：模板开头都有大段 `--` 说明，
    直接 `startswith("CREATE TABLE")` 会全部匹配不上（这个坑踩过一次）。
    """
    for name in names:
        for chunk in sql(name).split(";"):
            body = _strip_leading_comments(chunk)
            if body.upper().startswith("CREATE TABLE"):
                con.execute(body)


# 全部需要建的表（模板名），跑批前统一建一次。
# ⚠️ 这里的每一张表，它的 CREATE TABLE 都必须能在对应的 sql/*.sql 模板里找到——
# 不再单独维护一份"辅助表 DDL"。历史上 dwd_order_reject_audit 和 dim_date 的
# 建表语句各写了两份（一份在模板里、一份在这里），改了一处漏了另一处不会有任何
# 报错，只会让两张表结构悄悄分叉。现在只有一份真相：sql 模板。
#   · dim_date 的建表在 sql/dim_date.sql
#   · dwd_order_reject_audit 的建表在 sql/dwd_order_detail.sql（与它的 INSERT 同处）
ALL_TABLES: tuple[str, ...] = (
    "ods_order_info",
    "ods_user_log",
    "dim_sku",
    "dim_user",
    "dim_date",
    "dwd_order_detail",
    "dwd_page_log",
    "dws_traffic_user_1d",
    "dws_trade_user_1d",
    "ads_daily_metrics",
    "ads_gmv_category_1d",
    "ads_funnel_1d",
)


def ensure_schema(con) -> None:
    """一次性保证所有表结构存在。跑批的第一步调用，重复执行无副作用。"""
    create_tables(con, *ALL_TABLES)
