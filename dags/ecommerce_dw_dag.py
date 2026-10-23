"""ecommerce-dw 的 Airflow 调度层（只做调度，不重写数仓链路）。

设计取舍：
- **不把五层拆成五个 task**。`dw.cli run` 内部已经有严格顺序、质量门禁和幂等，
  拆开会让"DWD 失败后 DWS 仍可能被手动触发"这类问题重新出现；DuckDB 又是单写者，
  并发跑批会直接锁库。调度的价值在依赖、重试、补数、告警，不在拆得细。
- 日常 `run --limit 1`（T+1 日批）；手动触发时把 `full_refresh` 打开就跑全窗口。
- `quality` / `recon` / `dashboard` 作为独立 task，失败可见、日志可查。

已知边界：
- `--limit 1` 的"最后一天"由 `_baseline.json` 的 settle_date 决定，不是 Airflow 的
  logical_date。真正的按日期补数需要给 CLI 增加 `--date`，本调度层没有假装支持。
- 失败回调只写 Airflow 日志；接企业微信/钉钉/邮件时，在 `_on_failure` 里加一个 webhook。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

PROJECT_DIR = "/opt/airflow/project"
DW_PYTHON = "/opt/airflow/dwvenv/bin/python"


def _dw(command: str) -> str:
    """在项目目录里用**独立 venv** 调 dw CLI。

    为什么不直接用 Airflow 自带的 Python：Airflow 2.10 对 pandas 等库有自己的约束，
    而本项目锁的是 duckdb 1.5 / pandas 3.0 / numpy 2.5。把项目装进独立 venv，
    两边互不污染，升级 Airflow 也不会把数仓依赖带崩。
    """
    return f"cd {PROJECT_DIR} && {DW_PYTHON} -m dw.cli {command}"


def _on_failure(context: dict) -> None:
    ti = context.get("task_instance")
    log.error(
        "ecommerce-dw 任务失败: dag=%s task=%s run_id=%s try=%s",
        context.get("dag").dag_id if context.get("dag") else "?",
        ti.task_id if ti else "?",
        context.get("run_id"),
        ti.try_number if ti else "?",
    )


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="ecommerce_dw_daily",
    description="电商离线数仓：跑批 -> 质量 -> 对账 -> 看板（单写者，禁止并发）",
    doc_md=__doc__,
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["ecommerce-dw", "duckdb"],
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            title="全窗口重跑",
            description="关闭：只跑窗口最后一天（T+1）；打开：跑完整 14 天窗口并清理窗口外分区",
        ),
    },
) as dag:
    ensure_data = BashOperator(
        task_id="ensure_data",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"test -f data/raw/order_info.csv && ls data/raw/by_dt/order_info_*.csv >/dev/null 2>&1 && ls data/raw/by_dt/user_log_*.csv >/dev/null 2>&1 && test -f data/raw/_baseline.json || {DW_PYTHON} -m dw.cli gen"
        ),
        doc_md="首次部署时 data/ 是空的：没有源数据就先造数（含 9 类脏数据与对账基准）。",
    )

    run = BashOperator(
        task_id="run",
        bash_command=_dw("run {{ '' if params.full_refresh else '--limit 1' }}"),
        doc_md="跑 ODS→DIM→DWD→质量门禁→DWS→ADS。阻断级规则失败时下游不会执行。",
    )

    quality = BashOperator(
        task_id="quality",
        bash_command=_dw("quality {{ '' if params.full_refresh else '--limit 1' }}"),
        doc_md=(
            "输出最近一次质量报告；非 0 退出会把任务标红。"
            "窗口必须与上面的 run 对齐（日批是 --limit 1）：固定按全窗口重算的话，"
            "首次部署时 14 天里 13 天 GMV=0，会把'从 0 涨到正数'的那天判成 bad day，"
            "结论与刚才的门禁相反。"
        ),
    )

    recon = BashOperator(
        task_id="recon",
        bash_command=_dw("recon"),
        doc_md="输出对账差异与倾斜量化（两个技术难点的现场证据）。",
    )

    dashboard = BashOperator(
        task_id="dashboard",
        bash_command=_dw("dashboard"),
        doc_md="离线重渲染 HTML 看板；产物在 dashboard/out/report.html。",
    )

    ensure_data >> run >> quality >> recon >> dashboard
