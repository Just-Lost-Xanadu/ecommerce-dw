# Airflow 调度层（可选）

> 这一层只加**调度**：依赖、重试、补数、告警、产物可见。
> 数仓逻辑、质量门禁、幂等仍然全部在 `dw.cli run` 里，没有为了"看起来像生产"把五层拆散。

## 为什么只包一层

- `dw.cli run` 内部已经有严格顺序（ODS→DIM→DWD→quality→DWS→ADS）、质量门禁和幂等；
  拆成五个 Airflow task 后，任何一次手动触发都可能绕过门禁，形成"质量失败但 DWS 照跑"。
- DuckDB 是**单写者**。并发跑批会直接锁库，所以 DAG 必须 `max_active_runs=1`。
- 调度的真正价值是：每天定时、失败重试、补数入口、失败告警、日志集中。这些不需要拆层。

## 启动

```bash
# 1. 宿主机先生成一次源数据（也可以不执行，DAG 的 ensure_data 会自动造数）
python -m dw.cli gen

# 2. 起 Airflow（首次构建会下载镜像并安装依赖，约 3~8 分钟）
docker compose -f docker-compose.airflow.yml up --build -d

# 3. 打开 http://127.0.0.1:8081
#    账号 admin / admin（演示用，生产必须改）
```

DAG：`ecommerce_dw_daily`，每天 02:00 左右调度一次（`@daily`）。

> 实测（2026-10-20）：镜像构建成功，Web UI healthy；手动触发后
> `ensure_data → run → quality → recon → dashboard` 五个 task 全部 success。
> 宿主机 8080 被其他服务占用，所以 compose 把 Web UI 映射到 **8081**。

## DAG 结构

```
ensure_data → run → quality → recon → dashboard
```

| task | 做什么 | 失败会怎样 |
|---|---|---|
| `ensure_data` | 没有 `data/raw/order_info.csv` 时先 `gen` | 停在这里，后面不跑 |
| `run` | `run --limit 1`（T+1）或 `run`（全窗口） | 质量门禁失败 → 下游不跑，任务重试 2 次后标红 |
| `quality` | 输出最近一次质量报告 | 非 0 退出 → 重试后标红 |
| `recon` | 对账 + 倾斜量化 | 同上 |
| `dashboard` | 重渲染 HTML 看板 | 同上 |

**日常 vs 补数**：

- 默认 `full_refresh=false` → `run --limit 1`，只跑窗口最后一天；
- 在 Airflow 的 **Trigger DAG w/ config** 里把 `full_refresh` 设为 `true` → 跑完整 14 天窗口，
  并清理窗口外分区（这是本项目自己的补数/清幽灵分区入口）。

## 已知边界（不要吹成生产调度）

1. `--limit 1` 的"最后一天"来自 `_baseline.json` 的 `settle_date`，**不是 Airflow 的 logical_date**。
   真正的按日期补数需要给 CLI 增加 `--date`，本 DAG 没有假装支持。
2. 失败回调只写 Airflow 日志，没有接企业微信/钉钉/邮件；要接就在
   `dags/ecommerce_dw_dag.py` 的 `_on_failure` 里加 webhook。
3. Airflow 的元数据库用 Postgres，数仓仍是单机 DuckDB；这不是"分布式数仓"。
4. 没有做多环境（dev/prod）隔离、没有 CI/CD、没有 secrets backend；
   `docker-compose.airflow.yml` 里的 Fernet key 与 admin 密码都是**演示值**，上线必须换。

## 环境隔离

Airflow 2.10 对 pandas 等库有自己的版本约束，而本项目锁的是
`duckdb 1.5 / pandas 3.0 / numpy 2.5`。所以镜像里建了一个独立 venv：

```text
/opt/airflow/dwvenv/bin/python -m dw.cli ...
```

Airflow 自己的 Python 不装数仓依赖，升级 Airflow 不会把数仓依赖带崩；反过来也一样。
