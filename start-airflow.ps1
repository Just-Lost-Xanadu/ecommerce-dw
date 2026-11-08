# 一键启动 Airflow 调度层（Windows PowerShell）。
# 为什么不用直接 docker compose up --build：
#   Compose 2.3x 的 bake 构建器在项目路径含中文/非 ASCII 时会报
#   "x-docker-expose-session-sharedkey contains non-printable ASCII"。
#   关掉 bake 后走普通 BuildKit，构建正常（实测）。
$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

$env:COMPOSE_BAKE = "false"
docker compose -f docker-compose.airflow.yml up --build -d

Write-Host ""
Write-Host "Airflow: http://127.0.0.1:8081  (admin / admin，演示值，生产必须改)"
Write-Host "停止:   docker compose -f docker-compose.airflow.yml down"
