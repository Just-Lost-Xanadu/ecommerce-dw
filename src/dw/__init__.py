"""电商用户行为与订单离线数仓。

分层：ODS（原始）→ DWD（清洗+明细）→ DIM（维度）→ DWS（轻度汇总）→ ADS（指标）
引擎：DuckDB（单文件、零部署）；调度：自写 runner（见 dw.runner）。
"""

__version__ = "0.1.0"
