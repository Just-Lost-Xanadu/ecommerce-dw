"""DIM 层：维度表（用户 / 商品 / 日期）。

全量覆盖写：维度表都很小（本项目 2 万用户 + 800 商品 + 14 天日期），
直接清空重建最简单、也最不容易出错。真正的生产环境才需要拉链表做历史回溯
（SCD2 见 docs/可选加分项.md）。
"""

from __future__ import annotations

from datetime import date

import duckdb

from dw import config, paths


def run(
    con: duckdb.DuckDBPyConnection,
    dates: list[date],
    batch_id: str,
    window_start: date,
    window_end: date,
) -> dict:
    sku_src = paths.raw_csv(config.SRC_SKU)
    user_src = paths.raw_csv(config.SRC_USER_INFO)
    if not sku_src.is_file() or not user_src.is_file():
        raise FileNotFoundError("维度源文件不存在，请先运行：python -m dw.cli gen")

    base_params = {
        "batch_id": batch_id,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }

    # 商品维度：全量重建。
    # 必须先清空再写——维度表带 `sku_id` 主键，直接重复 INSERT 会撞主键
    # （这个报错踩过一次：第二次跑批就挂了，说明"看起来没问题"的写法未必幂等）。
    # 全量重建同时天然处理了"商品下架"（源里删掉的行不会残留在维表里）。
    #
    # 用事务把"删 + 插"包起来（与 layer_ods._load_one 同口径）：DuckDB 是自动提交的，
    # 中间失败（源文件被截断、主键冲突、磁盘满）会留下**空维表**，
    # 而空维表不会报错，只会让 DWD 的 LEFT JOIN 全变成孤儿商品——静默的错误结果，
    # 比直接失败严重得多。
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM dim_sku")
        con.execute(paths.render("dim_sku", src=sku_src.as_posix(), **base_params))

        # 用户维度：同上
        con.execute("DELETE FROM dim_user")
        con.execute(paths.render("dim_user", src=user_src.as_posix(), **base_params))

        # 日期维度：只重建窗口内的部分，窗口外的历史日期保留
        con.execute(paths.render("dim_date", window_start=window_start.isoformat(),
                                 window_end=window_end.isoformat()))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise

    n_sku = int(con.execute("SELECT count(*) FROM dim_sku").fetchone()[0])
    n_user = int(con.execute("SELECT count(*) FROM dim_user").fetchone()[0])
    n_date = int(con.execute("SELECT count(*) FROM dim_date").fetchone()[0])
    n_conflict = int(
        con.execute("SELECT count(*) FROM dim_user WHERE is_conflict").fetchone()[0]
    )

    return {
        "in_rows": n_sku + n_user + n_date,
        "out_rows": n_sku + n_user + n_date,
        "message": f"sku={n_sku:,} user={n_user:,} date={n_date}"
                   + (f" user_conflict={n_conflict}" if n_conflict else ""),
    }
