"""命令行入口。

    python -m dw.cli gen        # 生成源数据（含脏数据注入）
    python -m dw.cli run        # 跑完整链路（含质量门禁与看板）
    python -m dw.cli run --limit 1        # 只跑窗口内最后一天（模拟 T+1 日批）
    python -m dw.cli run --no-prune       # 全窗口跑批但不清理窗口外的历史分区
    python -m dw.cli run --prune          # 强制清理窗口外的历史分区（幽灵分区）
    python -m dw.cli quality    # 只看质量报告
    python -m dw.cli recon      # 对账 + 倾斜量化（两个技术难点的证据）
    python -m dw.cli tables     # 列出数仓所有表及行数
    python -m dw.cli runs       # 查看历史跑批记录
    python -m dw.cli dashboard  # 只重新生成看板
"""

from __future__ import annotations

import argparse
import math
import sys
from datetime import date

from dw import config, db, gen_source, paths, quality, runlog, runner


def _print_header(text: str) -> None:
    print(f"\n==== {text} ====")


def cmd_gen(args: argparse.Namespace) -> int:
    # `--scale 0` / 负数 / NaN / inf 都不是"小样本"，而是非法输入。
    # 之前 `use_scale` 用 `max(0.0001, scale)` 兜底，于是 `gen --scale -1`
    # **静默**把全量数据覆盖成 30 行，命令还返回 0；后面的 run/recon 全部失真。
    # 这类"看起来成功、实际把数据改坏"的参数必须在入口拒绝。
    if not math.isfinite(args.scale) or args.scale <= 0:
        raise SystemExit(
            f"--scale 必须是大于 0 的有限数，收到 {args.scale!r}。"
            "小样本请用 0.05 这类正数；0/负数/NaN/inf 会破坏源数据。"
        )
    # 这里**没有** --out：源数据写到哪由 config.RAW_DIR 决定（即 DW_RAW_DIR 环境变量），
    # 而 run / quality / recon 读的也是同一个 config.RAW_DIR。历史上这里有个 --out，
    # 它只改变"写"，读的一侧完全不知情——`gen --out D:\snap` 不报错也不生效，
    # 随后 run 要么拿旧数据算出错口径的指标、要么直接抛"找不到对账基准"。
    # 一个"写得到、读不到"的开关比没有这个开关更危险，所以直接去掉，
    # 换目录统一走 DW_DATA_DIR / DW_RAW_DIR。
    stats = gen_source.generate(seed=args.seed, scale=args.scale)
    _print_header("源数据生成完成")
    for k, v in stats["rows"].items():
        print(f"  {k:<10} {v:>12,} 行")
    print("  注入的脏数据：")
    for k, v in stats["injected"].items():
        print(f"    {k:<18} {v:>8,} 行")
    print(f"  对账基准 clean_gmv = {stats['baseline']['clean_gmv']:,.2f}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    baseline = paths.load_baseline()
    start, end = paths.default_window(baseline)
    default_start = start
    if args.limit < 0:
        # 负数不是"全窗口"，也不是"补数"：它会把 start 推到 end 之后，
        # 窗口变成空区间，dwd 取 dates[0] 时抛 IndexError（报错完全指不出真实原因）。
        # 直接在这里说不清楚。
        raise SystemExit(f"--limit 必须 >= 0（0 表示全窗口），收到 {args.limit}")
    if args.limit > 0:
        # 模拟 T+1 日批：只跑窗口内最后 N 天
        start = max(start, date.fromordinal(end.toordinal() - args.limit + 1))
    if args.prune and (start, end) != (default_start, end):
        # ⚠️ 这两个开关各自都合法，**凑在一起会静默删数据**：
        #   --limit N 把窗口收窄成最后 N 天；
        #   --prune  删掉「窗口之外的所有历史分区」（覆盖所有含 dt 列的表，含 dwd/dws/ads）。
        # 于是 `run --limit 1 --prune` 会把其余 13 天的数据从**十张表**里删掉，
        # 然后只重建那 1 天——各步状态全是 ok、退出码是 0，而 13 天的历史**永久消失**。
        # 更麻烦的是质量规则与对账都带 `dt BETWEEN 窗口` 过滤，事后完全看不见。
        # 所以这里直接拒绝，并说清两条各自正确的用法。
        raise SystemExit(
            "--limit 与 --prune 不能同时使用。\n"
            f"  · --limit {args.limit} 会把窗口收窄成 {start} ~ {end}；\n"
            "  · 而 --prune 会删掉**窗口之外的所有历史分区**"
            "（覆盖所有含 dt 列的表，包括 dwd / dws / ads）。\n"
            "  两者同时用，窗口外那几天的合法数据会被静默删除，而退出码仍然是 0。\n"
            "  想清幽灵分区 → 去掉 --limit，跑一次全窗口跑批"
            "（全窗口跑批本身就会顺带清理窗口外分区）；\n"
            "  只补某一天   → 去掉 --prune（补数默认不会清理窗口外的数据）。"
        )
    result = runner.run_pipeline(
        window_start=start, window_end=end,
        force=args.force, with_quality=not args.no_quality,
        dashboard=not args.no_dashboard,
        # None = 自动：全窗口跑批才清理窗口外的历史分区，补数场景不清理
        prune_out_of_window=args.prune,
    )
    if result.blocked:
        return 2
    return 0


def cmd_quality(args: argparse.Namespace) -> int:
    baseline = paths.load_baseline()
    start, end = paths.default_window(baseline)
    # 默认是全窗口重算。加 --limit N 时按 T+1 日批的窗口算（最后 N 天）——
    # 这条开关存在的原因：Airflow 日批是 `run --limit 1`，门禁在 1 天窗口上判；
    # 紧接着的 quality task 如果固定按 14 天重算，两边结论可以相反
    # （首次部署时 14 天里 13 天 GMV=0，会把"从 0 涨到正数"的那天判成 bad day）。
    limit = getattr(args, "limit", 0)
    if limit < 0:
        raise SystemExit(f"--limit 必须 >= 0（0 表示全窗口），收到 {limit}")
    if limit > 0:
        start = max(start, date.fromordinal(end.toordinal() - limit + 1))
    with db.warehouse() as con:
        results = quality.run_quality(con, paths.dates_between(start, end))
    _print_header(f"质量报告（窗口 {start} ~ {end}）")
    print(quality.format_report(results))
    bad = quality.blocking_failures(results)
    if bad:
        print(f"\n阻断级失败 {len(bad)} 条：{', '.join(r.rule for r in bad)}")
        return 2
    print("\n无阻断级失败。")
    return 0


def cmd_recon(args: argparse.Namespace) -> int:
    from dw import checks

    baseline = paths.load_baseline()
    start, end = paths.default_window(baseline)
    with db.warehouse() as con:
        rec = checks.reconcile(
            con, baseline["baseline"]["clean_gmv"],
            baseline["baseline"]["clean_orders"], start, end,
        )
        skew = checks.skew_stats(con, config.HOT_SKU_ID, start, end)
    _print_header("对账与倾斜")
    print(rec.format())
    print()
    print(skew.format())
    return 0


def cmd_tables(args: argparse.Namespace) -> int:
    _print_header("数仓表清单")
    with db.warehouse() as con:
        tables = db.list_tables(con)
        for t in tables:
            n = db.table_count(con, t)
            print(f"  {t:<28} {n:>12,} 行")
    print(f"\n  共 {len(tables)} 张表")
    return 0


def cmd_runs(args: argparse.Namespace) -> int:
    # SQLite 的 LIMIT -1 表示"不限制"：负数会被静默当成"列出全部"，
    # 而其他命令的 --limit 都是"必须 >= 0"。这里显式拒绝，避免同一个参数
    # 在不同子命令里含义不一致。
    if args.limit < 1:
        raise SystemExit(f"--limit 必须 >= 1，收到 {args.limit}")
    _print_header("历史跑批")
    rows = runlog.recent_runs(limit=args.limit)
    if not rows:
        print("  暂无记录，先跑 python -m dw.cli run")
        return 0
    print(f"  {'run_id':<24}{'开始时间':<22}{'成功步':>8}{'异常步':>8}{'总耗时ms':>10}")
    for r in rows:
        print(f"  {r['run_id']:<24}{r['started_at']:<22}"
              f"{r['ok_steps']:>8}{r['bad_steps']:>8}{r['total_ms']:>10,}")
    return 0


def cmd_dashboard(args: argparse.Namespace) -> int:
    from dw import export_dashboard

    baseline = paths.load_baseline()
    start, end = paths.default_window(baseline)
    with db.warehouse() as con:
        out = export_dashboard.export(con, start, end, batch_id="manual")
    _print_header("看板")
    print(f"  已生成：{out}")
    print("  用浏览器直接打开即可（不依赖任何 BI 平台）。")
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="dw", description="电商用户行为与订单离线数仓（DuckDB 实现）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("gen", help="生成源数据（含脏数据注入）")
    p.add_argument("--seed", type=int, default=20261005)
    p.add_argument("--scale", type=float, default=1.0,
                   help="数据量缩放系数（测试用小样本，例如 0.02）")
    p.set_defaults(func=cmd_gen)

    p = sub.add_parser("run", help="跑完整链路")
    p.add_argument("--limit", type=int, default=0,
                   help="只跑窗口最后 N 天（0=全窗口；必须 >= 0）")
    p.add_argument("--force", action="store_true", help="忽略质量门禁继续跑（会留痕）")
    p.add_argument("--no-quality", action="store_true", help="跳过质量校验")
    p.add_argument("--no-dashboard", action="store_true", help="跳过看板生成")
    p.add_argument("--prune", dest="prune", action="store_true", default=None,
                   help="强制清理窗口外的历史分区（幽灵分区）")
    p.add_argument("--no-prune", dest="prune", action="store_false",
                   help="不清理窗口外的历史分区（补数 / T+1 日批场景）")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("quality", help="只输出质量报告")
    p.add_argument("--limit", type=int, default=0,
                   help="只统计窗口内最后 N 天（0 = 全窗口重算，默认）；"
                        "与 `run --limit N` 对齐时用，避免两边窗口不同、结论相反")
    p.set_defaults(func=cmd_quality)

    p = sub.add_parser("recon", help="对账与倾斜量化")
    p.set_defaults(func=cmd_recon)

    p = sub.add_parser("tables", help="列出数仓表与行数")
    p.set_defaults(func=cmd_tables)

    p = sub.add_parser("runs", help="查看历史跑批记录")
    p.add_argument("--limit", type=int, default=10)
    p.set_defaults(func=cmd_runs)

    p = sub.add_parser("dashboard", help="重新生成看板")
    p.set_defaults(func=cmd_dashboard)

    return ap


def main(argv: list[str] | None = None) -> int:
    # 中文在 Windows 终端下默认按 GBK 输出会乱码，这里显式要求 UTF-8。
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
        except (AttributeError, ValueError):
            pass

    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
