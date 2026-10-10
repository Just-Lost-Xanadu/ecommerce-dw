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
import sys
from datetime import date

from dw import config, db, gen_source, paths, quality, runlog, runner


def _print_header(text: str) -> None:
    print(f"\n==== {text} ====")


def cmd_gen(args: argparse.Namespace) -> int:
    stats = gen_source.generate(
        seed=args.seed,
        out_dir=__import__("pathlib").Path(args.out) if args.out else None,
        scale=args.scale,
    )
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
    if args.limit:
        # 模拟 T+1 日批：只跑窗口内最后 N 天
        start = max(start, date.fromordinal(end.toordinal() - args.limit + 1))
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
    with db.warehouse() as con:
        results = quality.run_quality(con, paths.dates_between(start, end))
    _print_header("质量报告")
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
    p.add_argument("--out", type=str, default=None, help="输出目录（默认 data/raw）")
    p.set_defaults(func=cmd_gen)

    p = sub.add_parser("run", help="跑完整链路")
    p.add_argument("--limit", type=int, default=0, help="只跑窗口最后 N 天（0=全窗口）")
    p.add_argument("--force", action="store_true", help="忽略质量门禁继续跑（会留痕）")
    p.add_argument("--no-quality", action="store_true", help="跳过质量校验")
    p.add_argument("--no-dashboard", action="store_true", help="跳过看板生成")
    p.add_argument("--prune", dest="prune", action="store_true", default=None,
                   help="强制清理窗口外的历史分区（幽灵分区）")
    p.add_argument("--no-prune", dest="prune", action="store_false",
                   help="不清理窗口外的历史分区（补数 / T+1 日批场景）")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("quality", help="只输出质量报告")
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
