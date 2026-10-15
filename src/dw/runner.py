"""调度与编排：按依赖顺序执行各层，记录运行 trace，并在质量门禁失败时**阻断下游**。

依赖顺序：ods → dim → dwd → quality → dws → ads
  · dim 必须在 dwd 之前：dwd 的 LEFT JOIN dim_sku 需要维表已存在；
  · quality 放在 dws 之前：**质量不合格就不该让脏数据流进指标层**。

幂等：每层内部都是"按分区先删后插"或"全量重算"，所以同一个窗口重跑任意多次，
结果逐行一致（`tests/test_idempotent.py` 把这件事锁住）。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

from dw import (
    config,
    db,
    layer_ads,
    layer_dim,
    layer_dwd,
    layer_dws,
    layer_ods,
    paths,
    quality,
    runlog,
)


@dataclass
class RunResult:
    run_id: str
    batch_id: str
    window_start: date
    window_end: date
    rows: dict[str, int] = field(default_factory=dict)
    quality: list[quality.RuleResult] = field(default_factory=list)
    blocked: bool = False
    block_reason: str = ""
    meta_path: object | None = None

    @property
    def blocking_failures(self) -> list[quality.RuleResult]:
        return quality.blocking_failures(self.quality)

    def steps(self) -> list:
        """按 run_id 从运行记录里取回本轮的每一步。

        为什么从库里读而不是在内存里留一份：**跑批的进程可能中途挂掉**，
        而"这一轮跑到哪一步、哪一步慢、丢了多少行"恰恰是排查失败时最需要的信息。
        落库之后，即使进程没了，也能用 `dw.cli runs` 或直接查 SQLite 把现场捞出来。
        """
        con = db.meta_connect(self.meta_path)  # type: ignore[arg-type]
        try:
            return list(
                con.execute(
                    "SELECT * FROM run_steps WHERE run_id = ? "
                "ORDER BY coalesce(step_seq, 999999), started_at, step",
                    (self.run_id,),
                )
            )
        finally:
            con.close()


def _persist_quality(
    con_meta, run_id: str, results: list[quality.RuleResult]
) -> None:
    """把质量结果落库（保留历史，供趋势分析）。"""
    from datetime import datetime

    con_meta.executemany(
        "INSERT OR REPLACE INTO quality_results"
        "(run_id, layer, rule, layer_table, passed, actual, threshold,"
        " affected_rows, blocking, detail, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                run_id, r.layer, r.rule, r.table,
                1 if r.passed else 0, r.actual, r.threshold,
                r.affected_rows, 1 if r.blocking else 0, r.detail,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            for r in results
        ],
    )
    con_meta.commit()


def run_pipeline(
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    batch_id: str | None = None,
    force: bool = False,
    with_quality: bool = True,
    dashboard: bool = False,
    verbose: bool = True,
    warehouse_path=None,
    meta_path=None,
    prune_out_of_window: bool | None = None,
) -> RunResult:
    """跑完整链路。

    执行顺序由 `config.LAYERS` 定义（**它就是这个函数的循环体**，不再是文档）：
        ods → dim → dwd → quality（门禁）→ dws → ads

    force=True 时**忽略质量阻断继续跑完**——用于演示"如果不加门禁会发生什么"，
    也是真实运维里"已知问题、先出数"的应急通道（但要留痕）。

    prune_out_of_window 控制"幽灵分区清理"（见 db.prune_out_of_window）：
      · None（默认）= 自动判断：**只有这一轮的窗口恰好是项目的默认全窗口时**才清理。
        理由：全窗口跑批是"这一轮对整仓负责"，清掉窗口外的历史分区是对的；
        而 `run --limit 1` 这类补数场景，窗口之外的那些天是**合法数据**，绝不能删
        （`tests/test_idempotent.py::test_partial_window_rerun_keeps_other_days`
        就锁着这条性质）。
      · True / False = 显式指定（CLI 用 --prune / --no-prune 传进来）。
    """
    baseline = paths.load_baseline()
    default_start, default_end = paths.default_window(baseline)
    if window_start is None or window_end is None:
        window_start, window_end = default_start, default_end
    if window_start > window_end:
        # 空窗口不会在任何一层里"自然报错"：ODS 会安静地加载 0 行，
        # dwd 取 dates[0] 时才抛一个和真实原因毫无关系的 IndexError。
        # 所以在入口就把真正的原因说清楚。
        raise ValueError(
            f"统计窗口不合法：起始 {window_start} 晚于结束 {window_end}（窗口为空）。"
            "常见原因是传了负数的 --limit。"
        )
    if prune_out_of_window is None:
        prune_out_of_window = (window_start, window_end) == (default_start, default_end)

    # 显式 prune + 非全窗口 = 静默删数据，直接拒绝。
    # 为什么这是"静默"的：prune 删的是**窗口之外的所有历史分区**（覆盖所有含 dt 列的表，
    # 含 dwd/dws/ads），而这一轮只重建窗口内那几天。于是窗口外的数据**永久消失**，
    # 各步状态却全是 ok、退出码是 0；质量规则与对账又都带 `dt BETWEEN 窗口` 过滤，
    # 事后连"少了多少天"都看不见，只有 `dw.cli tables` 的全表计数能暴露。
    # CLI 侧（cmd_run）已经拒绝了 `--limit + --prune`，这里是同一道闸的兜底——
    # 防的是绕过 CLI 直接调 run_pipeline 的调用方。
    if prune_out_of_window and (window_start, window_end) != (default_start, default_end):
        raise ValueError(
            f"拒绝执行：这一轮的窗口是 {window_start} ~ {window_end}（非全窗口 "
            f"{default_start} ~ {default_end}），却要求清理窗口外的历史分区。"
            "这会把窗口外那些天的数据从所有含 dt 列的表里永久删掉，而本轮只重建窗口内的几天。"
            "要清幽灵分区请跑全窗口跑批（prune_out_of_window=None 时会自动判断）；"
            "只补某几天请不要传 prune_out_of_window=True。"
        )

    log = runlog.RunLog(meta_path=meta_path)
    batch = batch_id or log.run_id
    result = RunResult(
        run_id=log.run_id, batch_id=batch,
        window_start=window_start, window_end=window_end,
        meta_path=meta_path,
    )
    dates = paths.dates_between(window_start, window_end)

    if verbose:
        print(f"[run] run_id={log.run_id}  窗口 {window_start} ~ {window_end}  "
              f"({len(dates)} 天)  batch={batch}"
              + ("  [全窗口跑批：会清理窗口外的历史分区]"
                 if prune_out_of_window else ""))

    try:
        with db.warehouse(warehouse_path) as con:
            # ---- 建表：一次性保证全部表结构存在（幂等，重复执行无副作用）----
            paths.ensure_schema(con)

            # 每一层怎么跑。**顺序不在这里，而在 config.LAYERS 里**——
            # 用分派表 + 一个循环，好处是"执行顺序"只有一处定义，
            # 不会再出现"常量里写一个顺序、代码里跑另一个顺序"的历史问题。
            def _layer_steps() -> dict[str, Callable[[], dict]]:
                return {
                    "ods": lambda: layer_ods.run(con, dates, batch),
                    "dim": lambda: layer_dim.run(
                        con, dates, batch, window_start, window_end
                    ),
                    "dwd": lambda: layer_dwd.run(
                        con, dates, batch, window_start, window_end
                    ),
                    "dws": lambda: layer_dws.run(
                        con, dates, batch, window_start, window_end
                    ),
                    "ads": lambda: layer_ads.run(
                        con, dates, batch, window_start, window_end
                    ),
                }

            steps = _layer_steps()
            missing = [n for n in config.LAYERS if n != "quality" and n not in steps]
            if missing:
                raise ValueError(f"config.LAYERS 里的步骤没有实现：{missing}")

            for step in config.LAYERS:
                if step == "quality":
                    # ---- 质量门禁 ----
                    if with_quality:
                        with log.step("quality") as st:
                            results = quality.run_quality(con, dates)
                            result.quality = results
                            _persist_quality(log.con, log.run_id, results)
                            passed = sum(1 for x in results if x.passed)
                            st["message"] = f"{passed}/{len(results)} 条规则通过"
                            # --force 是"已知问题、先出数"的应急通道。既然它绕过了门禁，
                            # 就必须把"绕过了哪几条"写在这一步的运行记录里：否则
                            # `dw.cli runs` 里这一轮的"异常步"是 0，和一次干净跑批看起来
                            # 毫无区别，而 --force 的提示语承诺的是"会在 trace 里留痕"。
                            # 质量结果表里虽然有 passed=0 的行，但"这一轮是被强制放行的"
                            # 这件事只有运行记录能表达。
                            if force and result.blocking_failures:
                                forced = ", ".join(
                                    f"{r.rule}({r.actual:.4f} < {r.threshold})"
                                    for r in result.blocking_failures
                                )
                                st["message"] += (
                                    f"；--force 已忽略阻断级失败：{forced}"
                                )
                        if verbose:
                            print(quality.format_report(results))

                    # ---- 门禁判定：失败即阻断下游 ----
                    if result.blocking_failures and not force:
                        names = ", ".join(
                            f"{r.rule}({r.actual:.4f} < {r.threshold})"
                            for r in result.blocking_failures
                        )
                        reason = (
                            f"{len(result.blocking_failures)} 条阻断级规则未通过：{names}"
                        )
                        result.blocked = True
                        result.block_reason = reason
                        remaining = config.LAYERS[config.LAYERS.index(step) + 1:]
                        for skipped in remaining:
                            log.record_skipped(skipped, reason)
                        if verbose:
                            print(f"\n[BLOCKED] {reason}")
                            print(f"         下游（{'/'.join(remaining)}）已阻断，"
                                  "指标表未更新。")
                            print("         处理方式：修源数据/修规则后重跑；"
                                  "或用 --force 强制出数（会在 trace 里留痕）。")
                        return result
                    continue

                with log.step(step) as st:
                    r = steps[step]()
                    st.update(r)
                    if step == "ods" and prune_out_of_window:
                        # ---- 幽灵分区清理 ----
                        # ODS 的"按 dt 分区先删后插"只能保证"这一天不写两遍"，
                        # 删不掉别的窗口留下的分区。清理行数记进本步的 message，
                        # 这样"这一轮到底删了什么"随时可查（dw.cli runs）。
                        pruned = db.prune_out_of_window(con, window_start, window_end)
                        if pruned:
                            total_pruned = sum(pruned.values())
                            detail = ", ".join(
                                f"{k}:{v:,}" for k, v in sorted(pruned.items())
                            )
                            st["message"] = (
                                f"{r['message']} pruned={total_pruned:,} ({detail})"
                            )
                result.rows[step] = r["out_rows"]
                if step == "ads" and verbose:
                    print(f"[ads] 窗口 GMV = {r['total_gmv']:,.2f}")

            # ---- 看板（可选；它不是数仓分层，所以不在 config.LAYERS 里）----
            if dashboard:
                from dw import export_dashboard

                with log.step("dashboard") as st:
                    out = export_dashboard.export(
                        con, window_start, window_end, batch
                    )
                    st["message"] = str(out.name)
                if verbose:
                    print(f"[dashboard] 已生成：{out}")
    finally:
        if verbose:
            print("\n[run] 步骤明细：")
            print("\n".join(log.summary()))
        log.close()

    return result
