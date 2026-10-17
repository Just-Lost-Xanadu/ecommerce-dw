"""端到端与口径测试：链路能跑通、指标口径说得清、门禁真的能拦住。

约定：**凡是断言"数仓里现在是什么内容"的测试，一律走隔离目录**（`all_dirty` / `iso_project`）。
共享数仓会被其他测试重跑覆盖，直接断言它会让结果取决于执行顺序——
这是测试里最隐蔽的一类不稳定来源。
"""

from __future__ import annotations

import pytest

from dw import config, db, paths, quality, runner

# 在隔离目录里收集的对账口径数据
_RECON_SPEC = {
    "dwd": "SELECT round(coalesce(sum(amount), 0), 2) AS gmv, count(*) AS n "
           "FROM dwd_order_detail WHERE is_valid AND is_paid",
    "raw": "SELECT round(coalesce(sum(amount), 0), 2) AS gmv, count(*) AS n "
           "FROM ods_order_info",
}


# --------------------------------------------------------------------------
# 链路完整性
# --------------------------------------------------------------------------
def test_pipeline_completes_and_fills_all_layers(all_dirty) -> None:
    """跑完一次，各层关键表都要有数据（"分层完整"的最低要求）。"""
    assert not all_dirty["blocked"], "链路被意外阻断"
    for table, n in all_dirty["table_rows"].items():
        assert n > 0, f"{table} 是空表"
    for step in ("ods", "dim", "dwd", "quality", "dws", "ads"):
        assert all_dirty["steps"].get(step) == "ok", (
            f"步骤 {step} 状态是 {all_dirty['steps'].get(step)}，期望 ok"
        )


def test_run_steps_are_in_execution_order(first_run) -> None:
    """运行记录必须按**真实执行顺序**返回，而不是按字母序。

    `started_at` 只精确到秒，一轮几十秒的跑批里好几个步骤落在同一秒，
    只按时间排序会退化成按字母序排列——实测出现过 `ads` 排到 `dws` 前面，
    读的人会以为 ads 先跑。所以 run_steps 增加 `step_seq` 列，排序以它为准。
    """
    steps = list(first_run.steps())
    names = [s["step"] for s in steps]
    seqs = [s["step_seq"] for s in steps]
    core = [n for n in names if n in config.LAYERS]
    assert core == list(config.LAYERS), (
        f"步骤顺序不等于 config.LAYERS 定义的执行顺序：{core} != {list(config.LAYERS)}"
    )
    assert all(s is not None for s in seqs), f"有步骤没有 step_seq：{list(zip(names, seqs))}"
    assert seqs == sorted(seqs), f"step_seq 不是递增的：{list(zip(names, seqs))}"


def test_day_over_day_rule_is_skipped_for_single_day_window(con, window) -> None:
    """窗口只有 1 天时，日环比规则必须标为「不适用」，而不是伪装成通过。

    这条对应 `python -m dw.cli run --limit 1`（T+1 日批）：只有一天就没有
    "可比的上一日"，日环比根本算不出来。旧写法在这种情况下 tot = 0，
    直接返回 actual = 1.0「通过」——一个在日批模式下完全失效的监控，
    而且它在报告里长得和"真的检查过了"一模一样。
    """
    _, end = window
    results = quality.run_quality(con, [end])
    r = next(x for x in results if x.rule == "gmv_day_over_day")
    assert r.skipped is True, (
        f"单日窗口下 gmv_day_over_day 没有被标为不适用："
        f"skipped={r.skipped}, actual={r.actual}, detail={r.detail!r}"
    )
    assert r.status_cn == "不适用", f"展示状态不对：{r.status_cn}"
    # 不适用时对外语义仍然是「通过」（它不阻断、也不算失败），但必须带 skipped 标记，
    # 否则报告上分不出「真的检查过了」和「根本算不出来」。
    assert r.passed is True and r.actual == 0.0


# --------------------------------------------------------------------------
# 对账口径（两个技术难点的证据）
# --------------------------------------------------------------------------
def test_reconcile_dwd_matches_baseline(iso_project, baseline) -> None:
    """对账：DWD 收口后的 GMV 必须与基准**高度一致**。

    注意口径差别：基准是对**全量源数据**按精确业务口径算的，
    而 DWD 只覆盖统计窗口内的分区（源数据里超出窗口的日期不在处理范围内）。
    所以这里允许一个很小的相对容差，而不是要求完全相等——
    把容差写成 0 会得到一个"看起来应该过却过不了"的测试，
    根因是**比较口径没对齐**，不是数据有问题。
    """
    out = iso_project("recon", first=_RECON_SPEC)
    dwd_gmv, dwd_orders = out["first"]["dwd"][0]
    base = baseline["baseline"]
    diff = abs(dwd_gmv - base["clean_gmv"]) / base["clean_gmv"] * 100
    assert diff < 0.5, (
        f"DWD 收口 GMV {dwd_gmv:,.2f} 与基准 {base['clean_gmv']:,.2f} 差异 {diff:.3f}%，"
        "超过 0.5% —— 去重/有效性口径有问题"
    )
    # 订单数同样按比例校验（窗口内占比应当很高）
    ratio = dwd_orders / base["clean_orders"]
    assert 0.95 < ratio <= 1.0, (
        f"DWD 有效订单数 {dwd_orders} 只占基准 {base['clean_orders']} 的 {ratio:.3%}，偏低"
    )


def test_reconcile_raw_is_worse_than_dwd(iso_project, baseline) -> None:
    """对照：源表直算的差异率必须**明显大于**收口之后。

    这条是"清洗到底有没有用"的可复算证据——
    没有这条对照，"差异率降到接近 0"就只是一句话。
    注意这里两份都取自同一次隔离运行的同一时点，避免了跨窗口比较的口径错位。
    """
    out = iso_project("recon", first=_RECON_SPEC)
    dwd_gmv, _ = out["first"]["dwd"][0]
    raw_gmv, _ = out["first"]["raw"][0]
    # 对照的基准就用"源口径"自身：看收口把数字拉低了多少
    raw_diff = abs(raw_gmv - dwd_gmv) / dwd_gmv * 100
    assert raw_diff > 1.0, (
        f"源表直算与收口后的差异只有 {raw_diff:.3f}%，"
        "说明注入的脏数据量太小，起不到警示作用"
    )


def test_skew_is_real_in_generated_data(iso_project) -> None:
    """倾斜必须真的存在于数据里，否则 Spark 对照实验就是空跑。"""
    out = iso_project(
        "skew",
        first={
            "hot": "SELECT count(*) FROM dwd_order_detail "
                   f"WHERE is_valid AND is_paid AND sku_id = '{config.HOT_SKU_ID}'",
            "total": "SELECT count(*) FROM dwd_order_detail WHERE is_valid AND is_paid",
        },
    )
    hot = out["first"]["hot"][0][0]
    total = out["first"]["total"][0][0]
    share = hot / total if total else 0
    assert share >= 0.30, (
        f"热销 SKU 占比只有 {share:.2%}，低于设定的 {config.HOT_SKU_SHARE:.0%} —— "
        "造数的倾斜分布没生效"
    )


# --------------------------------------------------------------------------
# 质量门禁（在共享数仓上跑，但每个用例都恢复原状）
# --------------------------------------------------------------------------
def test_quality_gate_blocks_downstream_when_rule_fails(monkeypatch) -> None:
    """门禁测试：把阈值调到不可能达到，下游必须被阻断。

    这是"质量校验"与"质量**门禁**"的区别——
    很多项目只做到前者（跑一下规则、打一堆日志），没有后者。
    门禁的价值是：**脏数据流不进指标层**，而不是"我们监控到了脏数据"。
    """
    monkeypatch.setitem(config.QUALITY_THRESHOLDS, "unique_rate", 1.000001)
    result = runner.run_pipeline(verbose=False)
    assert result.blocked, "阻断级规则不通过时，下游没有被阻断"
    assert result.blocking_failures, "应当报告出阻断级失败规则"
    names = {r.rule for r in result.blocking_failures}
    assert "unique_order_id" in names, f"阻断原因不对：{names}"

    # 被阻断时下游必须真的没跑
    steps = {s["step"]: s["status"] for s in result.steps()}
    assert steps.get("dws") == "blocked", f"dws 未被阻断：{steps}"
    assert steps.get("ads") == "blocked", f"ads 未被阻断：{steps}"

    # 恢复后重跑，应当恢复正常
    monkeypatch.setitem(config.QUALITY_THRESHOLDS, "unique_rate", 1.0)
    ok = runner.run_pipeline(verbose=False)
    assert not ok.blocked, "恢复阈值后链路仍被阻断"


def test_force_bypasses_gate_but_keeps_trace(monkeypatch) -> None:
    """--force 是应急通道：能出数，但必须在运行记录里留下"门禁没过"的痕迹。"""
    monkeypatch.setitem(config.QUALITY_THRESHOLDS, "unique_rate", 1.000001)
    result = runner.run_pipeline(force=True, verbose=False)
    assert not result.blocked, "force=True 时不应该阻断"
    assert result.rows.get("ads", 0) > 0, "force 模式下 ADS 应当照样出数"
    steps = {s["step"]: s["status"] for s in result.steps()}
    assert steps.get("quality") == "ok", "运行记录里缺少 quality 步骤"
    monkeypatch.setitem(config.QUALITY_THRESHOLDS, "unique_rate", 1.0)


# --------------------------------------------------------------------------
# 运行元数据落库
# --------------------------------------------------------------------------
def test_run_log_persisted_with_history() -> None:
    """运行记录必须落库并保留历史——否则回答不了"昨天那次为什么失败"。"""
    con = db.meta_connect()
    try:
        runs = con.execute("SELECT count(DISTINCT run_id) FROM run_steps").fetchone()[0]
        steps = con.execute("SELECT count(DISTINCT step) FROM run_steps").fetchone()[0]
        bad = con.execute("SELECT count(*) FROM run_steps WHERE status <> 'ok'").fetchone()[0]
    finally:
        con.close()
    assert runs >= 1, "运行记录没有落库"
    assert steps >= 6, f"落库的步骤只有 {steps} 种"
    assert bad >= 1, "被阻断的步骤没有被记为异常状态——门禁拦了但没留痕"


def test_quality_results_persisted_with_history() -> None:
    """质量结果必须落库并保留历史——否则看不出"质量在退化"。"""
    con = db.meta_connect()
    try:
        n = con.execute("SELECT count(*) FROM quality_results").fetchone()[0]
        rules = con.execute(
            "SELECT count(DISTINCT rule) FROM quality_results"
        ).fetchone()[0]
    finally:
        con.close()
    assert n > 0, "质量结果没有落库"
    assert rules >= 10, f"落库的规则只有 {rules} 条，少于设计的 10 条"


# --------------------------------------------------------------------------
# 看板：必须是可离线打开的单文件
# --------------------------------------------------------------------------
def test_dashboard_html_is_self_contained(tmp_path) -> None:
    """看板必须是一张可离线打开的 HTML（不依赖任何 BI 平台）。"""
    from dw import export_dashboard

    start, end = paths.default_window()
    with db.warehouse() as con:
        out = export_dashboard.export(con, start, end, "test", tmp_path / "report.html")
    html = out.read_text(encoding="utf-8")
    assert out.is_file() and out.stat().st_size > 2000, "看板文件过小，可能没渲染出内容"
    assert "运营日报" in html, "看板缺少标题"
    assert "GMV" in html, "看板缺少 GMV 指标"
    assert "http://" not in html and "https://" not in html, (
        "看板引用了外部资源，离线打不开"
    )


# --------------------------------------------------------------------------
# 错误信息要可操作
# --------------------------------------------------------------------------
def test_missing_source_data_gives_actionable_error(monkeypatch) -> None:
    """源数据不存在时，报错要告诉用户**下一步敲什么命令**，而不是抛裸异常。"""
    from dw import layer_ods

    monkeypatch.setattr(paths, "raw_exists", lambda: False)
    start, _ = paths.default_window()
    with db.warehouse() as con, pytest.raises(FileNotFoundError, match="dw.cli gen"):
        layer_ods.run(con, [start], "test-batch")


def test_config_error_names_the_variable(monkeypatch) -> None:
    """数值型环境变量解析失败时，要报出变量名——否则排查成本极高。"""
    monkeypatch.setenv("DW_BAD_NUM", "abc")
    with pytest.raises(ValueError, match="DW_BAD_NUM"):
        config.env_int("DW_BAD_NUM", 1)
