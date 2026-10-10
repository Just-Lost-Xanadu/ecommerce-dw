"""数仓连接失败时的可操作性回归。

为什么单独有这一个文件：被中断的跑批会留下持锁的子进程，之后**每个命令都失败**，
而 DuckDB 的原始报错是

    IO Error: Could not set lock on file "...warehouse.duckdb":
    Conflicting lock is held in ...\\python.exe (PID 12345)

问题在于 **那个 PID 往往是已经退出的进程**——照着它去任务管理器里找，根本找不到，
于是人会卡在"到底是哪个进程占着"上。所以这里的性质不是"能连上"，而是
**"连不上时给出的必须是可执行的下一步"**。

注意：这个测试必须用**真正的第二个进程**来持锁。同一个进程里连着开两次
`duckdb.connect(同一个文件)` 是允许的，测不出锁冲突。
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from dw import db

# 子进程：拿住写连接不放，直到被杀。
_HOLDER = (
    "import duckdb, sys, time\n"
    "con = duckdb.connect(sys.argv[1])\n"
    "con.execute('CREATE TABLE held(i INTEGER)')\n"
    "print('ready', flush=True)\n"
    "time.sleep(60)\n"
)


@pytest.fixture()
def locked_warehouse(tmp_path):
    """起一个持锁子进程，yield 库文件路径，结束时确保它被清掉。"""
    path = tmp_path / "locked.duckdb"
    proc = subprocess.Popen(
        [sys.executable, "-c", _HOLDER, str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().strip()
        assert line == "ready", f"持锁子进程没起来：{line!r} stderr={proc.stderr.read() if proc.stderr else ''}"
        yield path
    finally:
        proc.kill()
        proc.wait(timeout=10)


def test_locked_warehouse_gives_actionable_error(locked_warehouse):
    """锁冲突时必须抛 RuntimeError，并且信息里带着可执行的恢复步骤。"""
    with pytest.raises(RuntimeError) as excinfo:
        db.connect(locked_warehouse)

    msg = str(excinfo.value)

    # 1) 指出是哪个文件（否则多套环境时没法定位）
    assert str(locked_warehouse) in msg, "报错里没有给出库文件路径"

    # 2) 必须点明"PID 可能是死的"这个最容易误导人的点
    assert "PID" in msg, "没有提醒 DuckDB 报出的 PID 可能已失效"

    # 3) 必须给出可直接照做的命令，而不只是描述现象
    assert "Stop-Process" in msg, "没有给出结束残留进程的命令"

    # 4) 必须给出最后的兜底手段
    assert "DW_WAREHOUSE" in msg, "没有给出换库路径这个兜底手段"


def test_unlocked_warehouse_connects_normally(tmp_path):
    """对照：没有冲突时照常连上，别把正常路径也改成抛错。"""
    con = db.connect(tmp_path / "free.duckdb")
    try:
        assert con.execute("SELECT 1").fetchone()[0] == 1
    finally:
        con.close()


def test_non_lock_failure_does_not_claim_it_is_a_lock(tmp_path):
    """非锁类失败不能照搬"残留进程"那套话术——那会把人引向错误的方向。"""
    blocked = tmp_path / "as_a_directory"
    blocked.mkdir()  # 用一个目录冒充库文件，制造非锁类的连接失败
    with pytest.raises(RuntimeError) as excinfo:
        db.connect(blocked)
    msg = str(excinfo.value)
    assert "Stop-Process" not in msg, "非锁类失败却给了『杀进程』的指引"
    assert "锁冲突" in msg and "不像" in msg, "没有明确排除锁冲突这条原因"
