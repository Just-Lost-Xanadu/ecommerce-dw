"""运行链路记录（trace）。

每次跑批生成一个 run_id，每一步（ods / dwd / quality / dws / ads）落一行记录：
**耗时、输入行数、输出行数、丢弃行数、状态、失败原因**。

为什么必须做这一层（而不是打 print）：
1. 出了问题要能回答"**哪一步、什么时候、慢在哪、丢了多少数据**"，日志是要能查询的；
2. 质量指标的**趋势**要看历史——只看当次结果，看不出"质量在慢慢退化"；
3. 失败也要落库（`status=failed`），因为**失败的那次才是排查的起点**。
"""

from __future__ import annotations

import contextlib
import sqlite3
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime

from dw import db


def new_run_id() -> str:
    """可读的运行号：日期时间 + 4 位随机后缀（同秒并发也能区分）。"""
    return f"{datetime.now():%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:4]}"


@dataclass
class RunLog:
    """一次跑批的运行记录。每一步通过 `step()` 上下文管理器计时并落库。"""

    run_id: str = field(default_factory=new_run_id)
    meta_path: object | None = None
    _con: sqlite3.Connection | None = field(default=None, repr=False)

    @property
    def con(self) -> sqlite3.Connection:
        if self._con is None:
            self._con = db.meta_connect(self.meta_path)  # type: ignore[arg-type]
        return self._con

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    # ---- 记录 ------------------------------------------------------------
    def _write(
        self,
        step: str,
        status: str,
        duration_ms: int,
        in_rows: int | None,
        out_rows: int | None,
        dropped_rows: int | None,
        message: str | None,
    ) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO run_steps"
            "(run_id, step, status, started_at, duration_ms, in_rows, out_rows,"
            " dropped_rows, message) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                self.run_id,
                step,
                status,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                duration_ms,
                in_rows,
                out_rows,
                dropped_rows,
                message,
            ),
        )
        self.con.commit()

    @contextlib.contextmanager
    def step(self, name: str) -> Iterator[dict]:
        """计时的步骤上下文。

        用法：
            with run.step("dwd") as st:
                st["in_rows"] = 100
                ...
                st["out_rows"] = 95

        异常会照常上抛（**不能被吞掉**），同时把这一步记为 failed。
        注意用 BaseException 捕获：KeyboardInterrupt / CancelledError 也要留下记录，
        否则"跑到一半被 Ctrl-C"在库里看不到任何痕迹。
        """
        started = time.perf_counter()
        info: dict = {
            "in_rows": None,
            "out_rows": None,
            "dropped_rows": None,
            "message": None,
        }
        try:
            yield info
        except BaseException as exc:  # noqa: BLE001 - 记录后原样上抛
            elapsed = int((time.perf_counter() - started) * 1000)
            self._write(
                name, "failed", elapsed,
                info.get("in_rows"), info.get("out_rows"),
                info.get("dropped_rows"), f"{type(exc).__name__}: {exc}",
            )
            raise
        else:
            elapsed = int((time.perf_counter() - started) * 1000)
            self._write(
                name, "ok", elapsed,
                info.get("in_rows"), info.get("out_rows"),
                info.get("dropped_rows"), info.get("message"),
            )

    def record_blocked(self, step: str, reason: str, duration_ms: int = 0) -> None:
        """质量门禁拦截下游时调用——被拦住也是一个需要留痕的结果。"""
        self._write(step, "blocked", duration_ms, None, None, None, reason)

    def record_skipped(self, step: str, reason: str) -> None:
        """因上游被阻断而跳过。"""
        self._write(step, "blocked", 0, None, None, None, f"上游阻断，已跳过：{reason}")

    # ---- 查询 ------------------------------------------------------------
    def steps(self) -> list[sqlite3.Row]:
        return list(
            self.con.execute(
                "SELECT * FROM run_steps WHERE run_id = ? ORDER BY started_at, step",
                (self.run_id,),
            )
        )

    def summary(self) -> list[str]:
        """给终端打印用的紧凑摘要。"""
        out = []
        for r in self.steps():
            rowinfo = ""
            if r["out_rows"] is not None:
                rowinfo = f" out={r['out_rows']}"
            if r["dropped_rows"]:
                rowinfo += f" dropped={r['dropped_rows']}"
            msg = f"  {r['message']}" if r["message"] else ""
            out.append(
                f"  {r['step']:<10} {r['status']:<8} {r['duration_ms']:>6}ms{rowinfo}{msg}"
            )
        return out


def recent_runs(limit: int = 10, meta_path: object | None = None) -> list[sqlite3.Row]:
    """最近几次跑批（按开始时间倒序）。"""
    con = db.meta_connect(meta_path)  # type: ignore[arg-type]
    try:
        return list(
            con.execute(
                "SELECT run_id, min(started_at) AS started_at, "
                "  sum(CASE WHEN status='ok' THEN 1 ELSE 0 END) AS ok_steps, "
                "  sum(CASE WHEN status<>'ok' THEN 1 ELSE 0 END) AS bad_steps, "
                "  sum(duration_ms) AS total_ms "
                "FROM run_steps GROUP BY run_id ORDER BY started_at DESC LIMIT ?",
                (limit,),
            )
        )
    finally:
        con.close()
