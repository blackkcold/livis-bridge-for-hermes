"""Hermes Agent 适配器：把 Livis 指令转成 `hermes -z` 子进程调用。

设计要点:
  - stdout 即结果文本（HerMes -z 单轮输出）
  - --usage-file 收集 token/花费（可选，用于统计）
  - 超时 -> kill；cancel -> kill（Hermes 无优雅取消信号）
  - 串行执行：同一时刻只跑一个任务（由 bridge 保证，这里只提供 run_one）
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("livis.adapter")

DEFAULT_HERMES_BIN = os.environ.get("HERMES_BIN", "hermes")
DEFAULT_TIMEOUT = 600  # 10 分钟


class HermesError(Exception):
    pass


@dataclass
class HermesResult:
    ok: bool
    text: str
    exit_code: int
    duration: float
    usage: dict | None = None
    error: str = ""

    @property
    def as_send_result(self) -> str:
        """转成给眼镜端显示的结果文本。"""
        if self.ok:
            return self.text.strip()
        return f"[Hermes 执行失败] exit={self.exit_code} {self.error[:200]}"


@dataclass
class HermesAdapter:
    bin: str = DEFAULT_HERMES_BIN
    timeout: float = DEFAULT_TIMEOUT
    usage_file: Path | None = field(default=None)

    async def run(self, content: str) -> HermesResult:
        """执行单条指令。任何异常都转成 HermesResult(ok=False)。"""
        import time

        start = time.monotonic()
        usage_path = self.usage_file
        if usage_path is None:
            data_dir = Path(os.environ.get("LIVIS_DATA_DIR", "./data"))
            usage_path = data_dir / f"usage-{int(start)}.json"
        usage_path.parent.mkdir(parents=True, exist_ok=True)

        cmd = [self.bin, "-z", content]
        if self.bin.endswith(".sh"):
            # fake_hermes.sh: 经 bash 执行，同样传 --usage-file
            cmd = ["bash", str(Path(self.bin).resolve()), "-z", content, "--usage-file", str(usage_path)]
        else:
            cmd = [self.bin, "-z", content, "--usage-file", str(usage_path)]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=self.timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            duration = time.monotonic() - start
            return HermesResult(
                ok=False,
                text="",
                exit_code=-1,
                duration=duration,
                error=f"超时 ({self.timeout}s) 已终止",
            )

        duration = time.monotonic() - start
        text = stdout_b.decode("utf-8", errors="replace")
        err = stderr_b.decode("utf-8", errors="replace").strip()

        usage = None
        if usage_path.exists():
            try:
                usage = json.loads(usage_path.read_text())
            except Exception:
                usage = None
            finally:
                usage_path.unlink(missing_ok=True)

        if proc.returncode != 0:
            return HermesResult(
                ok=False,
                text=text,
                exit_code=proc.returncode or -1,
                duration=duration,
                usage=usage,
                error=err or f"exit={proc.returncode}",
            )
        return HermesResult(
            ok=True,
            text=text,
            exit_code=0,
            duration=duration,
            usage=usage,
        )

    async def run_or_raise(self, content: str) -> HermesResult:
        """测试用：失败抛异常而非返回 ok=False。"""
        result = await self.run(content)
        if not result.ok:
            raise HermesError(result.error)
        return result


def make_fake_adapter() -> "HermesAdapter":
    """测试/开发环境：指向 fake_hermes.sh。"""
    return HermesAdapter(bin="bash", usage_file=None)
