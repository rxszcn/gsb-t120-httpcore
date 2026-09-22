"""协商流上限为 1 时，第二个请求带着四个超时下去会怎样。

跑法：.venv/bin/python repro/h2-stream-capacity.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os
from typing import List

import anyio
import hpack
import hyperframe.frame
import httpcore



class HangingStream(httpcore.AsyncMockStream):
    async def read(self, max_bytes: int, timeout=None) -> bytes:
        if not self._buffer:
            await anyio.sleep(999)  # server goes silent
        return self._buffer.pop(0)  # pragma: no cover


class HangingBackend(httpcore.AsyncMockBackend):
    async def connect_tcp(self, host, port, timeout=None, local_address=None,
                          socket_options=None):
        return HangingStream(list(self._buffer), http2=True)


def headers_frame(stream_id: int) -> bytes:
    return hyperframe.frame.HeadersFrame(
        stream_id=stream_id,
        data=hpack.Encoder().encode(
            [(b":status", b"200"), (b"content-type", b"plain/text")]
        ),
        flags=["END_HEADERS"],
    ).serialize()


BUFFER: List[bytes] = [
    hyperframe.frame.SettingsFrame(
        settings={hyperframe.frame.SettingsFrame.MAX_CONCURRENT_STREAMS: 1}
    ).serialize(),
    headers_frame(1),
]


async def main() -> None:
    pool = httpcore.AsyncConnectionPool(
        network_backend=HangingBackend(BUFFER), http2=True, max_connections=10
    )
    async with pool:
        async with pool.stream("GET", "https://example.com/") as held:
            conn = pool.connections[0]
            inner = conn._connection
            print("held:", held.status, "| pool:", repr(pool))
            print("connection:", repr(conn), "| negotiated max_streams:",
                  inner._max_streams)

            t = {"pool": 0.1, "read": 0.5, "write": 0.5, "connect": 0.5}
            outcome = {"result": "still running"}

            async def second() -> None:
                try:
                    await pool.request(
                        "GET", "https://example.com/", extensions={"timeout": t}
                    )
                    outcome["result"] = "returned a response"
                except BaseException as exc:  # noqa: BLE001
                    outcome["result"] = f"raised {type(exc).__name__}: {exc}"

            with anyio.move_on_after(2.0) as scope:
                await second()

            print("second request outcome:", outcome["result"])
            print("only stopped because we cancelled it at 2s:",
                  scope.cancelled_caught)
            print("timeouts given to the request:", t)
            print("pool after the stall:", repr(pool))
            print("connection bookkeeping:", pool.connections[0].info())


anyio.run(main)
