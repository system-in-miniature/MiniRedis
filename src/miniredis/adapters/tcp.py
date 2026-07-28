from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from miniredis.adapters.resp2 import (
    RespDecoder,
    RespProtocolError,
    encode_outbound,
    frame_to_request,
)
from miniredis.commands.request import CommandRequest
from miniredis.core.outbound import (
    OutboxClosed,
    ReplyMessage,
    ServerClosed,
    SessionEndpoint,
)
from miniredis.core.reply import Failure

if TYPE_CHECKING:
    from miniredis.runtime import MiniRedis


@dataclass(frozen=True, slots=True)
class TcpAddress:
    host: str
    port: int


class TcpSession:
    def __init__(
        self,
        runtime: MiniRedis,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        outbox_limit: int,
        max_buffered_frames: int,
        on_closed: Callable[[TcpSession], None],
        writer_starts_paused: bool,
    ) -> None:
        self.runtime = runtime
        self.reader = reader
        self.writer = writer
        self.decoder = RespDecoder()
        self.session_id = runtime.new_session_id()
        self._frames: deque[CommandRequest] = deque()
        self._max_buffered_frames = max_buffered_frames
        self._on_closed = on_closed
        self._reader_task: asyncio.Task[None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._pending_commands: set[asyncio.Task[None]] = set()
        self._admission_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._reader_quiescing = False
        self._reader_quiesced = asyncio.Event()
        self._transport_finishing = False
        self._transport_finished = asyncio.Event()
        self._writer_allowed = asyncio.Event()
        if not writer_starts_paused:
            self._writer_allowed.set()
        self._closed = False
        self.endpoint = SessionEndpoint(
            session_id=self.session_id,
            capacity=outbox_limit,
            reply_via_outbox=True,
            on_slow=runtime.session_became_slow,
            close_transport=self._request_transport_close,
        )

    @property
    def reader_task(self) -> asyncio.Task[None]:
        if self._reader_task is None:
            raise RuntimeError("TCP session is not started")
        return self._reader_task

    async def start(self) -> None:
        self.runtime.register_session(self.endpoint)
        self._writer_task = asyncio.create_task(
            self._write_loop(),
            name=f"miniredis:tcp-writer:{self.session_id}",
        )
        self._reader_task = asyncio.create_task(
            self._read_loop(),
            name=f"miniredis:tcp-reader:{self.session_id}",
        )
        self._reader_task.add_done_callback(self._reader_done)

    def _reader_done(self, task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            if not self._reader_quiescing:
                self.request_close()
        except BaseException as exc:
            self.endpoint.offer_best_effort(
                ServerClosed(f"session reader failed: {exc}")
            )
            self.endpoint.outbox.begin_close("session reader failed")
            self.request_close()

    def _request_transport_close(self, reason: str) -> None:
        self._writer_allowed.set()
        if reason != "runtime closed":
            self.writer.close()

    def _submit_available(self) -> None:
        while (
            not self._closed
            and not self._reader_quiescing
            and self._frames
            and self.endpoint.pending_request_count < self._max_buffered_frames
        ):
            request = self._frames[0]
            submitted = self.runtime.submit_request(self.session_id, request)
            if isinstance(submitted, Failure):
                if submitted.code == "BUSY":
                    self._ensure_admission_waiter()
                    return
                self._frames.popleft()
                token = self.runtime.executor.new_request_token()
                self.endpoint.offer(ReplyMessage(token, submitted))
                continue

            self._frames.popleft()
            task = asyncio.create_task(
                self.runtime.wait_for_session_submission(submitted),
                name=f"miniredis:tcp-command:{self.session_id}",
            )
            self._pending_commands.add(task)
            task.add_done_callback(self._command_done)

    def _ensure_admission_waiter(self) -> None:
        if self._admission_task is not None and not self._admission_task.done():
            return
        self._admission_task = asyncio.create_task(
            self._wait_for_admission(),
            name=f"miniredis:tcp-admission:{self.session_id}",
        )
        self._admission_task.add_done_callback(self._admission_done)

    async def _wait_for_admission(self) -> None:
        if await self.runtime.wait_for_submission_capacity():
            self._submit_available()

    def _admission_done(self, task: asyncio.Task[None]) -> None:
        if self._admission_task is task:
            self._admission_task = None
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except BaseException as exc:
            self.endpoint.offer_best_effort(
                ServerClosed(f"session admission failed: {exc}")
            )
            self.endpoint.outbox.begin_close("session admission failed")
            self.request_close()

    def _command_done(self, task: asyncio.Task[None]) -> None:
        self._pending_commands.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            self.runtime.request_session_close(self.session_id)
        except BaseException as exc:
            self.endpoint.offer_best_effort(
                ServerClosed(f"session command failed: {exc}")
            )
            self.endpoint.outbox.begin_close("session command failed")
        self._submit_available()

    async def _read_loop(self) -> None:
        protocol_error: RespProtocolError | None = None
        saw_eof = False
        try:
            while not self._reader_quiescing:
                data = await self.reader.read(65536)
                if not data:
                    saw_eof = True
                    try:
                        self.decoder.finish()
                    except RespProtocolError as exc:
                        protocol_error = exc
                    break
                try:
                    frames = self.decoder.feed(data)
                    if (
                        len(self._frames)
                        + self.endpoint.pending_request_count
                        + len(frames)
                        > self._max_buffered_frames
                    ):
                        raise RespProtocolError("too many buffered command frames")
                    for frame in frames:
                        self._frames.append(frame_to_request(frame))
                except RespProtocolError as exc:
                    protocol_error = exc
                    break
                self._submit_available()
        except asyncio.CancelledError:
            if not self._reader_quiescing:
                raise
        finally:
            self._reader_quiesced.set()

        if self._reader_quiescing:
            return
        if protocol_error is not None:
            self.endpoint.offer_best_effort(
                ServerClosed(f"protocol error: {protocol_error}")
            )
            self.endpoint.outbox.begin_close("protocol error")
            await self._drain_protocol_error_best_effort()
        if saw_eof or protocol_error is not None:
            await self.runtime.close_session(self.session_id)
            await self._settle_commands()
            await self._finish_transport()

    async def _write_loop(self) -> None:
        try:
            while True:
                await self._writer_allowed.wait()
                item = await self.endpoint.receive()
                self.writer.write(encode_outbound(item))
                await self.writer.drain()
        except OutboxClosed:
            self.runtime.request_session_close(self.session_id)
        except (ConnectionError, BrokenPipeError):
            self.runtime.request_session_close(self.session_id)

    async def _drain_protocol_error_best_effort(self) -> None:
        task = self._writer_task
        if task is None or task is asyncio.current_task() or task.done():
            return
        try:
            async with asyncio.timeout(
                self.runtime.config.outbox_drain_grace_ms / 1000
            ):
                await asyncio.shield(task)
        except TimeoutError:
            return

    def request_reader_quiesce(self) -> None:
        if self._reader_quiescing:
            return
        self._reader_quiescing = True
        if self._reader_task is not None and not self._reader_task.done():
            self._reader_task.cancel()

    async def wait_reader_quiesced(self) -> None:
        await self._reader_quiesced.wait()

    async def close(self) -> None:
        self.request_close()
        assert self._close_task is not None
        await asyncio.shield(self._close_task)

    def request_close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_once(),
                name=f"miniredis:tcp-close:{self.session_id}",
            )
            self._close_task.add_done_callback(self._close_done)

    @staticmethod
    def _close_done(task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except BaseException:
            return

    async def _close_once(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.request_reader_quiesce()
        await self.wait_reader_quiesced()
        await self._settle_reader()
        await self.runtime.close_session(self.session_id)
        await self._settle_commands()
        self.endpoint.outbox.abort("session closed")
        await self._finish_transport()

    async def finish_runtime_close(self) -> None:
        self._closed = True
        self._writer_allowed.set()
        self.endpoint.outbox.abort("runtime closed")
        await self._finish_transport()
        await self._settle_reader()
        await self._settle_commands()

    async def _settle_reader(self) -> None:
        if (
            self._reader_task is not None
            and self._reader_task is not asyncio.current_task()
        ):
            await asyncio.gather(
                self._reader_task,
                return_exceptions=True,
            )

    async def _settle_commands(self) -> None:
        if self._admission_task is not None:
            self._admission_task.cancel()
        tasks = tuple(self._pending_commands)
        if self._admission_task is not None:
            tasks += (self._admission_task,)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _finish_transport(self) -> None:
        if self._transport_finishing:
            await self._transport_finished.wait()
            return
        self._transport_finishing = True
        self._writer_allowed.set()
        try:
            self.writer.close()
            if (
                self._writer_task is not None
                and self._writer_task is not asyncio.current_task()
            ):
                await asyncio.gather(
                    self._writer_task,
                    return_exceptions=True,
                )
            try:
                await self.writer.wait_closed()
            except ConnectionError:
                pass
            self._on_closed(self)
        finally:
            self._transport_finished.set()

    def debug_pause_writer(self) -> None:
        self._writer_allowed.clear()

    def debug_resume_writer(self) -> None:
        self._writer_allowed.set()

    @property
    def owned_task_count(self) -> int:
        return sum(not task.done() for task in self._pending_commands) + sum(
            task is not None and not task.done()
            for task in (
                self._reader_task,
                self._writer_task,
                self._admission_task,
                self._close_task,
            )
        )


class TcpServer:
    def __init__(
        self,
        runtime: MiniRedis,
        host: str,
        port: int,
        outbox_limit: int,
    ) -> None:
        self.runtime = runtime
        self.host = host
        self.port = port
        self.outbox_limit = outbox_limit
        self._server: asyncio.Server | None = None
        self._closing_server: asyncio.Server | None = None
        self._sessions: set[TcpSession] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._close_task: asyncio.Task[None] | None = None
        self._closed = False
        self._new_writers_paused = False

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await asyncio.start_server(self._accept, self.host, self.port)

    @property
    def address(self) -> tuple[str, int]:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("TCP server is not started")
        host, port, *_ = self._server.sockets[0].getsockname()
        return str(host), int(port)

    @property
    def closed(self) -> bool:
        return self._closed

    async def _accept(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        owner = asyncio.current_task()
        if owner is not None:
            self._tasks.add(owner)
        if self._closed:
            writer.close()
            await writer.wait_closed()
            if owner is not None:
                self._tasks.discard(owner)
            return
        session = TcpSession(
            self.runtime,
            reader,
            writer,
            self.outbox_limit,
            self.runtime.config.max_session_frames,
            self._session_finished,
            self._new_writers_paused,
        )
        self._sessions.add(session)
        try:
            await session.start()
        except BaseException:
            self._sessions.discard(session)
            writer.close()
            await writer.wait_closed()
            raise
        finally:
            if owner is not None:
                self._tasks.discard(owner)

    def _session_finished(self, session: TcpSession) -> None:
        self._sessions.discard(session)

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_once(),
                name="miniredis:tcp-server-close",
            )
        await asyncio.shield(self._close_task)

    async def _close_once(self) -> None:
        await self.quiesce()
        await asyncio.gather(
            *(session.close() for session in tuple(self._sessions)),
            return_exceptions=False,
        )
        await self._wait_listener_closed()
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
        self._sessions.clear()
        self._tasks.clear()
        self._closed = True
        self.runtime.unregister_tcp_server(self)

    async def quiesce(self) -> None:
        if self._server is not None:
            self._server.close()
            self._closing_server = self._server
            self._server = None
        sessions = tuple(self._sessions)
        for session in sessions:
            session.request_reader_quiesce()
        await asyncio.gather(
            *(session.wait_reader_quiesced() for session in sessions),
            return_exceptions=True,
        )

    async def finish_runtime_close(self) -> None:
        await asyncio.gather(
            *(session.finish_runtime_close() for session in tuple(self._sessions)),
            return_exceptions=False,
        )
        await self._wait_listener_closed()
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
        self._sessions.clear()
        self._tasks.clear()
        self._closed = True
        self.runtime.unregister_tcp_server(self)

    async def _wait_listener_closed(self) -> None:
        server, self._closing_server = self._closing_server, None
        if server is not None:
            await server.wait_closed()

    def debug_sessions(self) -> tuple[TcpSession, ...]:
        return tuple(
            sorted(
                self._sessions,
                key=lambda session: session.session_id,
            )
        )

    def debug_pause_new_writers(self) -> None:
        self._new_writers_paused = True

    def debug_resume_new_writers(self) -> None:
        self._new_writers_paused = False

    @property
    def session_count(self) -> int:
        return len(self._sessions)

    @property
    def owned_task_count(self) -> int:
        return (
            sum(not task.done() for task in self._tasks)
            + sum(session.owned_task_count for session in self._sessions)
            + int(self._close_task is not None and not self._close_task.done())
        )
