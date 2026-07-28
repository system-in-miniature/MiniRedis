from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol, TypeAlias

from miniredis.core.commit import CommitBatch, SnapshotImage
from miniredis.persistence.codec import (
    AOF_HEADER,
    CodecError,
    encode_aof_record,
    scan_aof_bytes,
)


class AofCorruption(RuntimeError):
    pass


class AofPolicy(StrEnum):
    ALWAYS = "always"
    EVERYSEC = "everysec"
    NO = "no"


@dataclass(frozen=True, slots=True)
class AofAppendOk:
    seq: int


@dataclass(frozen=True, slots=True)
class AofAppendFailed:
    message: str


AofAppendOutcome: TypeAlias = AofAppendOk | AofAppendFailed


@dataclass(frozen=True, slots=True)
class AofLog:
    state_base: SnapshotImage | None
    batches: tuple[CommitBatch, ...]


class AofFileOps(Protocol):
    def open_append(self, path: Path) -> int:
        raise NotImplementedError

    def size(self, fd: int) -> int:
        raise NotImplementedError

    def read_header(self, fd: int) -> bytes:
        raise NotImplementedError

    def write_all(self, fd: int, data: bytes) -> None:
        raise NotImplementedError

    def fsync(self, fd: int) -> None:
        raise NotImplementedError

    def close(self, fd: int) -> None:
        raise NotImplementedError


class PosixAofFileOps:
    def open_append(self, path: Path) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)
        return os.open(
            path,
            os.O_CREAT | os.O_RDWR | os.O_APPEND,
            0o600,
        )

    def size(self, fd: int) -> int:
        return os.fstat(fd).st_size

    def read_header(self, fd: int) -> bytes:
        return os.pread(fd, len(AOF_HEADER), 0)

    def write_all(self, fd: int, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("AOF write made no progress")
            view = view[written:]

    def fsync(self, fd: int) -> None:
        os.fsync(fd)

    def close(self, fd: int) -> None:
        os.close(fd)


def load_aof(
    path: Path,
    *,
    repair_truncated_tail: bool,
) -> AofLog:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return AofLog(None, ())
    if data == b"":
        return AofLog(None, ())
    try:
        scan = scan_aof_bytes(data)
    except CodecError as exc:
        raise AofCorruption(str(exc)) from exc
    if not scan.has_truncated_tail:
        return AofLog(scan.state_base, scan.batches)
    if not repair_truncated_tail:
        raise AofCorruption("incomplete final AOF record")

    fd = os.open(path, os.O_WRONLY)
    try:
        os.ftruncate(fd, scan.valid_offset)
        os.fsync(fd)
    finally:
        os.close(fd)
    return AofLog(scan.state_base, scan.batches)


@dataclass(slots=True)
class _AppendWork:
    record: bytes
    seq: int
    barrier: asyncio.Future[AofAppendOutcome]


_STOP = object()


class AofWriter:
    def __init__(
        self,
        path: Path,
        policy: AofPolicy,
        *,
        fsync_interval_seconds: float = 1.0,
        ops: AofFileOps | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        on_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        if fsync_interval_seconds <= 0:
            raise ValueError("fsync interval must be positive")
        self._path = path
        self._policy = policy
        self._interval = fsync_interval_seconds
        self._ops = ops or PosixAofFileOps()
        self._sleep = sleep
        self._on_failure = on_failure or (lambda _error: None)
        self._queue: asyncio.Queue[_AppendWork | object] = asyncio.Queue()
        self._fd: int | None = None
        self._worker: asyncio.Task[None] | None = None
        self._sync_task: asyncio.Task[None] | None = None
        self._current_work: _AppendWork | None = None
        self._dirty = False
        self._sync_inflight = False
        self._accepting = False
        self._failure: BaseException | None = None
        self._failure_reported = False

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    @property
    def owned_task_count(self) -> int:
        return sum(
            task is not None and not task.done()
            for task in (self._worker, self._sync_task)
        )

    async def start(self) -> None:
        if self._worker is not None:
            return
        fd = await asyncio.to_thread(self._ops.open_append, self._path)
        try:
            size = await asyncio.to_thread(self._ops.size, fd)
            if size == 0:
                await asyncio.to_thread(
                    self._ops.write_all,
                    fd,
                    AOF_HEADER,
                )
                await asyncio.to_thread(self._ops.fsync, fd)
            else:
                header = await asyncio.to_thread(
                    self._ops.read_header,
                    fd,
                )
                if header != AOF_HEADER:
                    raise AofCorruption("invalid AOF header")
        except BaseException:
            await asyncio.to_thread(self._ops.close, fd)
            raise
        self._fd = fd
        self._accepting = True
        self._worker = asyncio.create_task(
            self._run_writer(),
            name="miniredis-aof-writer",
        )
        self._worker.add_done_callback(self._writer_done)
        if self._policy is AofPolicy.EVERYSEC:
            self._sync_task = asyncio.create_task(
                self._run_everysec(),
                name="miniredis-aof-fsync",
            )

    async def append(self, batch: CommitBatch) -> AofAppendOutcome:
        if self._failure is not None:
            return AofAppendFailed(str(self._failure))
        if not self._accepting or self._worker is None:
            return AofAppendFailed("AOF writer is not accepting")
        barrier = asyncio.get_running_loop().create_future()
        self._queue.put_nowait(
            _AppendWork(
                record=encode_aof_record(batch),
                seq=batch.seq,
                barrier=barrier,
            )
        )
        return await asyncio.shield(barrier)

    async def _run_writer(self) -> None:
        while True:
            item = await self._queue.get()
            if item is _STOP:
                return
            assert isinstance(item, _AppendWork)
            self._current_work = item
            if self._failure is not None:
                self._settle(
                    item.barrier,
                    AofAppendFailed(str(self._failure)),
                )
                self._current_work = None
                continue
            try:
                assert self._fd is not None
                await asyncio.to_thread(
                    self._ops.write_all,
                    self._fd,
                    item.record,
                )
                if self._failure is not None:
                    self._settle(
                        item.barrier,
                        AofAppendFailed(str(self._failure)),
                    )
                    self._fail_queued(self._failure)
                    self._current_work = None
                    return
                if self._policy is AofPolicy.ALWAYS:
                    await asyncio.to_thread(self._ops.fsync, self._fd)
                elif self._policy is AofPolicy.EVERYSEC:
                    self._dirty = True
            except BaseException as exc:
                self._settle(
                    item.barrier,
                    AofAppendFailed(str(exc)),
                )
                self._record_failure(exc)
                self._fail_queued(exc)
                self._current_work = None
                return
            self._settle(item.barrier, AofAppendOk(item.seq))
            self._current_work = None

    def _writer_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            error: BaseException | None = RuntimeError("AOF writer task was cancelled")
        else:
            error = task.exception()
        if error is None:
            return
        current = self._current_work
        self._current_work = None
        if current is not None:
            self._settle(
                current.barrier,
                AofAppendFailed(str(error)),
            )
        self._record_failure(error)
        self._fail_queued(error)

    async def _run_everysec(self) -> None:
        try:
            while self._accepting and self._failure is None:
                await self._sleep(self._interval)
                await self._sync_dirty()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            current = self._current_work
            if current is not None:
                self._settle(
                    current.barrier,
                    AofAppendFailed(str(exc)),
                )
            self._record_failure(exc)
            self._fail_queued(exc)

    async def _sync_dirty(self) -> None:
        if not self._dirty or self._failure is not None:
            return
        self._dirty = False
        try:
            assert self._fd is not None
            self._sync_inflight = True
            await asyncio.to_thread(self._ops.fsync, self._fd)
        except BaseException:
            self._dirty = True
            raise
        finally:
            self._sync_inflight = False

    @staticmethod
    def _settle(
        barrier: asyncio.Future[AofAppendOutcome],
        outcome: AofAppendOutcome,
    ) -> None:
        if not barrier.done():
            barrier.set_result(outcome)

    def _record_failure(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
        self._accepting = False
        if not self._failure_reported:
            self._failure_reported = True
            self._on_failure(self._failure)

    def _fail_queued(self, error: BaseException) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if isinstance(item, _AppendWork):
                self._settle(
                    item.barrier,
                    AofAppendFailed(str(error)),
                )

    async def close(self) -> None:
        if self._fd is None:
            return
        self._accepting = False
        if self._worker is not None and not self._worker.done():
            self._queue.put_nowait(_STOP)
            await asyncio.shield(self._worker)
        if self._sync_task is not None:
            if self._sync_inflight:
                await asyncio.shield(self._sync_task)
            else:
                self._sync_task.cancel()
                try:
                    await self._sync_task
                except asyncio.CancelledError:
                    pass
        if self._policy is AofPolicy.EVERYSEC and self._dirty and self._failure is None:
            try:
                await self._sync_dirty()
            except BaseException as exc:
                self._record_failure(exc)
        fd, self._fd = self._fd, None
        await asyncio.to_thread(self._ops.close, fd)

    async def crash_close(self) -> None:
        if self._fd is None:
            return
        self._accepting = False
        if self._worker is not None and not self._worker.done():
            self._queue.put_nowait(_STOP)
            await asyncio.shield(self._worker)
        if self._sync_task is not None:
            if self._sync_inflight:
                await asyncio.shield(self._sync_task)
            else:
                self._sync_task.cancel()
                try:
                    await self._sync_task
                except asyncio.CancelledError:
                    pass
        fd, self._fd = self._fd, None
        await asyncio.to_thread(self._ops.close, fd)
