"""Capture and atomically install custom stable keyspace checkpoints."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeAlias

from miniredis.core.commit import SnapshotImage
from miniredis.persistence.codec import encode_snapshot_file


@dataclass(frozen=True, slots=True)
class SnapshotSaved:
    path: Path
    checkpoint_seq: int


@dataclass(frozen=True, slots=True)
class SnapshotBusy:
    pass


@dataclass(frozen=True, slots=True)
class SnapshotFailed:
    message: str


SnapshotOutcome: TypeAlias = SnapshotSaved | SnapshotBusy | SnapshotFailed


class SnapshotFileOps(Protocol):
    def write_atomic(
        self,
        destination: Path,
        temporary: Path,
        data: bytes,
    ) -> None:
        raise NotImplementedError


class PosixSnapshotFileOps:
    @staticmethod
    def _write_all(fd: int, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("snapshot write made no progress")
            view = view[written:]

    def write_atomic(
        self,
        destination: Path,
        temporary: Path,
        data: bytes,
    ) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd: int | None = None
        try:
            fd = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            self._write_all(fd, data)
            os.fsync(fd)
            os.close(fd)
            fd = None
            os.replace(temporary, destination)
            directory_fd = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except BaseException:
            if fd is not None:
                os.close(fd)
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise


class SnapshotManager:
    def __init__(
        self,
        path: Path,
        capture: Callable[[], Awaitable[SnapshotImage]],
        *,
        ops: SnapshotFileOps | None = None,
    ) -> None:
        self._path = path
        self._capture = capture
        self._ops = ops or PosixSnapshotFileOps()
        self._generation = 0
        self._accepting = True
        self._active_job: asyncio.Task[SnapshotOutcome] | None = None

    @property
    def active_job(self) -> asyncio.Task[SnapshotOutcome] | None:
        return self._active_job

    async def save(self) -> SnapshotOutcome:
        if not self._accepting:
            return SnapshotFailed("snapshot manager is closing")
        if self._active_job is not None:
            return SnapshotBusy()
        self._generation += 1
        generation = self._generation
        task = asyncio.create_task(
            self._run_save(generation),
            name=f"miniredis-snapshot-{generation}",
        )
        self._active_job = task
        task.add_done_callback(self._job_done)
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._active_job is task:
                self._active_job = None

    def _job_done(self, task: asyncio.Task[SnapshotOutcome]) -> None:
        if self._active_job is task:
            self._active_job = None

    async def _run_save(self, generation: int) -> SnapshotOutcome:
        temporary = self._path.with_name(
            f".{self._path.name}.tmp.{os.getpid()}.{generation}"
        )
        try:
            image = await self._capture()
            encoded = encode_snapshot_file(image)
            await asyncio.to_thread(
                self._ops.write_atomic,
                self._path,
                temporary,
                encoded,
            )
            return SnapshotSaved(self._path, image.checkpoint_seq)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return SnapshotFailed(str(exc))

    async def close(self) -> None:
        self._accepting = False
        task = self._active_job
        if task is not None:
            await asyncio.shield(task)
            if self._active_job is task:
                self._active_job = None
