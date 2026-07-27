from __future__ import annotations

import os
from pathlib import Path

from miniredis.core.commit import CommitBatch
from miniredis.persistence.codec import CodecError, scan_aof_bytes


class AofCorruption(RuntimeError):
    pass


def load_aof(
    path: Path,
    *,
    repair_truncated_tail: bool,
) -> tuple[CommitBatch, ...]:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return ()
    if data == b"":
        return ()
    try:
        scan = scan_aof_bytes(data)
    except CodecError as exc:
        raise AofCorruption(str(exc)) from exc
    if not scan.has_truncated_tail:
        return scan.batches
    if not repair_truncated_tail:
        raise AofCorruption("incomplete final AOF record")

    fd = os.open(path, os.O_WRONLY)
    try:
        os.ftruncate(fd, scan.valid_offset)
        os.fsync(fd)
    finally:
        os.close(fd)
    return scan.batches
