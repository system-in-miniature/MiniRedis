from __future__ import annotations

from pathlib import Path

from miniredis.core.commit import SnapshotImage
from miniredis.core.database import Database
from miniredis.persistence.aof import AofCorruption, AofLog, load_aof
from miniredis.persistence.codec import CodecError, decode_snapshot_file


class RecoveryError(RuntimeError):
    pass


def _load_snapshot(path: Path | None) -> SnapshotImage:
    if path is None:
        return SnapshotImage(0, ())
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return SnapshotImage(0, ())
    try:
        return decode_snapshot_file(data)
    except CodecError as exc:
        raise RecoveryError(str(exc)) from exc


def recover_database(
    *,
    snapshot_path: Path | None,
    aof_path: Path | None,
    now_ms: int,
    repair_truncated_tail: bool,
) -> Database:
    image = _load_snapshot(snapshot_path)
    try:
        log = (
            load_aof(
                aof_path,
                repair_truncated_tail=repair_truncated_tail,
            )
            if aof_path is not None
            else AofLog(None, ())
        )
    except AofCorruption as exc:
        raise RecoveryError(str(exc)) from exc

    base = log.state_base
    image = (
        image
        if base is None or image.checkpoint_seq > base.checkpoint_seq
        else base
    )
    batches = log.batches
    post_checkpoint = tuple(
        batch
        for batch in batches
        if batch.seq > image.checkpoint_seq
    )
    aof_end = (
        batches[-1].seq
        if batches
        else base.checkpoint_seq
        if base is not None
        else None
    )
    if aof_end is not None and aof_end < image.checkpoint_seq:
        raise RecoveryError(
            f"AOF ends at seq {aof_end} before "
            f"snapshot checkpoint {image.checkpoint_seq}"
        )
    if image.checkpoint_seq == 0 and batches and batches[0].seq != 1:
        raise RecoveryError(
            f"expected replay seq 1, got {batches[0].seq}"
        )
    if (
        post_checkpoint
        and post_checkpoint[0].seq != image.checkpoint_seq + 1
    ):
        raise RecoveryError(
            "expected replay seq "
            f"{image.checkpoint_seq + 1}, got {post_checkpoint[0].seq}"
        )

    staged = Database()
    staged.install_snapshot(image, now_ms=now_ms)
    for batch in post_checkpoint:
        expected = staged.commit_seq + 1
        if batch.seq != expected:
            raise RecoveryError(
                f"expected replay seq {expected}, got {batch.seq}"
            )
        try:
            staged.apply_batch(batch, track_access=False)
        except (TypeError, ValueError) as exc:
            raise RecoveryError(
                f"invalid commit {batch.seq}: {exc}"
            ) from exc
    staged.discard_expired_for_recovery(now_ms)
    return staged
