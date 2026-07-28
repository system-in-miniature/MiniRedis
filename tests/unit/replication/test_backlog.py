import pytest

from miniredis.replication.backlog import ReplicationBacklog
from tests.unit.persistence.test_framing import batch


def test_backlog_drops_oldest_batches_and_reports_bounds():
    backlog = ReplicationBacklog(capacity_batches=2)
    backlog.append(batch(1))
    backlog.append(batch(2))
    backlog.append(batch(3))

    assert backlog.oldest_seq == 2
    assert backlog.newest_seq == 3
    assert backlog.batch_count == 2
    assert backlog.missing_after(1, current_seq=3) == (
        batch(2),
        batch(3),
    )


def test_backlog_distinguishes_current_empty_range_from_gap():
    backlog = ReplicationBacklog(capacity_batches=2)
    backlog.append(batch(4))
    backlog.append(batch(5))

    assert backlog.missing_after(5, current_seq=5) == ()
    assert backlog.missing_after(2, current_seq=5) is None
    assert backlog.missing_after(6, current_seq=5) is None


def test_backlog_rejects_non_contiguous_append():
    backlog = ReplicationBacklog(capacity_batches=2)
    backlog.append(batch(1))

    with pytest.raises(
        ValueError,
        match="replication backlog must be contiguous",
    ):
        backlog.append(batch(3))


def test_clear_removes_bounds_and_coverage():
    backlog = ReplicationBacklog(capacity_batches=2)
    backlog.append(batch(1))
    backlog.clear()

    assert backlog.oldest_seq is None
    assert backlog.newest_seq is None
    assert backlog.batch_count == 0
    assert backlog.missing_after(0, current_seq=1) is None


@pytest.mark.parametrize("capacity", [0, -1])
def test_backlog_capacity_must_be_positive(capacity):
    with pytest.raises(
        ValueError,
        match="replication backlog capacity must be positive",
    ):
        ReplicationBacklog(capacity_batches=capacity)
