import pytest

from miniredis.core.outbound import (
    CloseAwareOutbox,
    OutboxClosed,
    PubSubMessage,
)


@pytest.mark.asyncio
async def test_graceful_close_drains_without_a_sentinel():
    outbox = CloseAwareOutbox(capacity=2)
    first = PubSubMessage(b"c", b"1")
    second = PubSubMessage(b"c", b"2")
    assert outbox.offer(first)
    assert outbox.offer(second)
    outbox.begin_close("runtime closed")
    assert await outbox.receive() == first
    assert await outbox.receive() == second
    with pytest.raises(OutboxClosed, match="runtime closed"):
        await outbox.receive()
    assert outbox.pending_count == 0


@pytest.mark.asyncio
async def test_full_outbox_discards_pending_output_and_closes_once():
    overflows = 0

    def overflow() -> None:
        nonlocal overflows
        overflows += 1

    outbox = CloseAwareOutbox(capacity=1, on_overflow=overflow)
    assert outbox.offer(PubSubMessage(b"c", b"1"))
    assert not outbox.offer(PubSubMessage(b"c", b"2"))
    assert outbox.closed
    assert outbox.pending_count == 0
    assert overflows == 1
    assert not outbox.offer(PubSubMessage(b"c", b"3"))
    assert overflows == 1


def test_best_effort_notice_never_displaces_accepted_output():
    outbox = CloseAwareOutbox(capacity=1)
    assert outbox.offer(PubSubMessage(b"c", b"accepted"))
    assert not outbox.offer_best_effort(PubSubMessage(b"c", b"notice"))
    assert not outbox.closed
    assert outbox.pending_count == 1
