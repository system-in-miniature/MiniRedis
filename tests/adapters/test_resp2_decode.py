import pytest

from miniredis.adapters.resp2 import (
    RespArray,
    RespBulk,
    RespDecoder,
    RespLimits,
    RespProtocolError,
)


def test_fragmented_command_emits_only_when_complete():
    decoder = RespDecoder()
    assert decoder.feed(b"*2\r\n$3\r\nGE") == ()
    assert decoder.feed(b"T\r\n$1\r\nk\r\n") == (
        RespArray((RespBulk(b"GET"), RespBulk(b"k"))),
    )


def test_coalesced_commands_remain_separate():
    decoder = RespDecoder()
    assert decoder.feed(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n") == (
        RespArray((RespBulk(b"PING"),)),
        RespArray((RespBulk(b"PING"),)),
    )


def test_binary_bulk_is_not_utf8_decoded():
    decoder = RespDecoder()
    assert decoder.feed(b"$3\r\n\xff\x00x\r\n") == (RespBulk(b"\xff\x00x"),)


@pytest.mark.parametrize(
    "wire",
    [
        b"+bad\n",
        b"$x\r\n",
        b"$3\r\nab\r\n",
        b"*2\r\n$1\r\na\r\n",
    ],
)
def test_invalid_or_incomplete_at_eof_is_rejected(wire):
    decoder = RespDecoder()
    with pytest.raises(RespProtocolError):
        decoder.feed(wire)
        decoder.finish()


def test_bulk_and_buffer_limits_are_enforced():
    decoder = RespDecoder(RespLimits(max_buffer=16, max_bulk=2))
    with pytest.raises(RespProtocolError):
        decoder.feed(b"$3\r\nabc\r\n")
