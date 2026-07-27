import pytest

from miniredis.adapters.resp2 import (
    RespArray,
    RespBulk,
    RespInteger,
    RespProtocolError,
    encode_outbound,
    frame_to_request,
)
from miniredis.commands.request import CommandRequest
from miniredis.core.outbound import (
    PubSubMessage,
    PubSubPong,
    ReplyMessage,
    RequestToken,
    ServerClosed,
    SubscriptionAck,
)
from miniredis.core.reply import Bytes, Failure, Items, Number, Ok


def test_command_array_maps_without_text_decoding():
    frame = RespArray((RespBulk(b"SET"), RespBulk(b"k"), RespBulk(b"\xff")))
    assert frame_to_request(frame) == CommandRequest(b"SET", (b"k", b"\xff"))


@pytest.mark.parametrize(
    "frame",
    [
        RespInteger(1),
        RespArray(None),
        RespArray(()),
        RespArray((RespBulk(None),)),
        RespArray((RespInteger(1),)),
    ],
)
def test_non_command_frames_are_protocol_errors(frame):
    with pytest.raises(RespProtocolError):
        frame_to_request(frame)


def test_domain_replies_encode_as_resp2():
    assert encode_outbound(Ok()) == b"+OK\r\n"
    assert encode_outbound(Bytes(None)) == b"$-1\r\n"
    assert encode_outbound(Bytes(b"x")) == b"$1\r\nx\r\n"
    assert encode_outbound(Number(2)) == b":2\r\n"
    assert encode_outbound(Items((Bytes(b"a"), Number(1)))) == (
        b"*2\r\n$1\r\na\r\n:1\r\n"
    )
    assert encode_outbound(Failure("WRONGTYPE", "bad")) == b"-WRONGTYPE bad\r\n"


def test_every_frozen_outbound_value_encodes_as_resp2():
    token = RequestToken(7)
    assert encode_outbound(ReplyMessage(token, Number(3))) == b":3\r\n"
    assert (
        encode_outbound(SubscriptionAck("subscribe", b"c", 1))
        == b"*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n"
    )
    assert (
        encode_outbound(SubscriptionAck("unsubscribe", None, 0))
        == b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
    )
    assert (
        encode_outbound(PubSubMessage(b"c", b"m"))
        == b"*3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$1\r\nm\r\n"
    )
    assert encode_outbound(PubSubPong(b"x")) == b"*2\r\n$4\r\npong\r\n$1\r\nx\r\n"
    assert (
        encode_outbound(ServerClosed("runtime closed")) == b"-CLOSED runtime closed\r\n"
    )
