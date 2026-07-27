import pytest

from miniredis.adapters.resp2 import (
    RespArray,
    RespBulk,
    RespError,
    RespInteger,
    RespSimple,
    encode_frame,
)


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        (RespSimple(b"OK"), b"+OK\r\n"),
        (RespError(b"ERR bad"), b"-ERR bad\r\n"),
        (RespInteger(42), b":42\r\n"),
        (RespBulk(b"a\x00b"), b"$3\r\na\x00b\r\n"),
        (RespBulk(None), b"$-1\r\n"),
        (
            RespArray((RespBulk(b"GET"), RespBulk(b"k"))),
            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
        ),
        (RespArray(None), b"*-1\r\n"),
    ],
)
def test_encode_frame(frame, expected):
    assert encode_frame(frame) == expected
