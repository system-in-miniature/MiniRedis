from miniredis.commands.request import CommandRequest
from miniredis.config import MiniRedisConfig
import pytest


def test_command_request_preserves_binary_name_and_arguments() -> None:
    request = CommandRequest(b"echo", (b"\xff\x00",))

    assert request.name == b"echo"
    assert request.args == (b"\xff\x00",)


def test_lfu_configuration_is_explicit_and_validated():
    assert (
        MiniRedisConfig(eviction_policy="allkeys-lfu").eviction_policy
        == "allkeys-lfu"
    )
    with pytest.raises(ValueError, match="lfu_decay_interval_ms"):
        MiniRedisConfig(lfu_decay_interval_ms=0)


def test_replication_backlog_configuration_is_positive():
    assert MiniRedisConfig().replication_backlog_batches == 1024
    with pytest.raises(ValueError, match="replication_backlog_batches"):
        MiniRedisConfig(replication_backlog_batches=0)
