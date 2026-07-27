import pytest

from miniredis import CommandRequest
from miniredis.core.reply import Failure
from miniredis.persistence.aof import AofAppendFailed
from miniredis.persistence.codec import AOF_HEADER, encode_aof_record
from miniredis.persistence.recovery import recover_database
from tests.helpers.runtime import open_test_runtime


class WriteThenFailAppender:
    def __init__(self, path) -> None:
        self.path = path

    async def append(self, batch):
        self.path.write_bytes(AOF_HEADER + encode_aof_record(batch))
        return AofAppendFailed("uncertain write")


@pytest.mark.asyncio
async def test_disk_may_contain_a_write_that_never_applied_or_replied_ok(
    tmp_path,
):
    path = tmp_path / "appendonly.mraof"
    runtime = await open_test_runtime(aof_appender=WriteThenFailAppender(path))

    reply = await runtime.direct_client().execute(CommandRequest(b"SET", (b"k", b"v")))

    assert isinstance(reply, Failure)
    assert b"k" not in runtime.database.entries
    assert runtime.debug_commit_seq == 0
    await runtime.close()

    recovered = recover_database(
        snapshot_path=None,
        aof_path=path,
        now_ms=0,
        repair_truncated_tail=True,
    )
    assert recovered.entries[b"k"].value.data == b"v"
    assert recovered.commit_seq == 1
