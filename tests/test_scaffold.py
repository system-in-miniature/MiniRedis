"""Fast checks that the intentionally empty source skeleton is importable."""

from importlib import import_module

import pytest


MODULES = (
    "miniredis",
    "miniredis.__main__",
    "miniredis.clock",
    "miniredis.config",
    "miniredis.errors",
    "miniredis.mutation",
    "miniredis.store",
    "miniredis.expiry",
    "miniredis.engine",
    "miniredis.pubsub",
    "miniredis.session",
    "miniredis.connection",
    "miniredis.server",
    "miniredis.protocol.frame",
    "miniredis.protocol.codec",
    "miniredis.commands.model",
    "miniredis.commands.parser",
    "miniredis.commands.handlers",
    "miniredis.persistence.aof",
    "miniredis.persistence.snapshot",
    "miniredis.replication.backlog",
    "miniredis.replication.primary",
    "miniredis.replication.replica",
)


@pytest.mark.parametrize("module_name", MODULES)
def test_source_skeleton_is_importable(module_name: str) -> None:
    assert import_module(module_name).__name__ == module_name

