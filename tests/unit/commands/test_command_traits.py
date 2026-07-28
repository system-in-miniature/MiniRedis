from typing import get_args

from miniredis.commands import model


def test_every_frozen_command_type_has_exactly_one_dataset_trait():
    command_types = frozenset(get_args(model.Command))
    assert (
        model._DATASET_MUTATING_TYPES
        | model._NON_DATASET_MUTATING_TYPES
    ) == command_types
    assert (
        model._DATASET_MUTATING_TYPES
        & model._NON_DATASET_MUTATING_TYPES
    ) == frozenset()


def test_blpop_is_mutating_and_pubsub_is_explicitly_non_dataset():
    assert (
        model.is_dataset_mutating(model.BlockingPop((b"q",), 0, left=True)) is True
    )
    assert (
        model.is_dataset_mutating(model.Subscribe((b"c",))) is False
    )
    assert (
        model.is_dataset_mutating(model.Unsubscribe((b"c",))) is False
    )
    assert (
        model.is_dataset_mutating(model.Publish(b"c", b"p")) is False
    )
