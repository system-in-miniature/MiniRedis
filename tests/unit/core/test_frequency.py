import pytest

from miniredis.core.frequency import project_frequency


@pytest.mark.parametrize(
    ("frequency", "last_ms", "now_ms", "interval_ms", "expected"),
    [
        (8, 0, 999, 1000, (8, 0)),
        (8, 0, 1000, 1000, (4, 1000)),
        (9, 0, 3000, 1000, (1, 3000)),
        (1, 0, 5000, 1000, (0, 5000)),
        (8, 2000, 1000, 1000, (8, 2000)),
    ],
)
def test_project_frequency_decay(
    frequency,
    last_ms,
    now_ms,
    interval_ms,
    expected,
):
    assert project_frequency(frequency, last_ms, now_ms, interval_ms) == expected


@pytest.mark.parametrize(
    ("frequency", "interval"),
    [(-1, 1000), (1, 0), (1, -1)],
)
def test_project_frequency_rejects_invalid_inputs(frequency, interval):
    with pytest.raises(ValueError):
        project_frequency(frequency, 0, 0, interval)
