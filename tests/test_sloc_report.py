from tools.count_sloc import physical_lines


def test_physical_lines_does_not_count_a_missing_tail_line(tmp_path):
    empty = tmp_path / "empty"
    terminated = tmp_path / "terminated"
    unterminated = tmp_path / "unterminated"
    two_lines = tmp_path / "two-lines"
    empty.write_bytes(b"")
    terminated.write_bytes(b"one\n")
    unterminated.write_bytes(b"one")
    two_lines.write_bytes(b"one\ntwo")

    assert physical_lines(empty) == 0
    assert physical_lines(terminated) == 1
    assert physical_lines(unterminated) == 1
    assert physical_lines(two_lines) == 2
