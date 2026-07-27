from miniredis.commands.request import CommandRequest


def test_command_request_preserves_binary_name_and_arguments() -> None:
    request = CommandRequest(b"echo", (b"\xff\x00",))

    assert request.name == b"echo"
    assert request.args == (b"\xff\x00",)
