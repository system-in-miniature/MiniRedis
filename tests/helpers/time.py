class FakeClock:
    def __init__(self, now_ms: int = 0) -> None:
        self.value = now_ms

    def now_ms(self) -> int:
        return self.value

    def advance(self, milliseconds: int) -> None:
        self.value += milliseconds
