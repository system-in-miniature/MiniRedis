from collections import defaultdict
from collections.abc import Callable


class PubSubRegistry:
    def __init__(self, on_debug_change: Callable[[], None]) -> None:
        self._channels: dict[bytes, dict[int, None]] = defaultdict(dict)
        self._sessions: dict[int, dict[bytes, None]] = defaultdict(dict)
        self._on_debug_change = on_debug_change

    def count(self, session_id: int) -> int:
        return len(self._sessions.get(session_id, ()))

    def subscribe(self, session_id: int, channel: bytes) -> int:
        self._channels[channel][session_id] = None
        self._sessions[session_id][channel] = None
        count = self.count(session_id)
        self._on_debug_change()
        return count

    def unsubscribe(self, session_id: int, channel: bytes) -> int:
        members = self._channels.get(channel)
        if members is not None:
            members.pop(session_id, None)
            if not members:
                del self._channels[channel]
        owned = self._sessions.get(session_id)
        if owned is not None:
            owned.pop(channel, None)
            if not owned:
                del self._sessions[session_id]
        count = self.count(session_id)
        self._on_debug_change()
        return count

    def unsubscribe_targets(
        self,
        session_id: int,
        requested: tuple[bytes, ...],
    ) -> tuple[bytes | None, ...]:
        if requested:
            return requested
        current = tuple(self._sessions.get(session_id, ()))
        return current if current else (None,)

    def subscribers(self, channel: bytes) -> tuple[int, ...]:
        return tuple(self._channels.get(channel, ()))

    def remove_session(self, session_id: int) -> None:
        for channel in tuple(self._sessions.get(session_id, ())):
            self.unsubscribe(session_id, channel)
        self._on_debug_change()

    def clear(self) -> None:
        self._channels.clear()
        self._sessions.clear()
        self._on_debug_change()

    @property
    def membership_count(self) -> int:
        return sum(len(channels) for channels in self._sessions.values())
