from miniredis.core.commit import DeleteKey, DeleteReason
from miniredis.core.database import Entry


def is_expired(entry: Entry, now_ms: int) -> bool:
    return entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms


def expiry_delete(key: bytes) -> DeleteKey:
    return DeleteKey(key, DeleteReason.EXPIRED)
