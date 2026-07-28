def project_frequency(
    frequency: int,
    last_decay_ms: int,
    now_ms: int,
    interval_ms: int,
) -> tuple[int, int]:
    if frequency < 0:
        raise ValueError("frequency cannot be negative")
    if interval_ms <= 0:
        raise ValueError("LFU decay interval must be positive")
    if now_ms <= last_decay_ms:
        return frequency, last_decay_ms
    windows = (now_ms - last_decay_ms) // interval_ms
    if windows == 0:
        return frequency, last_decay_ms
    projected = 0 if windows >= frequency.bit_length() else frequency >> windows
    return projected, last_decay_ms + windows * interval_ms
