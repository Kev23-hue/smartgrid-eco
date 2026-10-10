import time


class SystemWallClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000
