import time


class MonotonicClock:
    def now_ns(self) -> int:
        return time.perf_counter_ns()
