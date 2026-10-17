import time


class MonotonicClock:
    def now_ns(self) -> int:
        return time.perf_counter_ns()


class SystemWallClock:
    """Hora de pared en milisegundos (para vigencia de comandos)."""

    def now_ms(self) -> int:
        return int(time.time() * 1000)
