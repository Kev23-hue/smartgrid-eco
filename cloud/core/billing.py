"""Informe de huecos para facturación (ES-06): solo intervalos cerrados, nunca se rellena en silencio."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .model import IntervalAggregate


@dataclass
class GapReport:
    complete_intervals: int = 0
    billable_kwh: float = 0.0
    gaps: list[IntervalAggregate] = field(default_factory=list)   # intervalos con menos de 180 lecturas

    @property
    def missing_readings(self) -> int:
        return sum(g.missing for g in self.gaps)


def gap_report(aggregates: Iterable[IntervalAggregate]) -> GapReport:
    rep = GapReport()
    for a in aggregates:
        if a.complete:
            rep.complete_intervals += 1
            rep.billable_kwh += a.sum_kwh
        else:
            rep.gaps.append(a)
    return rep
