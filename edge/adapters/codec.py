"""Serialización de lecturas a JSON. Vive en los adaptadores: el núcleo no conoce formatos."""
from __future__ import annotations

import json
from dataclasses import asdict

from ..core.model import Reading


def reading_to_json(r: Reading) -> str:
    return json.dumps(asdict(r), separators=(",", ":"), sort_keys=True)


def reading_from_json(text: str) -> Reading:
    return Reading(**json.loads(text))
