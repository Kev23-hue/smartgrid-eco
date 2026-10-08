"""Repositorio de políticas local (archivo JSON; en el prototipo reemplaza a SQLite de políticas)."""
from __future__ import annotations

import json

from ..core.model import Policy


class FilePolicyRepository:
    def __init__(self, path: str):
        self._path = path

    def load(self) -> Policy:
        with open(self._path, encoding="utf-8") as f:
            return Policy(**json.load(f))


class StaticPolicyRepository:
    def __init__(self, policy: Policy):
        self._policy = policy

    def load(self) -> Policy:
        return self._policy
