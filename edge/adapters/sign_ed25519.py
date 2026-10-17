"""Verificación y firma Ed25519 (librería 'cryptography'). Las llaves públicas viven en el borde;
la llave privada de cada operador nunca sale de su HSM/KMS (en el prototipo, un archivo local)."""
from __future__ import annotations

import json
import secrets
import time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from ..core.commands import Command, approval_body


class Ed25519Verifier:
    def __init__(self, public_keys_hex: dict[str, str]):
        self._keys = {k: Ed25519PublicKey.from_public_bytes(bytes.fromhex(v)) for k, v in public_keys_hex.items()}

    def verify(self, key_id: str, message: bytes, signature: bytes) -> bool:
        pk = self._keys.get(key_id)
        if pk is None:
            return False
        try:
            pk.verify(signature, message)
            return True
        except (InvalidSignature, ValueError, TypeError):
            return False


class Operator:
    """Lado del centro de control (simulado): sabe firmar. Solo se usa en pruebas y demos."""

    def __init__(self, key_id: str):
        self.key_id = key_id
        self._sk = Ed25519PrivateKey.generate()

    @property
    def public_hex(self) -> str:
        raw = self._sk.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        return raw.hex()

    def issue(self, command_id: str, action: str, params: dict[str, str] | None = None,
              now_ms: int | None = None, ttl_ms: int = 30_000, nonce: str | None = None) -> Command:
        now = int(time.time() * 1000) if now_ms is None else now_ms
        unsigned = Command(command_id, action, tuple(sorted((params or {}).items())), self.key_id,
                           now, now + ttl_ms, nonce or secrets.token_hex(16), b"")
        return Command(**{**unsigned.__dict__, "signature": self._sk.sign(unsigned.body())})

    def approve(self, cmd: Command) -> bytes:
        return self._sk.sign(approval_body(cmd))


def command_to_json(c: Command) -> str:
    d = dict(c.__dict__)
    d["signature"] = c.signature.hex()
    d["params"] = [list(p) for p in c.params]
    return json.dumps(d, sort_keys=True)


def command_from_json(text: str) -> Command:
    d = json.loads(text)
    d["signature"] = bytes.fromhex(d["signature"])
    d["params"] = tuple((k, v) for k, v in d["params"])
    return Command(**d)
