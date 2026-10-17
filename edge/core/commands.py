"""Pasarela de comandos del borde: autenticidad (ES-05) y autorización dual (ES-07). ADR-007.

Reglas, en este orden (el orden importa):
  1. Estructura válida.
  2. Firma del emisor válida. Si no, se rechaza SIN tocar el almacén de nonces:
     un atacante sin llave no puede "quemar" nonces ni llenar el disco.
  3. Vigencia: ni vencido, ni emitido en el futuro, ni con vigencia mayor al máximo.
  4. Nonce no visto antes (anti-repetición). Persistente: sobrevive a reinicios.
  5. Si el comando es crítico: queda PENDIENTE hasta recibir la aprobación firmada de OTRO operador.
     Ninguna acción se ejecuta con una sola autorización.
Todo resultado, aceptado o rechazado, queda en la auditoría encadenada.

El núcleo no sabe de criptografía concreta ni de formatos: usa puertos.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence

from .ports import AuditPort

ACCEPTED, PENDING, REJECTED = "ACEPTADO", "PENDIENTE", "RECHAZADO"


@dataclass(frozen=True)
class Command:
    command_id: str
    action: str
    params: tuple[tuple[str, str], ...]   # pares ordenados (clave, valor)
    issuer: str                           # id de la llave pública del operador emisor
    issued_at_ms: int
    expires_at_ms: int
    nonce: str
    signature: bytes

    def body(self) -> bytes:
        """Bytes exactos que se firman. Cualquier campo alterado invalida la firma."""
        ps = ";".join(f"{k}={v}" for k, v in self.params)
        txt = "|".join(("CMD1", self.command_id, self.action, ps, self.issuer,
                        str(self.issued_at_ms), str(self.expires_at_ms), self.nonce))
        return txt.encode("utf-8")


def approval_body(cmd: Command) -> bytes:
    """Lo que firma el segundo operador: el comando completo, incluida la firma del primero."""
    return b"APPROVE1|" + cmd.body() + b"|" + cmd.signature.hex().encode()


@dataclass(frozen=True)
class Result:
    status: str          # ACEPTADO | PENDIENTE | RECHAZADO
    reason: str
    command_id: str = ""


class SignatureVerifierPort(Protocol):
    def verify(self, key_id: str, message: bytes, signature: bytes) -> bool:
        """False si la llave no existe o la firma no coincide. Nunca lanza por datos malos."""


class NonceStorePort(Protocol):
    def remember_if_new(self, nonce: str, expires_at_ms: int) -> bool:
        """Atómico. True si era nuevo (y lo guarda); False si ya se había visto."""


class WallClockPort(Protocol):
    def now_ms(self) -> int: ...


class ExecutorPort(Protocol):
    def execute(self, action: str, params: Mapping[str, str]) -> None: ...


@dataclass(frozen=True)
class CommandPolicy:
    critical_actions: frozenset[str] = frozenset({"open_breaker", "island_mode", "set_export_zero"})
    allowed_actions: frozenset[str] = frozenset(
        {"set_battery_kw", "set_inverter_limit", "open_breaker", "island_mode", "set_export_zero"})
    max_ttl_ms: int = 60_000
    max_future_skew_ms: int = 5_000


@dataclass
class _Pending:
    cmd: Command
    approvals: list[str] = field(default_factory=list)


class CommandGateway:
    def __init__(self, verifier: SignatureVerifierPort, nonces: NonceStorePort, clock: WallClockPort,
                 executor: ExecutorPort, audit: AuditPort, policy: CommandPolicy = CommandPolicy()):
        self._v, self._n, self._clock = verifier, nonces, clock
        self._exec, self._audit, self._pol = executor, audit, policy
        self._pending: dict[str, _Pending] = {}
        self.executed = 0

    # -- utilidades ---------------------------------------------------------------------------
    def _reject(self, cmd: Command | None, reason: str, who: str = "") -> Result:
        cid = cmd.command_id if cmd else ""
        self._audit.record("command_rejected", {"command_id": cid, "reason": reason,
                                                 "issuer": cmd.issuer if cmd else "", "by": who})
        return Result(REJECTED, reason, cid)

    def _run(self, cmd: Command, authorizers: Sequence[str]) -> Result:
        self._exec.execute(cmd.action, dict(cmd.params))
        self.executed += 1
        self._audit.record("command_executed", {"command_id": cmd.command_id, "action": cmd.action,
                                                 "authorized_by": list(authorizers)})
        return Result(ACCEPTED, "ejecutado", cmd.command_id)

    # -- entrada principal --------------------------------------------------------------------
    def submit(self, cmd: Command) -> Result:
        if not isinstance(cmd, Command) or not cmd.command_id or not cmd.nonce or not cmd.issuer:
            return self._reject(cmd if isinstance(cmd, Command) else None, "estructura inválida")
        if not cmd.signature or not self._v.verify(cmd.issuer, cmd.body(), cmd.signature):
            return self._reject(cmd, "no autenticado: firma inválida o llave desconocida")
        if cmd.action not in self._pol.allowed_actions:
            return self._reject(cmd, f"acción no permitida: {cmd.action}")
        now = self._clock.now_ms()
        if now > cmd.expires_at_ms:
            return self._reject(cmd, "comando vencido")
        if cmd.issued_at_ms > now + self._pol.max_future_skew_ms:
            return self._reject(cmd, "emitido en el futuro")
        if cmd.expires_at_ms - cmd.issued_at_ms > self._pol.max_ttl_ms or cmd.expires_at_ms <= cmd.issued_at_ms:
            return self._reject(cmd, "vigencia fuera de política")
        if not self._n.remember_if_new(cmd.nonce, cmd.expires_at_ms):
            return self._reject(cmd, "repetición: nonce ya usado")
        if cmd.action in self._pol.critical_actions:
            self._pending[cmd.command_id] = _Pending(cmd)
            self._audit.record("command_pending", {"command_id": cmd.command_id, "action": cmd.action,
                                                    "issuer": cmd.issuer})
            return Result(PENDING, "crítico: falta la aprobación de un segundo operador", cmd.command_id)
        return self._run(cmd, [cmd.issuer])

    def approve(self, command_id: str, approver: str, signature: bytes) -> Result:
        p = self._pending.get(command_id)
        if p is None:
            return self._reject(None, f"no hay comando pendiente {command_id}", approver)
        cmd = p.cmd
        if not signature or not self._v.verify(approver, approval_body(cmd), signature):
            return self._reject(cmd, "aprobación no autenticada", approver)
        if approver == cmd.issuer:
            return self._reject(cmd, "el aprobador debe ser distinto del emisor", approver)
        if self._clock.now_ms() > cmd.expires_at_ms:
            del self._pending[command_id]
            return self._reject(cmd, "comando vencido antes de la aprobación", approver)
        del self._pending[command_id]
        return self._run(cmd, [cmd.issuer, approver])
