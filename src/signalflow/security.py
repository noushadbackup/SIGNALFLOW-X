"""Operators, roles and the audit log for actions taken from the control tower or the CLI.

Roles are ordered: viewer < analyst < operator < admin. Each action names the lowest role allowed to perform it.
Passwords are stored as scrypt hashes; nothing here reads credentials from files in the repository. Every attempt to
act, allowed or denied, is written to ``ops.audit_log``.
"""

from __future__ import annotations

import base64
import functools
import hashlib
import hmac
import os
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

ROLES = ("viewer", "analyst", "operator", "admin")
PERMISSIONS = {
    "case.note": "analyst",
    "case.assign": "analyst",
    "case.resolve": "analyst",
    "case.reopen": "analyst",
    "chaos.run": "operator",
    "chaos.recover": "operator",
    "replay.run": "operator",
    "operators.manage": "admin",
}
MIN_PASSWORD_LENGTH = 10
_SCRYPT = {"n": 2**14, "r": 8, "p": 1}


class PermissionDenied(Exception):
    pass


@dataclass(frozen=True)
class Operator:
    username: str
    display_name: str
    role: str

    def can(self, permission: str) -> bool:
        return ROLES.index(self.role) >= ROLES.index(PERMISSIONS[permission])


def hash_password(password: str) -> str:
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"password must have at least {MIN_PASSWORD_LENGTH} characters")
    salt = os.urandom(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, dklen=32, **_SCRYPT)
    b64 = base64.b64encode
    return f"scrypt${_SCRYPT['n']}${_SCRYPT['r']}${_SCRYPT['p']}${b64(salt).decode()}${b64(digest).decode()}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, n, r, p, salt, digest = encoded.split("$")
    except ValueError:
        return False
    if scheme != "scrypt":
        return False
    candidate = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt), n=int(n), r=int(r), p=int(p),
                               dklen=32)
    return hmac.compare_digest(candidate, base64.b64decode(digest))


@functools.cache
def _dummy_hash() -> str:
    return hash_password(base64.b64encode(os.urandom(18)).decode())


def audit(conn: psycopg.Connection, actor: str, role: str | None, action: str, outcome: str,
          target_type: str | None = None, target_id: str | None = None,
          details: dict[str, Any] | None = None) -> None:
    conn.execute(
        """INSERT INTO ops.audit_log (actor, actor_role, action, target_type, target_id, outcome, details)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (actor, role, action, target_type, target_id, outcome, Jsonb(details or {})))


def add_operator(conn: psycopg.Connection, username: str, display_name: str, role: str, password: str,
                 actor: str = "cli") -> Operator:
    if role not in ROLES:
        raise ValueError(f"role must be one of {', '.join(ROLES)}")
    encoded = hash_password(password)
    with conn.transaction():
        conn.execute(
            """INSERT INTO ops.operators (username, display_name, role, password_hash) VALUES (%s, %s, %s, %s)
               ON CONFLICT (username) DO UPDATE SET display_name = EXCLUDED.display_name, role = EXCLUDED.role,
                   password_hash = EXCLUDED.password_hash, disabled = false""",
            (username, display_name, role, encoded))
        audit(conn, actor, None, "operators.upsert", "OK", "operator", username, {"role": role})
    return Operator(username, display_name, role)


def set_disabled(conn: psycopg.Connection, username: str, disabled: bool, actor: str = "cli") -> bool:
    with conn.transaction():
        found = conn.execute("UPDATE ops.operators SET disabled = %s WHERE username = %s RETURNING 1",
                             (disabled, username)).fetchone() is not None
        audit(conn, actor, None, "operators.disable" if disabled else "operators.enable",
              "OK" if found else "FAILED", "operator", username)
    return found


def authenticate(conn: psycopg.Connection, username: str, password: str) -> Operator | None:
    row = conn.execute("SELECT display_name, role, password_hash, disabled FROM ops.operators WHERE username = %s",
                       (username,)).fetchone()
    # Hash even for unknown users so response time does not reveal which usernames exist.
    matches = verify_password(password, row[2] if row else _dummy_hash())
    ok = row is not None and not row[3] and matches
    with conn.transaction():
        audit(conn, username, row[1] if row else None, "auth.login", "OK" if ok else "DENIED")
    return Operator(username, row[0], row[1]) if ok and row else None


def authorize(conn: psycopg.Connection, operator: Operator | None, permission: str, target_type: str,
              target_id: str) -> Operator:
    """Raise PermissionDenied (after auditing it, in its own transaction) unless the operator may act."""
    if operator is not None and operator.can(permission):
        return operator
    with conn.transaction():
        audit(conn, operator.username if operator else "anonymous", operator.role if operator else None,
              permission, "DENIED", target_type, target_id, {"required_role": PERMISSIONS[permission]})
    raise PermissionDenied(f"{permission} needs role {PERMISSIONS[permission]} or higher")
