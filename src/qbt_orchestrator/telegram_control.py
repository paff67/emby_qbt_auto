from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Dict, Set
VIEWER = {"status", "trace", "perf"}; OPERATOR = VIEWER | {"pause", "resume", "queue"}; ADMIN = OPERATOR | {"force_upload", "cleanup", "preempt", "config"}
class TelegramAuthorizer:
    def __init__(self, viewers: Set[int] | None = None, operators: Set[int] | None = None, admins: Set[int] | None = None): self.viewers = viewers or set(); self.operators = operators or set(); self.admins = admins or set()
    def role_for(self, user_id: int) -> str | None:
        if user_id in self.admins: return "admin"
        if user_id in self.operators: return "operator"
        if user_id in self.viewers: return "viewer"
        return None
    def allowed(self, user_id: int, command: str) -> bool:
        role = self.role_for(user_id)
        return (role == "admin" and command in ADMIN) or (role == "operator" and command in OPERATOR) or (role == "viewer" and command in VIEWER)
@dataclass
class Approval:
    action: str; payload: dict; expires_at: int; approved: bool = False
class ApprovalStore:
    def __init__(self, now: Callable[[], int] | None = None): self.now = now or (lambda: int(__import__("time").time())); self._items: Dict[str, Approval] = {}; self._next = 1
    def create(self, action: str, payload: dict, ttl: int) -> str:
        aid = f"approval-{self._next}"; self._next += 1; self._items[aid] = Approval(action, payload, self.now() + ttl); return aid
    def approve_once(self, approval_id: str, user_id: int) -> bool:
        item = self._items.get(approval_id)
        if not item or item.approved or item.expires_at < self.now(): return False
        item.approved = True; return True
