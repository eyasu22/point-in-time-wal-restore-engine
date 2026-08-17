from __future__ import annotations

from app.domain.models import DomainError


class FailureInjector:
    def __init__(self) -> None:
        self._db = 0
        self._process_interrupt = False

    def arm_transient_db_failures(self, count: int) -> None:
        self._db = int(count)

    def arm_process_interrupt(self) -> None:
        self._process_interrupt = True

    def maybe_fail_db(self, op: str = "db") -> None:
        if self._db > 0:
            self._db -= 1
            raise DomainError("transient_db_failure", f"injected:{op}", status_code=503)

    def consume_process_interrupt(self) -> bool:
        if self._process_interrupt:
            self._process_interrupt = False
            return True
        return False
