from __future__ import annotations

from enum import Enum
from typing import Any


class WalOp(str, Enum):
    PUT = "PUT"
    DELETE = "DELETE"


class CheckpointStatus(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"


class RestoreStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class DomainError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400):
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        return {"error": self.code, "message": self.message}


class ConflictError(DomainError):
    def __init__(self, code: str, message: str):
        super().__init__(code, message, 409)


class NotFoundError(DomainError):
    def __init__(self, code: str, message: str):
        super().__init__(code, message, 404)
