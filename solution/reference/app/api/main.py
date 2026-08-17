from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.domain.models import DomainError
from app.domain.service import PitrService
from app.persistence.db import connect
from app.scheduling.clock import Clock, FakeClock, SystemClock
from app.scheduling.failures import FailureInjector


class PutBody(BaseModel):
    value: Any
    expected_version: int | None = None


class DeleteBody(BaseModel):
    expected_version: int | None = None


class WorkerBody(BaseModel):
    worker_id: str
    lease_seconds: float = 30.0


class RestoreCreate(BaseModel):
    as_of_ms: float | None = None
    as_of_lsn: int | None = None


class ClockAdvance(BaseModel):
    seconds: float


class FailuresArm(BaseModel):
    transient_db_failures: int | None = None
    process_interrupt: bool | None = None


class OptimisticBody(BaseModel):
    expected_version: int


def create_app(
    db_path: str | Path | None = None,
    clock: Clock | None = None,
    injector: FailureInjector | None = None,
) -> FastAPI:
    clock = clock or SystemClock()
    injector = injector or FailureInjector()
    store = PitrService(connect(db_path), clock=clock, injector=injector)
    app = FastAPI(title="Point-in-Time WAL Restore Engine", version="1.0.0")
    app.state.store = store
    app.state.clock = clock
    app.state.injector = injector

    @app.exception_handler(DomainError)
    async def _err(_req: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict())

    @app.get("/health")
    def health() -> dict[str, Any]:
        return store.health()

    @app.get("/metrics")
    def metrics() -> dict[str, Any]:
        return store.metrics()

    @app.put("/records/{key}")
    def put(key: str, body: PutBody) -> JSONResponse:
        rec, code = store.put_record(key, body.value, body.expected_version)
        return JSONResponse(status_code=code, content=rec)

    @app.get("/records/{key}")
    def get(key: str) -> dict[str, Any]:
        return store.get_record(key)

    @app.delete("/records/{key}")
    def delete(key: str, expected_version: int | None = None) -> dict[str, Any]:
        return store.delete_record(key, expected_version)

    @app.get("/records")
    def list_records() -> list[dict[str, Any]]:
        return store.list_records()

    @app.get("/wal")
    def wal(from_lsn: int = 0) -> list[dict[str, Any]]:
        return store.list_wal(from_lsn)

    @app.post("/checkpoints", status_code=201)
    def create_ckpt() -> dict[str, Any]:
        return store.create_checkpoint()

    @app.get("/checkpoints")
    def list_ckpt() -> list[dict[str, Any]]:
        return store.list_checkpoints()

    @app.get("/checkpoints/{checkpoint_id}")
    def get_ckpt(checkpoint_id: str) -> dict[str, Any]:
        return store.get_checkpoint(checkpoint_id)

    @app.post("/checkpoints/{checkpoint_id}/run")
    def run_ckpt(checkpoint_id: str, body: WorkerBody) -> dict[str, Any]:
        return store.run_checkpoint(checkpoint_id, body.worker_id, body.lease_seconds)

    @app.post("/restore", status_code=201)
    def create_restore(body: RestoreCreate) -> dict[str, Any]:
        return store.create_restore(as_of_ms=body.as_of_ms, as_of_lsn=body.as_of_lsn)

    @app.get("/restore/{restore_id}")
    def get_restore(restore_id: str) -> dict[str, Any]:
        return store.get_restore(restore_id)

    @app.post("/restore/{restore_id}/run")
    def run_restore(restore_id: str, body: WorkerBody) -> dict[str, Any]:
        return store.run_restore(restore_id, body.worker_id, body.lease_seconds)

    @app.post("/records/{key}/optimistic")
    def optimistic(key: str, body: OptimisticBody) -> dict[str, Any]:
        return store.force_version(key, body.expected_version)

    @app.get("/audit")
    def audit(limit: int = 100) -> list[dict[str, Any]]:
        return store.list_audit(limit)

    @app.post("/_test/clock/advance")
    def advance(body: ClockAdvance) -> dict[str, Any]:
        if not isinstance(clock, FakeClock):
            raise DomainError("clock_not_fake", "fake clock required")
        return {"now": clock.advance(body.seconds)}

    @app.post("/_test/failures/arm")
    def arm(body: FailuresArm) -> dict[str, str]:
        if body.transient_db_failures is not None:
            injector.arm_transient_db_failures(body.transient_db_failures)
        if body.process_interrupt:
            injector.arm_process_interrupt()
        return {"status": "armed"}

    return app
