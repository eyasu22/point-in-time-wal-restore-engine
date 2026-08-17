from fastapi import FastAPI


def create_app(*args, **kwargs):
    """Incomplete starter — implement the point-in-time WAL restore engine."""
    app = FastAPI(title="Point-in-Time WAL Restore Engine (incomplete)")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/metrics")
    def metrics():
        return {
            "records_live": 0,
            "records_deleted": 0,
            "wal_records": 0,
            "max_lsn": 0,
            "checkpoints_ready": 0,
            "restores_succeeded": 0,
            "audit_events": 0,
        }

    return app
