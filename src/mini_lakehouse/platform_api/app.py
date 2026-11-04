"""FastAPI control plane (design doc section 7).

Treats other teams as internal customers: register a dataset, view its schema,
lineage, and health, and propose a new schema version -- with breaking changes
rejected using the *same* pure compatibility checker the ingestor and CI use
(:func:`mini_lakehouse.domain.contracts.check_compatibility`).

State is kept in an in-memory registry here; the production shape is a Postgres
table (the OLTP control-plane catalog). Provisioning (topic/DLQ/table creation)
is a port the app calls on registration; locally it is logged.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException

from mini_lakehouse.domain.contracts import DatasetContract, check_compatibility

from .models import (
    DatasetView,
    HealthView,
    LineageView,
    ProposeSchemaRequest,
    RegisterDatasetRequest,
)

log = logging.getLogger("platform-api")


class DatasetRegistry:
    """In-memory stand-in for the Postgres-backed control-plane catalog."""

    def __init__(self) -> None:
        self._datasets: dict[str, DatasetContract] = {}

    def register(self, contract: DatasetContract) -> None:
        problems = contract.validate()
        if problems:
            raise ValueError("; ".join(problems))
        self._datasets[contract.name] = contract

    def get(self, name: str) -> DatasetContract:
        if name not in self._datasets:
            raise KeyError(name)
        return self._datasets[name]

    def names(self) -> list[str]:
        return sorted(self._datasets)


def create_app(registry: DatasetRegistry | None = None) -> FastAPI:
    registry = registry or DatasetRegistry()
    app = FastAPI(title="Mini-Lakehouse Platform API", version="0.1.0")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    def metrics() -> str:
        # Real deployment exposes Prometheus text here (prometheus-client).
        return "# mini_lakehouse platform api\nplatform_api_up 1\n"

    @app.post("/datasets", status_code=201)
    def register_dataset(req: RegisterDatasetRequest) -> dict[str, str]:
        try:
            registry.register(req.to_contract())
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        # Provisioning port call would create topic, DLQ and Iceberg table here.
        log.info("provisioning dataset %s (owner=%s)", req.name, req.owner)
        return {"status": "registered", "dataset": req.name}

    @app.get("/datasets/{name}", response_model=DatasetView)
    def get_dataset(name: str) -> DatasetView:
        contract = _load_or_404(registry, name)
        return DatasetView(
            name=contract.name,
            owner=contract.owner,
            key=contract.key,
            retention_days=contract.retention_days,
            pii=contract.pii,
            schema_version=contract.schema.version,
            partitioning=list(contract.partitioning),
        )

    @app.get("/datasets/{name}/lineage", response_model=LineageView)
    def get_lineage(name: str) -> LineageView:
        _load_or_404(registry, name)
        return LineageView(
            dataset=name,
            upstream_topic="telemetry.events.v1",
            downstream_tables=["silver.player_daily"],
        )

    @app.get("/datasets/{name}/health", response_model=HealthView)
    def get_health(name: str) -> HealthView:
        _load_or_404(registry, name)
        # Real deployment reads meta.table_health written by the maintenance job.
        return HealthView(dataset=name)

    @app.post("/schemas/{name}/versions", status_code=201)
    def propose_schema(name: str, req: ProposeSchemaRequest) -> dict[str, object]:
        contract = _load_or_404(registry, name)
        violations = check_compatibility(contract.schema, req.schema_.to_schema())
        if violations:
            raise HTTPException(
                status_code=409,
                detail={"error": "incompatible_schema_change", "violations": violations},
            )
        return {"status": "accepted", "dataset": name, "version": req.schema_.version}

    return app


def _load_or_404(registry: DatasetRegistry, name: str) -> DatasetContract:
    try:
        return registry.get(name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"dataset {name!r} not found") from exc


app = create_app()
