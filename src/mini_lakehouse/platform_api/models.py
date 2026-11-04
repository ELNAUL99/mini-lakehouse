"""Pydantic models for the Platform API (design doc section 7)."""

from __future__ import annotations

from pydantic import BaseModel, Field

from mini_lakehouse.domain.contracts import DatasetContract, FieldSpec, Schema


class FieldModel(BaseModel):
    name: str
    type: str
    required: bool = True
    doc: str = ""

    def to_spec(self) -> FieldSpec:
        return FieldSpec(name=self.name, type=self.type, required=self.required, doc=self.doc)


class SchemaModel(BaseModel):
    version: int = 1
    fields: list[FieldModel]

    def to_schema(self) -> Schema:
        return Schema(version=self.version, fields=tuple(f.to_spec() for f in self.fields))


class RegisterDatasetRequest(BaseModel):
    name: str
    owner: str
    key: str
    retention_days: int = Field(gt=0)
    pii: bool = False
    description: str = ""
    partitioning: list[str] = Field(default_factory=list)
    schema_: SchemaModel = Field(alias="schema")

    model_config = {"populate_by_name": True}

    def to_contract(self) -> DatasetContract:
        return DatasetContract(
            name=self.name,
            owner=self.owner,
            key=self.key,
            retention_days=self.retention_days,
            schema=self.schema_.to_schema(),
            pii=self.pii,
            description=self.description,
            partitioning=tuple(self.partitioning),
        )


class DatasetView(BaseModel):
    name: str
    owner: str
    key: str
    retention_days: int
    pii: bool
    schema_version: int
    partitioning: list[str]
    current_snapshot: int | None = None
    freshness_seconds: float | None = None


class LineageView(BaseModel):
    dataset: str
    upstream_topic: str
    downstream_tables: list[str]


class HealthView(BaseModel):
    dataset: str
    small_file_ratio: float | None = None
    last_compaction: str | None = None
    consumer_lag: int | None = None
    dlq_rate: float | None = None


class ProposeSchemaRequest(BaseModel):
    schema_: SchemaModel = Field(alias="schema")
    model_config = {"populate_by_name": True}
