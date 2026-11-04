"""Data contracts and schema compatibility rules.

A dataset is declared in a small YAML file (name, owner, schema, key, retention);
CI validates it and the platform provisions the topic, DLQ, and Iceberg table from
it (design doc section 7). This module is the *pure* half: the contract model and
the compatibility checker. Parsing YAML lives in an adapter; the rules live here so
they are unit-tested and reused by CI and the Platform API's
``POST /schemas/{name}/versions`` endpoint.

Compatibility policy (section 7):

* Adding an **optional** field is allowed (backward compatible).
* Removing a field, renaming a field, or changing a field's type is **rejected**
  unless a new major version is created.
* Adding a **required** field is a breaking change (old producers omit it).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """One field in a dataset schema."""

    name: str
    type: str
    required: bool = True
    doc: str = ""


@dataclass(frozen=True, slots=True)
class Schema:
    """A versioned ordered set of fields."""

    version: int
    fields: tuple[FieldSpec, ...]

    def by_name(self) -> dict[str, FieldSpec]:
        return {f.name: f for f in self.fields}


@dataclass(frozen=True, slots=True)
class DatasetContract:
    """A declarative dataset contract.

    ``key`` is the field used as the Kafka message key (ordering) and dedupe/idempotency
    anchor; ``retention_days`` bounds the topic retention; ``pii`` flags handling.
    """

    name: str
    owner: str
    key: str
    retention_days: int
    schema: Schema
    pii: bool = False
    description: str = ""
    partitioning: tuple[str, ...] = field(default_factory=tuple)

    def validate(self) -> list[str]:
        """Return a list of structural problems; empty means the contract is well-formed."""
        problems: list[str] = []
        if not self.name:
            problems.append("contract.name is required")
        if not self.owner:
            problems.append("contract.owner is required")
        if self.retention_days <= 0:
            problems.append("contract.retention_days must be positive")
        names = [f.name for f in self.schema.fields]
        if not names:
            problems.append("schema must declare at least one field")
        if len(names) != len(set(names)):
            problems.append("schema has duplicate field names")
        if self.key and self.key not in set(names):
            problems.append(f"key {self.key!r} is not a field in the schema")
        return problems


class CompatibilityError(Exception):
    """Raised when a proposed schema change is not backward compatible."""

    def __init__(self, violations: list[str]) -> None:
        self.violations = violations
        super().__init__("; ".join(violations))


def check_compatibility(old: Schema, new: Schema) -> list[str]:
    """Return a list of backward-incompatibility violations between ``old`` and ``new``.

    An empty list means ``new`` is a safe, additive evolution of ``old`` within the
    same major version. A non-empty list means a new major version is required.
    """
    violations: list[str] = []
    old_fields = old.by_name()
    new_fields = new.by_name()

    # Removed fields.
    for name in old_fields:
        if name not in new_fields:
            violations.append(f"field {name!r} was removed")

    for name, new_spec in new_fields.items():
        old_spec = old_fields.get(name)
        if old_spec is None:
            # Added field: only optional additions are backward compatible.
            if new_spec.required:
                violations.append(f"field {name!r} was added as required (must be optional)")
            continue
        # Type change (covers renames-as-retype at the same position too).
        if old_spec.type != new_spec.type:
            violations.append(
                f"field {name!r} changed type {old_spec.type!r} -> {new_spec.type!r}"
            )
        # Widening required -> optional is fine; narrowing optional -> required breaks old writers.
        if not old_spec.required and new_spec.required:
            violations.append(f"field {name!r} became required (was optional)")

    return violations


def assert_compatible(old: Schema, new: Schema) -> None:
    """Raise :class:`CompatibilityError` if ``new`` is not backward compatible with ``old``."""
    violations = check_compatibility(old, new)
    if violations:
        raise CompatibilityError(violations)
