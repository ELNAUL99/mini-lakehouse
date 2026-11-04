"""Validate events against the versioned JSON Schema before they enter Kafka.

Design doc F2: events are validated against a versioned schema; F4: invalid events
go to the DLQ with a failure reason. The gateway validates on the way in; the
schema chosen is driven by the event's ``schema_version`` so v1 and v2 can be
accepted simultaneously during an evolution window.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema import exceptions as js_exceptions

_DEFAULT_SCHEMA_DIR = Path(__file__).resolve().parents[3] / "contracts" / "schemas"


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    ok: bool
    reason: str = ""
    detail: str = ""


class SchemaRegistry:
    """Loads versioned JSON Schemas and validates events by ``schema_version``."""

    def __init__(self, schema_dir: Path | None = None) -> None:
        self._dir = schema_dir or _DEFAULT_SCHEMA_DIR
        self._validators: dict[int, Draft202012Validator] = {}

    def _validator(self, version: int) -> Draft202012Validator | None:
        if version in self._validators:
            return self._validators[version]
        path = self._dir / f"event.v{version}.json"
        if not path.exists():
            return None
        schema = json.loads(path.read_text())
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        self._validators[version] = validator
        return validator

    def validate(self, event: dict[str, Any]) -> ValidationOutcome:
        version = event.get("schema_version")
        if not isinstance(version, int) or isinstance(version, bool):
            return ValidationOutcome(False, "bad_schema_version", repr(version))
        validator = self._validator(version)
        if validator is None:
            return ValidationOutcome(False, "unknown_schema_version", str(version))
        try:
            validator.validate(event)
        except js_exceptions.ValidationError as exc:
            location = "/".join(str(p) for p in exc.absolute_path) or "<root>"
            return ValidationOutcome(False, "schema_violation", f"{location}: {exc.message}")
        return ValidationOutcome(True)
