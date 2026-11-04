"""Validate dataset contract YAML files (CI step, design doc section 7).

For each ``*.yaml`` under the given directory this:

* parses it into a :class:`~mini_lakehouse.domain.contracts.DatasetContract`,
* runs the structural ``.validate()`` rules, and
* checks that each JSON Schema referenced by ``schema.version`` (and any
  ``evolution`` entries) exists under ``contracts/schemas/`` and parses.

Exits non-zero if any contract is invalid, so a bad contract fails the build.

Usage:  python -m mini_lakehouse.tools.validate_contracts contracts/
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

from mini_lakehouse.domain.contracts import DatasetContract, FieldSpec, Schema


def _load_contract(doc: dict) -> DatasetContract:
    schema_doc = doc.get("schema", {})
    fields = tuple(
        FieldSpec(
            name=f["name"],
            type=f["type"],
            required=bool(f.get("required", True)),
            doc=f.get("doc", ""),
        )
        for f in schema_doc.get("fields", [])
    )
    return DatasetContract(
        name=doc.get("name", ""),
        owner=doc.get("owner", ""),
        key=doc.get("key", ""),
        retention_days=int(doc.get("retention_days", 0)),
        schema=Schema(version=int(schema_doc.get("version", 1)), fields=fields),
        pii=bool(doc.get("pii", False)),
        description=doc.get("description", ""),
        partitioning=tuple((doc.get("table", {}) or {}).get("partitioning", [])),
    )


def validate_file(path: Path, schema_dir: Path) -> list[str]:
    problems: list[str] = []
    try:
        doc = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        return [f"{path.name}: YAML parse error: {exc}"]

    contract = _load_contract(doc)
    problems += [f"{path.name}: {p}" for p in contract.validate()]

    versions = {contract.schema.version}
    for evo in doc.get("evolution", []) or []:
        versions.add(int(evo["version"]))
    for version in sorted(versions):
        schema_path = schema_dir / f"event.v{version}.json"
        if not schema_path.exists():
            problems.append(f"{path.name}: missing JSON Schema {schema_path.name}")
            continue
        try:
            json.loads(schema_path.read_text())
        except json.JSONDecodeError as exc:
            problems.append(f"{path.name}: {schema_path.name} is not valid JSON: {exc}")
    return problems


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    root = Path(argv[0]) if argv else Path("contracts")
    schema_dir = root / "schemas"

    contracts = sorted(root.glob("*.yaml"))
    if not contracts:
        print(f"no contracts found under {root}/")
        return 1

    all_problems: list[str] = []
    for path in contracts:
        all_problems += validate_file(path, schema_dir)

    if all_problems:
        print("Contract validation FAILED:")
        for p in all_problems:
            print(f"  - {p}")
        return 1
    print(f"OK: {len(contracts)} contract(s) valid")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
