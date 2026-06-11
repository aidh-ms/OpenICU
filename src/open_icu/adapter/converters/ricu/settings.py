"""Converter settings and OpenICU project introspection."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .derived_rules import DEFAULT_RECURSIVE_NAME_OVERRIDES


@dataclass
class SourceTarget:
    dataset: str
    version: str


@dataclass
class ConverterSettings:
    """User-configurable conversion settings."""

    sources: dict[str, SourceTarget] = field(default_factory=dict)
    table_aliases: dict[str, dict[str, str]] = field(default_factory=dict)
    event_names: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    concept_names: dict[str, str] = field(default_factory=dict)
    # How RICU regex patterns are converted to OpenICU code patterns.
    # "none" preserves the RICU regex as-is in parentheses.
    # "contains" prepends .*? so the regex may match anywhere in the code string.
    regex_prefix_mode: str = "none"
    # How logical RICU concepts with set_val(TRUE) are converted.
    # "preserve" keeps OpenICU's extracted numeric_value/text_value columns.
    # "boolean" writes const(1)/const("true") for matching events.
    logical_columns_mode: str = "preserve"
    default_version: str = "1.0.0"
    default_extension_columns: dict[str, str] = field(
        default_factory=lambda: {"dataset": 'col("dataset")', "table": 'col("table")'}
    )

    @classmethod
    def from_file(cls, path: str | Path | None) -> "ConverterSettings":
        settings = cls.with_defaults()
        if path is None:
            return settings

        with Path(path).open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        if "sources" in data:
            settings.sources.update(
                {
                    key: SourceTarget(dataset=value["dataset"], version=str(value["version"]))
                    for key, value in data["sources"].items()
                }
            )
        if "table_aliases" in data:
            settings.table_aliases.update(data["table_aliases"] or {})
        if "events" in data:
            settings.event_names.update(_normalize_events(data["events"] or {}))
        if "concept_names" in data:
            settings.concept_names.update(data["concept_names"] or {})
        if "regex_prefix_mode" in data:
            settings.regex_prefix_mode = str(data["regex_prefix_mode"] or "none")
        if "logical_columns_mode" in data:
            settings.logical_columns_mode = str(data["logical_columns_mode"] or "preserve")
        if "default_version" in data:
            settings.default_version = str(data["default_version"])
        if "default_extension_columns" in data:
            settings.default_extension_columns = data["default_extension_columns"] or {}
        return settings

    @classmethod
    def with_defaults(cls) -> "ConverterSettings":
        return cls(
            sources={
                "mimic": SourceTarget(dataset="mimic-iv", version="3.1"),
                "mimic_demo": SourceTarget(dataset="mimic-demo", version="3.1"),
                "eicu": SourceTarget(dataset="eicu-crd", version="2.0"),
                "eicu_demo": SourceTarget(dataset="eicu-demo", version="2.0"),
                "miiv": SourceTarget(dataset="mimic-iv", version="3.1"),
            },
            table_aliases={
                "mimic": {
                    "inputevents_mv": "inputevents",
                    "inputevents_cv": "inputevents",
                },
                "mimic_demo": {
                    "inputevents_mv": "inputevents",
                    "inputevents_cv": "inputevents",
                },
                "miiv": {
                    "inputevents": "inputevents",
                },
            },
            concept_names=dict(DEFAULT_RECURSIVE_NAME_OVERRIDES),
            event_names={
                "mimic-iv": {
                    "chartevents": ["CHART"],
                    "labevents": ["LAB"],
                    "prescriptions": ["MEDICATION_ORDER"],
                    "inputevents": ["INFUSION_START", "INFUSION_END"],
                },
                "mimic-demo": {
                    "chartevents": ["CHART"],
                    "labevents": ["LAB"],
                    "prescriptions": ["MEDICATION_ORDER"],
                    "inputevents": ["INFUSION_START", "INFUSION_END"],
                },
                "eicu-crd": {
                    "lab": ["LAB"],
                    "infusiondrug": ["INFUSION"],
                    "medication": ["MEDICATION_ORDER"],
                    "vitalperiodic": ["VITAL_PERIODIC"],
                    "vitalaperiodic": ["VITAL_APERIODIC"],
                    "patient": ["PATIENT"],
                    "admissiondx": ["ADMISSION_DX"],
                },
                "eicu-demo": {
                    "lab": ["LAB"],
                    "infusiondrug": ["INFUSION"],
                    "medication": ["MEDICATION_ORDER"],
                    "vitalperiodic": ["VITAL_PERIODIC"],
                    "patient": ["PATIENT"],
                },
            },
        )

    def source_target(self, source: str) -> SourceTarget | None:
        return self.sources.get(source)

    def normalize_table(self, source: str, table: str) -> str:
        return self.table_aliases.get(source, {}).get(table, table)


def _normalize_events(raw: dict[str, Any]) -> dict[str, dict[str, list[str]]]:
    normalized: dict[str, dict[str, list[str]]] = {}
    for dataset, tables in raw.items():
        normalized[dataset] = {}
        for table, value in (tables or {}).items():
            if isinstance(value, str):
                normalized[dataset][table] = [value]
            elif isinstance(value, list):
                normalized[dataset][table] = [str(item) for item in value]
            else:
                normalized[dataset][table] = []
    return normalized


def load_project_event_names(config_root: str | Path) -> dict[str, dict[str, list[str]]]:
    """Infer event names from an existing OpenICU ``config`` directory.

    The result is ``{dataset: {table: [event_name, ...]}}``. It is intentionally
    conservative and only reads YAML files under ``config/dataset/*/*/dataset``.
    """

    root = Path(config_root)
    inferred: dict[str, dict[str, list[str]]] = {}
    dataset_root = root / "dataset"
    if not dataset_root.exists():
        return inferred

    for yaml_path in dataset_root.glob("*/*/dataset/*.yml"):
        parts = yaml_path.relative_to(dataset_root).parts
        if len(parts) < 4:
            continue
        dataset = parts[0]
        table = yaml_path.stem
        try:
            with yaml_path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except yaml.YAMLError:
            continue
        events = data.get("events") or []
        names = [event.get("name") for event in events if isinstance(event, dict) and event.get("name")]
        if names:
            inferred.setdefault(dataset, {})[table] = names
    return inferred


def load_project_event_names_by_code_column(
    config_root: str | Path,
) -> dict[tuple[str, str, str], list[str]]:
    """Infer event names keyed by ``(dataset, table, source_column)``.

    This is more precise than the table-only fallback. Example: in MIMIC
    ``inputevents``, events with ``code: [col(itemid), col(label)]`` are selected
    for RICU items where ``sub_var == itemid``; weight events with ``const(kg)``
    are not selected.
    """

    root = Path(config_root)
    dataset_root = root / "dataset"
    inferred: dict[tuple[str, str, str], list[str]] = {}
    if not dataset_root.exists():
        return inferred

    for yaml_path in dataset_root.glob("*/*/dataset/*.yml"):
        parts = yaml_path.relative_to(dataset_root).parts
        if len(parts) < 4:
            continue
        dataset = parts[0]
        table = yaml_path.stem
        try:
            with yaml_path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except yaml.YAMLError:
            continue
        for event in data.get("events") or []:
            if not isinstance(event, dict) or not event.get("name"):
                continue
            code_exprs = ((event.get("columns") or {}).get("code") or [])
            if isinstance(code_exprs, str):
                code_exprs = [code_exprs]
            for expr in code_exprs:
                if not isinstance(expr, str):
                    continue
                col_name = _extract_single_col_name(expr)
                if col_name:
                    inferred.setdefault((dataset, table, col_name), []).append(event["name"])
    return inferred


def _extract_single_col_name(expr: str) -> str | None:
    expr = expr.strip()
    if expr.startswith("col(") and expr.endswith(")"):
        inner = expr[4:-1].strip().strip('"').strip("'")
        return inner or None
    return None
