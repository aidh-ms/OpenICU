"""Map RICU concepts to OpenICU concept YAML dictionaries."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from .derived_rules import (
    KNOWN_DERIVED_RULES,
    build_complex_dataset_config,
    build_derived_dataset_config,
    dependency_keys,
)
from .models import GeneratedFile, RICUConcept, RICUSourceItem, UnsupportedItem
from .naming import category_name, concept_name
from .settings import ConverterSettings


class RICUToOpenICUMapper:
    """Convert loaded RICU concepts into OpenICU YAML payloads."""

    def __init__(
        self,
        settings: ConverterSettings,
        *,
        inferred_events_by_code_column: dict[tuple[str, str, str], list[str]] | None = None,
    ) -> None:
        self.settings = settings
        self.inferred_events_by_code_column = inferred_events_by_code_column or {}
        self.unsupported: list[UnsupportedItem] = []

    def build_files(
        self,
        concepts: dict[str, RICUConcept],
        *,
        sources: list[str],
        categories: set[str] | None = None,
        concept_keys: set[str] | None = None,
        include_derived: bool = False,
        complex_stubs: bool = False,
        complex_transformer: str = "open_icu.concepts.ricu_transformers.UnsupportedRICUTransformer",
    ) -> list[GeneratedFile]:
        files: list[GeneratedFile] = []

        for ricu_key, concept in sorted(concepts.items()):
            if concept_keys is not None and ricu_key not in concept_keys:
                continue
            if categories is not None and concept.category not in categories:
                continue
            if concept.is_recursive_or_derived:
                if not include_derived:
                    self.unsupported.append(
                        UnsupportedItem(
                            concept_key=ricu_key,
                            concept_name=self._concept_name(concept),
                            source="*",
                            table=None,
                            reason="derived/recursive RICU concepts are skipped by default",
                            raw=concept.raw,
                        )
                    )
                    continue

                files.append(self._global_concept_file(concept))
                for source in sources:
                    dataset_file = self._recursive_dataset_concept_file(
                        concept,
                        concepts,
                        source,
                        complex_stubs=complex_stubs,
                        complex_transformer=complex_transformer,
                    )
                    if dataset_file is not None:
                        files.append(dataset_file)
                continue

            files.append(self._global_concept_file(concept))
            for source in sources:
                dataset_file = self._dataset_concept_file(concept, source)
                if dataset_file is not None:
                    files.append(dataset_file)

        return files

    def _global_concept_file(self, concept: RICUConcept) -> GeneratedFile:
        name = self._concept_name(concept)
        data: dict[str, Any] = {
            "name": name,
            "version": self.settings.default_version,
            "unit": self._unit(concept),
            "extension_columns": self.settings.default_extension_columns,
        }
        rel_path = f"concept/{category_name(concept.category)}/{name}.yml"
        return GeneratedFile(path=rel_path, content=data)


    def _recursive_dataset_concept_file(
        self,
        concept: RICUConcept,
        concepts: dict[str, RICUConcept],
        source: str,
        *,
        complex_stubs: bool,
        complex_transformer: str,
    ) -> GeneratedFile | None:
        target = self.settings.source_target(source)
        if target is None:
            self.unsupported.append(
                UnsupportedItem(
                    concept_key=concept.key,
                    concept_name=self._concept_name(concept),
                    source=source,
                    table=None,
                    reason="source is not configured in converter settings",
                    raw=concept.raw,
                )
            )
            return None

        name = self._concept_name(concept)
        rel_path = f"dataset/{target.dataset}/{target.version}/concept/{name}.yml"

        rule = KNOWN_DERIVED_RULES.get(concept.key)
        if rule is not None:
            content = build_derived_dataset_config(
                rule=rule,
                concept=concept,
                concepts=concepts,
                settings=self.settings,
                target=target,
            )
            if rule.note:
                self.unsupported.append(
                    UnsupportedItem(
                        concept_key=concept.key,
                        concept_name=name,
                        source=source,
                        table=None,
                        reason=f"derived rule generated with note: {rule.note}",
                        raw=concept.raw,
                    )
                )
            return GeneratedFile(path=rel_path, content=content)

        if complex_stubs:
            content = build_complex_dataset_config(
                concept=concept,
                concepts=concepts,
                settings=self.settings,
                transformer_path=complex_transformer,
            )
            self.unsupported.append(
                UnsupportedItem(
                    concept_key=concept.key,
                    concept_name=name,
                    source=source,
                    table=None,
                    reason="complex stub generated; implementation still required",
                    raw={"depends_on": dependency_keys(concept), "callback": concept.callback},
                )
            )
            return GeneratedFile(path=rel_path, content=content)

        self.unsupported.append(
            UnsupportedItem(
                concept_key=concept.key,
                concept_name=name,
                source=source,
                table=None,
                reason="recursive RICU concept has no known derived rule; enable --complex-stubs to generate a complex placeholder",
                raw={"depends_on": dependency_keys(concept), "callback": concept.callback},
            )
        )
        return None

    def _dataset_concept_file(self, concept: RICUConcept, source: str) -> GeneratedFile | None:
        target = self.settings.source_target(source)
        if target is None:
            self.unsupported.append(
                UnsupportedItem(
                    concept_key=concept.key,
                    concept_name=self._concept_name(concept),
                    source=source,
                    table=None,
                    reason="source is not configured in converter settings",
                    raw=concept.raw,
                )
            )
            return None

        source_items = concept.sources.get(source) or []
        mappings: list[dict[str, Any]] = []
        for item in source_items:
            mappings.extend(self._mappings_for_item(concept, item, target.dataset))

        if not mappings:
            return None

        name = self._concept_name(concept)
        rel_path = f"dataset/{target.dataset}/{target.version}/concept/{name}.yml"
        return GeneratedFile(path=rel_path, content={"type": "simple", "mappings": mappings})

    def _mappings_for_item(
        self,
        concept: RICUConcept,
        item: RICUSourceItem,
        dataset: str,
    ) -> list[dict[str, Any]]:
        if item.table is None:
            self._unsupported(concept, item, "source item has no table")
            return []

        table = self.settings.normalize_table(item.source, item.table)
        code = self._code_pattern(item)
        if code is None:
            self._unsupported(concept, item, "source item has neither ids nor regex")
            return []

        event_names = self._event_names(dataset, table, item)
        if not event_names:
            self._unsupported(concept, item, "no event mapping found for source table")
            return []

        columns = self._columns(concept, item)
        mappings = []
        for event_name in event_names:
            mappings.append(
                {
                    "pattern": {
                        "table": table,
                        "event": event_name,
                        "code": code,
                    },
                    "columns": columns,
                }
            )

        if item.callback:
            self._unsupported(concept, item, "RICU callback preserved only in unsupported report")
        return mappings

    def _event_names(self, dataset: str, table: str, item: RICUSourceItem) -> list[str]:
        if item.sub_var:
            precise = self.inferred_events_by_code_column.get((dataset, table, item.sub_var))
            if precise:
                return precise
        return self.settings.event_names.get(dataset, {}).get(table, [])

    def _code_pattern(self, item: RICUSourceItem) -> str | None:
        if item.regex is not None:
            return self._regex_code(item.regex)
        if item.ids is not None:
            return self._ids_code(item.ids)
        if item.val_var is not None:
            # Column concepts such as eICU vitalperiodic systemicdiastolic map to
            # events where the code often is the value column name.
            return re.escape(str(item.val_var))
        return None

    @staticmethod
    def _ids_code(ids: Any) -> str:
        if isinstance(ids, list):
            values = ids
        else:
            values = [ids]
        parts = [f"{re.escape(str(value))}(//.*)?" for value in values]
        return "(" + "|".join(parts) + ")"

    def _regex_code(self, regex: str) -> str:
        if self.settings.regex_prefix_mode == "none":
            return f"({regex})"
        if self.settings.regex_prefix_mode == "contains":
            return f".*?({regex})"
        raise ValueError(
            "Unknown regex_prefix_mode "
            f"{self.settings.regex_prefix_mode!r}; expected 'none' or 'contains'."
        )

    def _columns(self, concept: RICUConcept, item: RICUSourceItem) -> dict[str, str]:
        if self.settings.logical_columns_mode == "boolean" and self._looks_like_logical_true(concept, item):
            return {
                "numeric_value": "const(1)",
                "text_value": 'const("true")',
            }

        return {
            "numeric_value": "col(numeric_value)",
            "text_value": "col(text_value)",
        }

    @staticmethod
    def _looks_like_logical_true(concept: RICUConcept, item: RICUSourceItem) -> bool:
        concept_class = concept.concept_class
        is_logical = concept_class == "lgl_cncpt" or (
            isinstance(concept_class, list) and "lgl_cncpt" in concept_class
        )
        callback = item.callback or concept.callback or ""
        return is_logical and "set_val(TRUE)" in callback

    def _unit(self, concept: RICUConcept) -> str:
        unit = concept.unit
        if isinstance(unit, list):
            return str(unit[0]) if unit else "None"
        if unit is not None:
            return str(unit)
        concept_class = concept.concept_class
        if concept_class == "lgl_cncpt" or (
            isinstance(concept_class, list) and "lgl_cncpt" in concept_class
        ):
            return "boolean"
        return "None"

    def _concept_name(self, concept: RICUConcept) -> str:
        return concept_name(concept.key, concept.description, self.settings.concept_names)

    def _unsupported(self, concept: RICUConcept, item: RICUSourceItem, reason: str) -> None:
        self.unsupported.append(
            UnsupportedItem(
                concept_key=concept.key,
                concept_name=self._concept_name(concept),
                source=item.source,
                table=item.table,
                reason=reason,
                raw=item.raw,
            )
        )


def unsupported_report(items: list[UnsupportedItem]) -> dict[str, Any]:
    """Group unsupported items into a compact YAML report."""

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item.reason].append(
            {
                "concept_key": item.concept_key,
                "concept_name": item.concept_name,
                "source": item.source,
                "table": item.table,
                "raw": item.raw,
            }
        )
    return {"unsupported": dict(sorted(grouped.items()))}
