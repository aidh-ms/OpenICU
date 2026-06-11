"""Small typed models used by the converter.

The converter intentionally keeps the raw RICU item around. RICU contains many
source-specific fields and R callbacks that should not be silently discarded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RICUSourceItem:
    """One item from ``concept["sources"][source]``."""

    source: str
    raw: dict[str, Any]

    @property
    def table(self) -> str | None:
        return self.raw.get("table")

    @property
    def ids(self) -> Any | None:
        return self.raw.get("ids")

    @property
    def regex(self) -> str | None:
        return self.raw.get("regex")

    @property
    def sub_var(self) -> str | None:
        return self.raw.get("sub_var")

    @property
    def val_var(self) -> str | None:
        return self.raw.get("val_var")

    @property
    def index_var(self) -> str | None:
        return self.raw.get("index_var")

    @property
    def callback(self) -> str | None:
        return self.raw.get("callback")

    @property
    def item_class(self) -> str | None:
        return self.raw.get("class")


@dataclass(frozen=True)
class RICUConcept:
    """One top-level entry from RICU's ``concept-dict.json``."""

    key: str
    raw: dict[str, Any]
    sources: dict[str, list[RICUSourceItem]] = field(default_factory=dict)

    @property
    def description(self) -> str | None:
        return self.raw.get("description")

    @property
    def category(self) -> str:
        return self.raw.get("category") or "uncategorized"

    @property
    def unit(self) -> str | list[str] | None:
        return self.raw.get("unit")

    @property
    def min_value(self) -> int | float | None:
        return self.raw.get("min")

    @property
    def max_value(self) -> int | float | None:
        return self.raw.get("max")

    @property
    def concept_class(self) -> str | list[str] | None:
        return self.raw.get("class")

    @property
    def callback(self) -> str | None:
        return self.raw.get("callback")

    @property
    def is_recursive_or_derived(self) -> bool:
        return "concepts" in self.raw or self.raw.get("class") == "rec_cncpt"


@dataclass(frozen=True)
class GeneratedFile:
    path: str
    content: dict[str, Any]


@dataclass(frozen=True)
class UnsupportedItem:
    concept_key: str
    concept_name: str
    source: str
    table: str | None
    reason: str
    raw: dict[str, Any]
