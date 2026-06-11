"""Helpers for reading RICU concept metadata."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class RicuConceptMeta:
    """Small wrapper around ``ricu/inst/extdata/config/concept-dict.json``."""

    def __init__(self, data: dict[str, dict[str, Any]] | None = None) -> None:
        self.data = data or {}

    @classmethod
    def from_json(cls, path: str | Path | None) -> "RicuConceptMeta":
        if path is None:
            return cls({})
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"RICU concept dictionary not found: {p}")
        with p.open("r", encoding="utf-8") as f:
            return cls(json.load(f))

    def range_for(self, ricu_name: str) -> tuple[float | None, float | None]:
        item = self.data.get(ricu_name, {})
        return item.get("min"), item.get("max")

    def aggregate_for(self, ricu_name: str, default: str = "mean") -> str:
        item = self.data.get(ricu_name, {})
        return item.get("aggregate", default)

    def description_for(self, ricu_name: str) -> str | None:
        item = self.data.get(ricu_name, {})
        return item.get("description")
