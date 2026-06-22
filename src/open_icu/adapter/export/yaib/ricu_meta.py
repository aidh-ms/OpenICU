"""Helpers for reading RICU concept-dict metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RicuConceptMeta:
    """Small access layer for RICU concept metadata.

    The parser is intentionally tolerant because RICU concept dictionaries can
    contain nested metadata and source-specific details.
    """

    concepts: dict[str, Any]

    @classmethod
    def from_json(cls, path: str | Path | None) -> "RicuConceptMeta":
        if path is None:
            return cls({})
        p = Path(path)
        if not p.is_file():
            raise FileNotFoundError(f"RICU concept dictionary not found: {p}")
        with p.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        if isinstance(raw, dict) and "concepts" in raw and isinstance(raw["concepts"], dict):
            raw = raw["concepts"]
        if not isinstance(raw, dict):
            raise ValueError("RICU concept dictionary must be a JSON object or contain a 'concepts' object.")
        return cls(raw)

    def concept(self, name: str) -> dict[str, Any]:
        obj = self.concepts.get(name, {})
        return obj if isinstance(obj, dict) else {}

    def _find_numeric_meta(self, name: str, key: str) -> float | None:
        obj = self.concept(name)

        def walk(x: Any) -> float | None:
            if isinstance(x, dict):
                if key in x and isinstance(x[key], (int, float)):
                    return float(x[key])
                for child in x.values():
                    found = walk(child)
                    if found is not None:
                        return found
            elif isinstance(x, list):
                for child in x:
                    found = walk(child)
                    if found is not None:
                        return found
            return None

        return walk(obj)

    def range_for(self, name: str) -> tuple[float | None, float | None]:
        """Return RICU min/max range for a concept, if available."""
        return self._find_numeric_meta(name, "min"), self._find_numeric_meta(name, "max")

    def aggregate_for(self, name: str, default: str = "mean") -> str:
        """Return configured RICU aggregation, if available."""
        obj = self.concept(name)

        def walk(x: Any) -> str | None:
            if isinstance(x, dict):
                for key in ("aggregate", "aggregation", "fun"):
                    value = x.get(key)
                    if isinstance(value, str):
                        return value
                for child in x.values():
                    found = walk(child)
                    if found is not None:
                        return found
            elif isinstance(x, list):
                for child in x:
                    found = walk(child)
                    if found is not None:
                        return found
            return None

        return walk(obj) or default
