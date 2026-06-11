"""Load RICU JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import RICUConcept, RICUSourceItem


def load_concept_dict(path: str | Path) -> dict[str, RICUConcept]:
    """Load a RICU ``concept-dict.json`` file.

    Parameters
    ----------
    path:
        Path to the JSON file.
    """

    json_path = Path(path)
    with json_path.open("r", encoding="utf-8") as f:
        data: dict[str, dict[str, Any]] = json.load(f)

    concepts: dict[str, RICUConcept] = {}
    for key, raw_concept in data.items():
        raw_sources = raw_concept.get("sources") or {}
        sources: dict[str, list[RICUSourceItem]] = {}
        for source_name, source_items in raw_sources.items():
            if not isinstance(source_items, list):
                source_items = [source_items]
            sources[source_name] = [
                RICUSourceItem(source=source_name, raw=item)
                for item in source_items
                if isinstance(item, dict)
            ]
        concepts[key] = RICUConcept(key=key, raw=raw_concept, sources=sources)

    return concepts
