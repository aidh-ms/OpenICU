"""Naming helpers for generated OpenICU config names."""

from __future__ import annotations

import re


def snake_case(value: str) -> str:
    """Convert a human-readable RICU description into a safe config name.

    This keeps existing uppercase letters from descriptions such as ``GCS`` or
    ``CO2`` because your current OpenICU config names also preserve some of
    those forms.
    """

    value = value.strip()
    value = value.replace("/", "_")
    value = value.replace("-", "_")
    value = re.sub(r"[^0-9A-Za-z_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "unnamed_concept"


def concept_name(ricu_key: str, description: str | None, overrides: dict[str, str]) -> str:
    """Return the OpenICU concept name for a RICU key."""

    if ricu_key in overrides:
        return overrides[ricu_key]
    if description:
        return snake_case(description)
    return snake_case(ricu_key)


def category_name(category: str | None) -> str:
    """Return a filesystem-safe concept category."""

    return snake_case(category or "uncategorized")
