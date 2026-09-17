"""Global event-order configuration."""

import re
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator

from open_icu.config.root import get_config_root


class EventOrderGroup(BaseModel):
    """A semantic event group with optional explicit internal ordering."""

    order: int = Field(
        ...,
        description="Sort priority of this group. Lower values come first.",
    )
    patterns: list[str] = Field(
        default_factory=list,
        description="Regular expressions used to assign event codes to this group.",
    )
    explicit_order: list[str] = Field(
        default_factory=list,
        description=(
            "Optional explicit ordering for selected concepts within this group. "
            "Other matching events are ordered alphabetically."
        ),
    )


class EventOrderConfig(BaseModel):
    """Global configuration defining canonical ordering of simultaneous events."""

    default_group_order: int = Field(
        default=50,
        description="Group order used for events that do not match any configured group.",
    )
    unassigned: Literal["ignore", "warn", "error"] = Field(
        default="warn",
        description="How to handle events that do not match any configured group.",
    )
    groups: dict[str, EventOrderGroup] = Field(
        default_factory=dict,
        description="Named semantic event groups.",
    )

    @classmethod
    def load(cls, path: Path | None = None) -> Self:
        """Load the global event-order configuration.

        If no path is provided, OpenICU's shipped default configuration is used.
        """
        if path is None:
            config_root = get_config_root()
            if config_root is None:
                raise FileNotFoundError(
                    "Could not locate OpenICU's shipped configuration directory."
                )

            path = config_root / "event_order" / "default.yml"

        with path.open("r") as f:
            data = yaml.safe_load(f) or {}

        return cls(**data)

    @model_validator(mode="after")
    def validate_configuration(self) -> "EventOrderConfig":
        """Validate regular expressions and explicit event assignments."""
        explicitly_ordered: dict[str, str] = {}

        for group_name, group in self.groups.items():
            for pattern in group.patterns:
                try:
                    re.compile(pattern)
                except re.error as exc:
                    raise ValueError(
                        f"Invalid event-order regex in group '{group_name}': "
                        f"{pattern!r}: {exc}"
                    ) from exc

            for event in group.explicit_order:
                if event in explicitly_ordered:
                    raise ValueError(
                        f"Event '{event}' has an explicit order in multiple groups: "
                        f"'{explicitly_ordered[event]}' and '{group_name}'."
                    )

                explicitly_ordered[event] = group_name

        return self

    def group_for(self, code: str) -> tuple[str | None, EventOrderGroup | None]:
        """Return the first configured group matching an event code."""
        event_code = code.split("//", 1)[0]

        for group_name, group in self.groups.items():
            if any(re.search(pattern, event_code) for pattern in group.patterns):
                return group_name, group

        return None, None

    def group_order_for(self, code: str) -> int:
        """Return the semantic group order for an event code."""
        _, group = self.group_for(code)
        if group is None:
            return self.default_group_order

        return group.order

    def explicit_order_for(self, code: str) -> int | None:
        """Return explicit internal order for an event code, if configured."""
        _, group = self.group_for(code)
        if group is None:
            return None

        try:
            return group.explicit_order.index(code)
        except ValueError:
            return None
