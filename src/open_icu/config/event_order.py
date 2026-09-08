"""Global event-order configuration."""

from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator


DEFAULT_EVENT_ORDER_CONFIG = Path(__file__).with_name("default_event_order.yml")


class EventOrderGroup(BaseModel):
    """A group of concepts sharing the same event-order priority."""

    order: int = Field(
        ...,
        description="Sort priority for concepts in this group. Lower values come first.",
    )
    concepts: list[str] = Field(
        default_factory=list,
        description="Concept names assigned to this event-order group.",
    )


class EventOrderConfig(BaseModel):
    """Global configuration defining semantic ordering of simultaneous events."""

    default_order: int = Field(
        default=50,
        description="Order used for concepts that are not assigned to any group.",
    )
    unassigned: Literal["ignore", "warn", "error"] = Field(
        default="warn",
        description="How to handle concepts that use the default event order.",
    )
    groups: dict[str, EventOrderGroup] = Field(
        default_factory=dict,
        description="Named semantic event-order groups.",
    )

    @classmethod
    def load(cls, path: Path | None = None) -> Self:
        """Load an event-order configuration.

        If no path is provided, the built-in OpenICU default configuration
        is loaded.
        """
        config_path = path or DEFAULT_EVENT_ORDER_CONFIG

        with config_path.open("r") as f:
            data = yaml.safe_load(f)

        return cls(**data)

    @model_validator(mode="after")
    def validate_unique_concepts(self) -> "EventOrderConfig":
        """Ensure each concept is assigned to at most one group."""
        seen: dict[str, str] = {}

        for group_name, group in self.groups.items():
            for concept in group.concepts:
                if "//" in concept:
                    raise ValueError(
                        f"Event-order concept '{concept}' must be a concept name, "
                        "not a full code."
                    )

                if concept in seen:
                    raise ValueError(
                        f"Concept '{concept}' is assigned to multiple event-order "
                        f"groups: '{seen[concept]}' and '{group_name}'."
                    )

                seen[concept] = group_name

        return self

    def concept_orders(self) -> dict[str, int]:
        """Return a mapping from concept name to semantic sort order."""
        return {
            concept: group.order
            for group in self.groups.values()
            for concept in group.concepts
        }

    def order_for(self, concept: str) -> int:
        """Return the configured order for a concept or the default order."""
        return self.concept_orders().get(concept, self.default_order)
