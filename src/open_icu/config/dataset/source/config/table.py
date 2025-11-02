from abc import ABCMeta
from enum import StrEnum, auto
from typing import Any

from pydantic import BaseModel, Field

from open_icu.config.dataset.source.config.callback import CallbackConfig
from open_icu.config.dataset.source.config.event import EventConfig, MEDSEventFieldDefaultConfig
from open_icu.config.dataset.source.config.field import ConstantFieldConfig, FieldConfig


class TableType(StrEnum):
    CSV = auto()


class BaseTableConfig(BaseModel, metaclass=ABCMeta):
    name: str = Field(..., description="The name of the table.")
    path: str = Field(..., description="The file path to the table data.")
    type: TableType = Field(TableType.CSV, description="The type of the table (e.g. csv, json).")
    fields: list[ConstantFieldConfig |FieldConfig] = Field(
        default_factory=list,
        description="The list of field configurations for the table."
    )
    callbacks: list[CallbackConfig] = Field(
        default_factory=list, description="The list of callback configurations for the table."
    )


class JsonTableConfig(BaseTableConfig):
    both_on: list[str] = Field(
        default_factory=list,
        description="List of fields to be used for joining table on both sides.",
    )
    left_on: list[str] = Field(
        default_factory=list,
        description="List of fields to be used for joining table on the left side.",
    )
    right_on: list[str] = Field(
        default_factory=list,
        description="List of fields to be used for joining table on the right side.",
    )
    how: str = Field(
        "left",
        description="Type of join to be performed (e.g. inner, left, right, outer).",
    )


class TableConfig(BaseTableConfig):
    join: list[JsonTableConfig] = Field(
        default_factory=list,
        description="List of join configurations for joining tables.",
    )
    events: list[EventConfig] = Field(
        default_factory=list,
        description="List of event configurations associated with the table.",
    )

    def __init__(self, **data: Any) -> None:
        event_defaults = MEDSEventFieldDefaultConfig(**data.get("event_defaults", {}))

        events = data.pop("events", [])
        for event in events:
            event["fields"] = event_defaults.apply_defaults(event.get("fields", {}))
        data["events"] = events

        super().__init__(**data)
