from abc import ABCMeta
from enum import StrEnum, auto
from typing import Any

from polars.datatypes import DataTypeClass
from pydantic import BaseModel, ConfigDict, Field, computed_field

from open_icu.config.base import BaseConfig
from open_icu.steps.extraction.config.callback import CallbackConfig
from open_icu.steps.extraction.config.event import EventConfig, MEDSEventFieldDefaultConfig
from open_icu.steps.extraction.config.field import ConstantFieldConfig, FieldConfig


class TableType(StrEnum):
    CSV = auto()


class BaseTableConfig(BaseModel, metaclass=ABCMeta):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    path: str = Field(..., description="The file path to the table data.")
    type: TableType = Field(TableType.CSV, description="The type of the table (e.g. csv, json).")
    fields: list[ConstantFieldConfig | FieldConfig] = Field(
        default_factory=list,
        description="The list of field configurations for the table."
    )
    pre_callbacks: list[CallbackConfig] = Field(
        default_factory=list, description="The list of pre-processing callback configurations for the table."
    )
    callbacks: list[CallbackConfig] = Field(
        default_factory=list, description="The list of callback configurations for the table."
    )
    post_callbacks: list[CallbackConfig] = Field(
        default_factory=list, description="The list of post-processing callback configurations for the table."
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def dtypes(self) -> dict[str, DataTypeClass]:
        dtype_map = {}
        for field in self.fields:
            if isinstance(field, ConstantFieldConfig):
                continue

            dtype_map[field.name] = field.dtype

        return dtype_map


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

    @computed_field  # type: ignore[prop-decorator]
    @property
    def join_params(self) -> dict[str, list[str]]:
        params = {}
        if self.both_on:
            params["on"] = self.both_on
        if self.left_on:
            params["left_on"] = self.left_on
        if self.right_on:
            params["right_on"] = self.right_on

        return params


class TableConfig(BaseConfig, BaseTableConfig):
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
