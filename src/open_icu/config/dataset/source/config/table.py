from abc import ABCMeta
from enum import StrEnum, auto
from typing import Any, Dict, List

from polars.datatypes import DataTypeClass
from pydantic import ConfigDict, Field, computed_field

from open_icu.config.dataset.source.config.callback import CallbackConfig
from open_icu.config.dataset.source.config.dtype import DTYPES
from open_icu.config.dataset.source.config.event import EventConfig, MEDSEventFieldDefaultConfig
from open_icu.config.dataset.source.config.field import ConstantFieldConfig, FieldConfig
from open_icu.config.dataset.source.config.base import OpenICUBaseModel


class TableType(StrEnum):
    CSV = auto()


class BaseTableConfig(OpenICUBaseModel, metaclass=ABCMeta):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(..., description="The name of the table.")
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

            dtype_map[field.name] = DTYPES[field.type]

        return dtype_map

    def to_dict(self)-> Dict[str, Any] | str | List[Any]:
        return {
            "name" : self.name,
            "path" : self.path,
            "type" : self.type,
            "fields" : [field.to_dict() for field in self.fields],
            "pre_callbacks" : [pre_callback.to_dict() for pre_callback in  self.pre_callbacks],
            "callbacks"  : [callback.to_dict() for callback in self.callbacks],
            "post_callbacks" : [post_callback.to_dict() for post_callback in self.post_callbacks]
        }
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "name" : self.name,
            "path" : self.path,
            "type" : self.type,
            "fields_count" : len(self.fields),
            "pre_callbacks_count" : len(self.pre_callbacks),
            "callbacks_count"  : len(self.callbacks),
            "post_callbacks_count" : len(self.post_callbacks),
        }
    



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
    
    def to_dict(self)-> Dict[str, Any] | str | List[Any]:
        return super().to_dict() | {    # type: ignore
            "both_on" : self.both_on,
            "left_on" : self.left_on,
            "right_on" : self.right_on or [],
            "how": self.how,
        }      
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return super().summary() | {    # type: ignore
            "both_on_count" : len(self.both_on),
            "left_on_count" : len(self.left_on),
            "right_on_count" : len(self.right_on),
            "how": self.how,
        }



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
    
    def to_dict(self)-> Dict[str, Any] | str | List[Any]:
        return super().to_dict() | {    # type: ignore
            "join" : [json_table_config.to_dict() for json_table_config in self.join],
            "events" :  [event.to_dict() for event in self.events]
        }
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return super().summary() | {    # type: ignore
            "join_count" : len(self.join),
            "events_count" : len(self.events)
        }
