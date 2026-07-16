"""Table configuration models for data extraction.

This module defines configurations for source tables, including column definitions,
callback transformations, join specifications, and event extraction rules.
"""

from abc import ABCMeta
from enum import StrEnum, auto
from typing import Any, ClassVar

from polars.datatypes import DataTypeClass
from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator

from open_icu.config.base import BaseDatasetConfig
from open_icu.steps.extraction.config.column import ColumnConfig
from open_icu.steps.extraction.config.event import EventConfig, MEDSEventFieldDefaultConfig


def _get_or_default(data: dict[str, Any], key: str, default: Any) -> Any:
    """Return a configured value if present, otherwise return the default.

    This intentionally checks key existence instead of truthiness so that empty
    lists can explicitly override defaults.
    """
    return data[key] if key in data else default


class TableType(StrEnum):
    """Supported table file formats."""

    CSV = auto()
    CSVGZ = auto()
    PARQUET = auto()


# File-extension hints used to infer the table format when ``type`` is omitted.
# Parquet is the default for any path without a recognised extension.
_TYPE_BY_SUFFIX: list[tuple[tuple[str, ...], TableType]] = [
    ((".parquet", ".pq"), TableType.PARQUET),
    ((".csv.gz", ".csv.bz2", ".csv.zip", ".gz"), TableType.CSVGZ),
    ((".csv",), TableType.CSV),
]


def _infer_table_type(path: str) -> TableType:
    """Infer the table format from a file path, defaulting to Parquet."""
    lowered = path.lower()
    for suffixes, table_type in _TYPE_BY_SUFFIX:
        if lowered.endswith(suffixes):
            return table_type
    return TableType.PARQUET


class BaseTableConfig(BaseModel, metaclass=ABCMeta):
    """Abstract base configuration for table extraction.

    Defines columns, data types, callback transformations, filters, and frame
    transformations for reading and processing a source table.

    Attributes:
        path: File path to the table data relative to dataset root
        type: Table file format (parquet, csv, or csvgz); inferred from the path
            extension when omitted, defaulting to parquet
        columns: List of column configurations
        pre_callbacks: Callbacks to apply before column processing
        pre_filters: Filters to apply before column processing
        callbacks: Callbacks to apply after column processing
        filters: Filters to apply after callbacks
        post_join_callbacks: Callbacks to apply after joins
        post_join_filters: Filters to apply after post-join callbacks
        transformations: Frame transformations to apply after post-join filters
        dtypes: Computed dictionary mapping column names to Polars types
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    path: str = Field(..., description="The file path to the table data.")
    type: TableType = Field(
        TableType.PARQUET,
        description="The table file format. Defaults to Parquet; when omitted it is "
        "inferred from the path extension (.parquet/.csv/.csv.gz).",
    )
    columns: list[ColumnConfig] = Field(
        default_factory=list,
        description="The list of column configurations for the table.",
    )
    pre_callbacks: list[str] = Field(
        default_factory=list,
        description="The list of pre-processing callback configurations for the table.",
    )
    pre_filters: list[str] = Field(
        default_factory=list,
        description="The list of pre-processing filter configurations for the table.",
    )
    callbacks: list[str] = Field(
        default_factory=list,
        description="The list of callback configurations for the table.",
    )
    filters: list[str] = Field(
        default_factory=list,
        description="The list of filter configurations for the table.",
    )
    post_join_callbacks: list[str] = Field(
        default_factory=list,
        description="The list of callback configurations applied after joins.",
    )
    post_join_filters: list[str] = Field(
        default_factory=list,
        description="The list of filter configurations applied after post-join callbacks.",
    )
    transformations: list[str] = Field(
        default_factory=list,
        description="The list of frame transformation configurations for the table.",
    )

    @model_validator(mode="before")
    @classmethod
    def _default_type_from_path(cls, data: Any) -> Any:
        """Infer ``type`` from the path extension when it is not given explicitly."""
        if isinstance(data, dict) and not data.get("type") and data.get("path"):
            data = {**data, "type": _infer_table_type(str(data["path"]))}
        return data

    @computed_field
    @property
    def dtypes(self) -> dict[str, DataTypeClass]:
        dtype_map = {}
        for col in self.columns:
            dtype_map[col.name] = col.dtype

        return dtype_map


class JoinTableConfig(BaseTableConfig):
    """Configuration for a table to join with the main table.

    Extends BaseTableConfig with join specification parameters.

    Attributes:
        both_on: Columns to join on (same name in both tables)
        left_on: Columns in the left (main) table for the join
        right_on: Columns in the right (join) table for the join
        how: Join type ("left", "inner", "outer", "right")
        join_params: Computed dictionary of join parameters for Polars
    """

    both_on: list[str] = Field(
        default_factory=list,
        description="List of columns to be used for joining table on both sides.",
    )
    left_on: list[str] = Field(
        default_factory=list,
        description="List of columns to be used for joining table on the left side.",
    )
    right_on: list[str] = Field(
        default_factory=list,
        description="List of columns to be used for joining table on the right side.",
    )
    how: str = Field(
        "left",
        description="Type of join to be performed (e.g. inner, left, right, outer).",
    )

    @computed_field
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


class TableConfig(BaseDatasetConfig, BaseTableConfig):
    """Complete configuration for extracting MEDS events from a table.

    Combines BaseConfig (for versioning/identification) with BaseTableConfig
    (for table processing) and adds event extraction specifications. Event
    columns defaults can be specified to reduce repetition across events.

    Attributes:
        name: Human-readable name of the configuration
        version: Version string for the configuration
        identifier: Computed hierarchical identifier (e.g., "openicu.config.classname.version.name")
        identifier_tuple: Tuple of (class_name, dataset, version, name)
        uuid: UUID generated from the identifier
        dataset: Name of the dataset this table belongs to
        join: List of tables to join before event extraction
        events: List of MEDS events to extract from this table
    """

    __open_icu_config_type__: ClassVar[str] = "table"

    join: list[JoinTableConfig] = Field(
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
            event["code_prefix"] = _get_or_default(
                event,
                "code_prefix",
                event_defaults.code_prefix or [],
            )
            event["code_suffix"] = _get_or_default(
                event,
                "code_suffix",
                event_defaults.code_suffix or [],
            )

            event["columns"] = event_defaults.apply_defaults(event.get("columns", {}))

        data["events"] = events

        super().__init__(**data)

    @computed_field
    @property
    def identifier_tuple(self) -> tuple[str, ...]:
        return self.__open_icu_config_type__, self.dataset, self.version, self.name
