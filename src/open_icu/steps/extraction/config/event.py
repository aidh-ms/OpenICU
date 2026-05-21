"""Event configuration models for MEDS column mappings.

This module defines Pydantic models for configuring how source table columns
map to MEDS event columns (subject_id, time, code, numeric_value, text_value).
"""

from typing import Any

from pydantic import BaseModel, Field


def _get_or_default(data: dict[str, Any], key: str, default: Any) -> Any:
    """Return an event-specific value if present, otherwise return the default.

    This intentionally checks key existence instead of truthiness so that empty
    lists can explicitly override defaults.
    """
    return data[key] if key in data else default


class MEDSEventFieldConfig(BaseModel):
    """Field mapping configuration for a MEDS event.

    Specifies which source columns map to each MEDS standard column. The code
    column can be a list of additional code parts that will be concatenated with
    "//" separator after the event name.

    Attributes:
        subject_id: Column name for subject/patient identifier
        time: Column name for event timestamp
        code: List of additional code parts to append after the event name
        numeric_value: Column name for numeric measurement value (optional)
        text_value: Column name for text value (optional)
        extension: Dictionary mapping MEDS extension columns to source columns
    """

    subject_id: str = Field(..., description="The subject identifier column name.")
    time: str = Field(..., description="The timestamp column name.")
    code: list[str] = Field(
        default_factory=list,
        description="Additional code parts appended after the event name.",
    )
    numeric_value: str | None = Field(None, description="The numeric value column name.")
    text_value: str | None = Field(None, description="The text value column name.")
    extension: dict[str, str] = Field(
        default_factory=dict,
        description="The extension column name mapping.",
    )


class MEDSEventFieldDefaultConfig(BaseModel):
    """Default column mapping configuration for events in a table.

    Provides default values for column mappings that can be inherited by
    individual events. Supports prefixes and suffixes for code construction.

    Attributes:
        subject_id: Default column name for subject identifier
        time: Default column name for timestamp
        code: Default list of additional code parts appended after the event name
        code_prefix: Code parts inserted after automatic db/table and before event name
        code_suffix: Code parts appended after event-specific code parts
        numeric_value: Default column name for numeric value
        text_value: Default column name for text value
        extension: Default extension column mappings
    """

    subject_id: str | None = Field(
        None,
        description="The default subject identifier column name.",
    )
    time: str | None = Field(
        None,
        description="The default timestamp column name.",
    )
    code: list[str] | None = Field(
        None,
        description="Default additional code parts appended after the event name.",
    )
    code_prefix: list[str] | None = Field(
        None,
        description="Default code parts inserted after automatic db/table and before event name.",
    )
    code_suffix: list[str] | None = Field(
        None,
        description="Default code parts appended after event-specific code parts.",
    )
    numeric_value: str | None = Field(
        None,
        description="The default numeric value column name.",
    )
    text_value: str | None = Field(
        None,
        description="The default text value column name.",
    )
    extension: dict[str, str] | None = Field(
        None,
        description="The default extension column name mapping.",
    )

    def apply_defaults(self, event_column_config: dict[str, Any]) -> dict[str, Any]:
        """Apply default column mappings to an event configuration.

        Merges the default column mappings with event-specific mappings, with
        event-specific values taking precedence.

        This method only applies defaults for MEDS column mappings. It does not
        merge code_prefix or code_suffix into columns.code, because the final
        code is built later as:

            db_name // table_name // code_prefix // event.name // columns.code // code_suffix

        Args:
            event_column_config: Event-specific column configuration dictionary

        Returns:
            Merged column configuration with defaults applied
        """
        return {
            "subject_id": _get_or_default(event_column_config, "subject_id", self.subject_id),
            "time": _get_or_default(event_column_config, "time", self.time),
            "code": _get_or_default(event_column_config, "code", self.code or []),
            # "code_prefix": _get_or_default(
            #     event_column_config,
            #     "code_prefix",
            #     self.code_prefix or [],
            # ),
            # "code_suffix": _get_or_default(
            #     event_column_config,
            #     "code_suffix",
            #     self.code_suffix or [],
            # ),
            "numeric_value": _get_or_default(
                event_column_config,
                "numeric_value",
                self.numeric_value,
            ),
            "text_value": _get_or_default(
                event_column_config,
                "text_value",
                self.text_value,
            ),
            "extension": {
                **(self.extension or {}),
                **(event_column_config.get("extension") or {}),
            },
        }


class EventConfig(BaseModel):
    """Configuration for a single MEDS event to extract from a table.

    Defines an event type to extract, including its name, optional code
    prefix/suffix parts, column mappings, and optional callbacks to apply before
    writing.

    Attributes:
        name: Name of the event (used in output filename and code construction)
        code_prefix: Code parts inserted after automatic db/table and before event name
        code_suffix: Code parts appended after event-specific code parts
        columns: Column mapping configuration for this event
        pre_callbacks: Callbacks to apply before MEDS column mapping
        callbacks: Callbacks to apply after MEDS column mapping and code construction
        filters: Filters to apply after event callbacks
        transformations: LazyFrame transformations to apply after event filters
        output_filters: Filters to apply after selecting final MEDS output columns
    """

    name: str = Field(..., description="The name of the event.")
    code_prefix: list[str] = Field(
        default_factory=list,
        description="Code parts inserted after automatic db/table and before event name.",
    )
    code_suffix: list[str] = Field(
        default_factory=list,
        description="Code parts appended after event-specific code parts.",
    )
    columns: MEDSEventFieldConfig = Field(
        ...,
        description="The column configuration for the event.",
    )
    pre_callbacks: list[str] = Field(
        default_factory=list,
        description="The list of callback configurations applied before MEDS column mapping.",
    )
    callbacks: list[str] = Field(
        default_factory=list,
        description="The list of callback configurations for the event.",
    )
    filters: list[str] = Field(
        default_factory=list,
        description="The list of filter configurations for the event.",
    )
    transformations: list[str] = Field(
        default_factory=list,
        description="The list of transformation configurations for the event.",
    )
    output_filters: list[str] = Field(
        default_factory=list,
        description="The list of output filter configurations for the event.",
    )