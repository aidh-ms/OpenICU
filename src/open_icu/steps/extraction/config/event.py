"""Event configuration models for MEDS field mappings.

This module defines Pydantic models for configuring how source table columns
map to MEDS event fields (subject_id, time, code, numeric_value, text_value).
"""

from typing import Any

from pydantic import BaseModel, Field


class MEDSEventFieldConfig(BaseModel):
    """Field mapping configuration for a MEDS event.

    Specifies which source columns map to each MEDS standard field. The code
    field can be a list of columns that will be concatenated with "//" separator.

    Attributes:
        subject_id: Column name for subject/patient identifier
        time: Column name for event timestamp
        code: List of column names to concatenate for the event code
        numeric_value: Column name for numeric measurement value (optional)
        text_value: Column name for text value (optional)
        extension: Dictionary mapping MEDS extension fields to source columns
    """
    subject_id: str = Field(..., description="The subject identifier field name.")
    time: str = Field(..., description="The timestamp field name.")
    code: list[str] = Field(default_factory=list, description="The code field name.")
    numeric_value: str | None = Field(None, description="The numeric value field name.")
    text_value: str | None = Field(None, description="The text value field name.")
    extension: dict[str, str] = Field(default_factory=dict, description="The extension field name mapping.")


class MEDSEventFieldDefaultConfig(BaseModel):
    """Default field mapping configuration for events in a table.

    Provides default values for field mappings that can be inherited by
    individual events. Supports prefixes and suffixes for code fields.

    Attributes:
        subject_id: Default column name for subject identifier
        time: Default column name for timestamp
        code: Default list of column names for code
        numeric_value: Default column name for numeric value
        text_value: Default column name for text value
        extension: Default extension field mappings
        code_prefix: Columns to prepend to event code lists
        code_suffix: Columns to append to event code lists
    """
    subject_id: str | None = Field(None, description="The default subject identifier field name.")
    time: str | None = Field(None, description="The default timestamp field name.")
    code: list[str] | None = Field(None, description="The default code field name.")
    numeric_value: str | None = Field(None, description="The default numeric value field name.")
    text_value: str | None = Field(None, description="The default text value field name.")
    extension: dict[str, str] | None = Field(None, description="The default extension field name mapping.")
    code_prefix: list[str] | None = Field(
        None, description="List of default prefixes to be added to the code field."
    )
    code_suffix: list[str] | None = Field(
        None, description="List of suffixes to be added to the code field."
    )

    def apply_defaults(self, event_field_config: dict[str, Any]) -> dict[str, Any]:
        """Apply default field mappings to an event configuration.

        Merges the default field mappings with event-specific mappings, with
        event-specific values taking precedence. Adds code prefixes and suffixes.

        Args:
            event_field_config: Event-specific field configuration dictionary

        Returns:
            Merged field configuration with defaults applied
        """
        code = event_field_config.get("code") or self.code or []
        if self.code_prefix:
            code = self.code_prefix + code
        if self.code_suffix:
            code = code + self.code_suffix

        return {
            "subject_id": event_field_config.get("subject_id") or self.subject_id,
            "time": event_field_config.get("time") or self.time,
            "code": code,
            "numeric_value": event_field_config.get("numeric_value") or self.numeric_value,
            "text_value": event_field_config.get("text_value") or self.text_value,
            "extension": event_field_config.get("extension") or self.extension or {},
        }


class EventConfig(BaseModel):
    """Configuration for a single MEDS event to extract from a table.

    Defines an event type to extract, including its name, field mappings,
    and optional callbacks to apply before writing.

    Attributes:
        name: Name of the event (used in output filename)
        fields: Field mapping configuration for this event
        callbacks: List of callbacks to apply to the event data
    """
    name: str = Field(..., description="The name of the event.")
    fields: MEDSEventFieldConfig = Field(..., description="The field configuration for the event.")
    callbacks: list[str] = Field(
        default_factory=list, description="The list of callback configurations for the event."
    )
