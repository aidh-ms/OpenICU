
from typing import Any

from pydantic import BaseModel, Field

from open_icu.config.dataset.source.config.callback import CallbackConfig


class MEDSEventFieldConfig(BaseModel):
    subject_id: str = Field(..., description="The subject identifier field name.")
    time: str = Field(..., description="The timestamp field name.")
    code: list[str] = Field(default_factory=list, description="The code field name.")
    numeric_value: str | None = Field(None, description="The numeric value field name.")
    text_value: str | None = Field(None, description="The text value field name.")
    extension: dict[str, str] = Field(default_factory=dict, description="The extension field name mapping.")


class MEDSEventFieldDefaultConfig(BaseModel):
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
    name: str = Field(..., description="The name of the event.")
    fields: MEDSEventFieldConfig = Field(..., description="The field configuration for the event.")
    callbacks: list[CallbackConfig] = Field(
        default_factory=list, description="The list of callback configurations for the event."
    )
