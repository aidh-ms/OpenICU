
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from open_icu.config.dataset.source.config.callback import CallbackConfig
from open_icu.config.dataset.source.config.base import OpenICUBaseModel


class MEDSEventFieldConfig(OpenICUBaseModel):
    subject_id: str = Field(..., description="The subject identifier field name.")
    time: str = Field(..., description="The timestamp field name.")
    code: list[str] = Field(default_factory=list, description="The code field name.")
    numeric_value: str | None = Field(None, description="The numeric value field name.")
    text_value: str | None = Field(None, description="The text value field name.")
    extension: dict[str, str] = Field(default_factory=dict, description="The extension field name mapping.")

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "subject_id" : self.subject_id,
            "time" : self.time,
            "code": self.code,
            "numeric_value" : self.numeric_value,
            "text_value" : self.text_value,
            "extension" : self.extension,
        }

    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "subject_id" : self.subject_id,
            "time" : self.time,
            "code_count": len(self.code),
            "numeric_value" : self.numeric_value,
            "text_value" : self.text_value,
            "extension_count" : len(self.extension),
        }


class MEDSEventFieldDefaultConfig(OpenICUBaseModel):
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
    
    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "subject_id" : self.subject_id,
            "time" : self.time,
            "code" : self.code or [],
            "numeric_value" : self.numeric_value,
            "text_value": self.text_value,
            "extension": self.extension or {},
            "code_prefix": self.code_prefix or [],
            "code_suffix": self.code_suffix or [],
        }
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "subject_id" : self.subject_id,
            "time" : self.time,
            "code_count" : len(self.code or []),
            "numeric_value" : self.numeric_value,
            "text_value": self.text_value,
            "extension_count": len(self.extension or {}),
            "code_prefix_count": len(self.code_prefix or []),
            "code_suffix_count": len(self.code_suffix or []),
        }


class EventConfig(OpenICUBaseModel):
    name: str = Field(..., description="The name of the event.")
    fields: MEDSEventFieldConfig = Field(..., description="The field configuration for the event.")
    callbacks: list[CallbackConfig] = Field(
        default_factory=list, description="The list of callback configurations for the event."
    )

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "name": self.name,
            "fields": self.fields.to_dict(),
            "callbacks": [c.to_dict() for c in self.callbacks]
        }
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "name": self.name,
            "fields_summary": self.fields.summary(),
            "callbacks_count": len(self.callbacks)
        }
