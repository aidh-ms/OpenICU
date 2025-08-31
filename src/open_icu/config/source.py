from pydantic import BaseModel, Field

from open_icu.config.base import Config


class JoinConfig(BaseModel):
    table: str
    on: list[str] = Field(default_factory=list)
    left_on: list[str] = Field(default_factory=list)
    right_on: list[str] = Field(default_factory=list)
    how: str = "inner"


class FieldConfig(BaseModel):
    field: str
    type: str


class MEDSFieldsConfig(BaseModel):
    subject_id: FieldConfig
    time: FieldConfig
    code: list[FieldConfig] = Field(default_factory=list)
    numeric_value: FieldConfig | None = None
    text_value: FieldConfig | None = None


class EventConfig(BaseModel):
    name: str
    join: list[JoinConfig] = Field(default_factory=list)
    fields: MEDSFieldsConfig


class TableConfig(BaseModel):
    name: str
    events: list[EventConfig] = Field(default_factory=list)


class SourceConfig(Config):
    name: str
    version: str
    tables: list[TableConfig] = Field(default_factory=list)
