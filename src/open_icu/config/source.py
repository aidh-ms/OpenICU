
from pydantic import BaseModel, ConfigDict, Field, computed_field

from open_icu.config.base import Config


class MEDSFieldsConfig(BaseModel):
    subject_id: str
    time: str
    code: list[str] = Field(default_factory=list)
    numeric_value: str | None = None
    text_value: str | None = None
    extension: dict[str, str] = Field(default_factory=dict)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ext_field_names(self) -> list[str]:
        return list(self.extension.values())


class FilterConfig(BaseModel):
    dropna: list[str] = Field(default_factory=list)


class EventConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    fields: MEDSFieldsConfig
    filters: FilterConfig = Field(default_factory=FilterConfig)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def field_names(self) -> list[str]:
        return [*filter(None, [
            self.fields.subject_id,
            self.fields.time,
            *self.fields.code,
            self.fields.numeric_value,
            self.fields.text_value,
            *self.fields.ext_field_names
        ])]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def column_mapping(self) -> dict[str, str]:
        mappings = {}
        if self.fields.subject_id:
            mappings[self.fields.subject_id] = "subject_id"
        if self.fields.time:
            mappings[self.fields.time] = "time"
        if self.fields.numeric_value:
            mappings[self.fields.numeric_value] = "numeric_value"
        if self.fields.text_value:
            mappings[self.fields.text_value] = "text_value"

        mappings.update({v: k for k, v in self.fields.extension.items()})
        return mappings

    @computed_field  # type: ignore[prop-decorator]
    @property
    def column_order(self) -> list[str]:
        return [
            "subject_id",
            "time",
            "code",
            "numeric_value",
            "text_value",
            *sorted(self.fields.extension.keys())
        ]


class FieldConfig(BaseModel):
    field: str
    type: str
    constant: str | int | float | None = None


class CalcDatetimeFieldConfig(BaseModel):
    field: str
    year: FieldConfig
    month: FieldConfig
    day: FieldConfig
    time: FieldConfig
    offset: FieldConfig

    @computed_field  # type: ignore[prop-decorator]
    @property
    def table_field_dtypes(self) -> dict[str, str]:
        return {
            field.field: field.type
            for _, field in iter(self)
            if isinstance(field, FieldConfig) and field.constant is None
        }

    @computed_field  # type: ignore[prop-decorator]
    @property
    def table_constants(self) -> dict[str, str | int | float]:
        return {
            field.field: field.constant
            for _, field in iter(self)
            if isinstance(field, FieldConfig) and field.constant is not None
        }

class OffsetDatetimeFieldConfig(BaseModel):
    field: str
    base: FieldConfig
    offset: FieldConfig

    @computed_field  # type: ignore[prop-decorator]
    @property
    def table_field_dtypes(self) -> dict[str, str]:
        return {self.offset.field: self.offset.type}

    @computed_field  # type: ignore[prop-decorator]
    @property
    def table_constants(self) -> dict[str, str | int | float]:
        return {}


class JoinConfig(BaseModel):
    name: str
    path: str
    fields: list[FieldConfig | CalcDatetimeFieldConfig | OffsetDatetimeFieldConfig] = Field(default_factory=list)
    both_on: list[str] = Field(default_factory=list)
    left_on: list[str] = Field(default_factory=list)
    right_on: list[str] = Field(default_factory=list)
    how: str = "left"

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


class TableConfig(BaseModel):
    name: str
    path: str
    fields: list[FieldConfig | CalcDatetimeFieldConfig | OffsetDatetimeFieldConfig] = Field(default_factory=list)
    join: list[JoinConfig] = Field(default_factory=list)
    events: list[EventConfig] = Field(default_factory=list)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def table_field_dtypes(self) -> dict[str, dict[str, str]]:
        field_dtypes = {}
        for join_table in self.join:
            field_dtypes[join_table.name] = {field.field: field.type for field in join_table.fields if isinstance(field, FieldConfig) and field.constant is None}
        field_dtypes[self.name] = {field.field: field.type for field in self.fields if isinstance(field, FieldConfig) and field.constant is None}

        for filed in self.fields:
            if not isinstance(filed, CalcDatetimeFieldConfig):
                continue
            field_dtypes[self.name].update(filed.table_field_dtypes)

        for join_table in self.join:
            for filed in join_table.fields:
                if not isinstance(filed, CalcDatetimeFieldConfig):
                    continue
                field_dtypes[join_table.name].update(filed.table_field_dtypes)

        for filed in self.fields:
            if not isinstance(filed, OffsetDatetimeFieldConfig):
                continue
            field_dtypes[self.name].update(filed.table_field_dtypes)

        for join_table in self.join:
            for filed in join_table.fields:
                if not isinstance(filed, OffsetDatetimeFieldConfig):
                    continue
                field_dtypes[join_table.name].update(filed.table_field_dtypes)

        return field_dtypes

    @computed_field  # type: ignore[prop-decorator]
    @property
    def table_constants(self) -> dict[str, dict[str, str | int | float]]:
        constants: dict[str, dict[str, str | int | float]] = {}
        for join_table in self.join:
            constants[join_table.name] = {field.field: field.constant for field in join_table.fields if isinstance(field, FieldConfig) and field.constant is not None}
        constants[self.name] = {field.field: field.constant for field in self.fields if isinstance(field, FieldConfig) and field.constant is not None}

        for filed in self.fields:
            if not isinstance(filed, CalcDatetimeFieldConfig):
                continue
            constants[self.name].update(filed.table_constants)

        for join_table in self.join:
            for filed in join_table.fields:
                if not isinstance(filed, CalcDatetimeFieldConfig):
                    continue
                constants[join_table.name].update(filed.table_constants)

        return constants

    @computed_field  # type: ignore[prop-decorator]
    @property
    def calc_datetime_fields(self) -> dict[str, list[CalcDatetimeFieldConfig]]:
        datetime_fields: dict[str, list[CalcDatetimeFieldConfig]] = {}
        for join_table in self.join:
            datetime_fields[join_table.name] = [field for field in join_table.fields if isinstance(field, CalcDatetimeFieldConfig)]
        datetime_fields[self.name] = [field for field in self.fields if isinstance(field, CalcDatetimeFieldConfig)]

        return datetime_fields

    @computed_field  # type: ignore[prop-decorator]
    @property
    def offset_datetime_fields(self) -> dict[str, list[OffsetDatetimeFieldConfig]]:
        datetime_fields: dict[str, list[OffsetDatetimeFieldConfig]] = {}
        for join_table in self.join:
            datetime_fields[join_table.name] = [field for field in join_table.fields if isinstance(field, OffsetDatetimeFieldConfig)]
        datetime_fields[self.name] = [field for field in self.fields if isinstance(field, OffsetDatetimeFieldConfig)]

        return datetime_fields


class SourceConfig(Config):
    name: str
    version: str
    tables: list[TableConfig] = Field(default_factory=list)
