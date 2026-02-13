from pydantic import BaseModel, Field, computed_field


class MappingColumnConfig(BaseModel):
    """Configuration for a concept mapping.

    Attributes:
        numeric_value: Column name for numeric values
        text_value: Column name for text values
    """
    numeric_value: str | None = Field(None, description="Column name for numeric values.")
    text_value: str | None = Field(None, description="Column name for text values.")


class MappingPatternConfig(BaseModel):
    """Configuration for concept mapping patterns.

    Attributes:
        dataset: Dataset name to match.
        version: Dataset version to match.
        table: Table name to match.
        event: Event name to match.
        code: Code value to match.
        regex: Regular expression pattern to match.
    """
    dataset: str | None = Field(None, description="Dataset name to match.")
    version: str | None = Field(None, description="Dataset version to match.")
    table: str | None = Field(None, description="Table name to match.")
    event: str | None = Field(None, description="Event name to match.")
    code: str | None = Field(None, description="Code value to match.")
    unit: str | None = Field(None, description="unit to match.")
    regex: str | None = Field(None, description="Regular expression pattern to match.")



class MappingConfig(BaseModel):
    """Configuration for a concept mapping.

    Attributes:
        pattern: Pattern configuration for concept mapping.
        columns: Column configuration for concept mapping.
        filters: The list of filter configurations for the mapping.
    """
    pattern: MappingPatternConfig = Field(..., description="Pattern configuration for concept mapping.")
    columns: MappingColumnConfig = Field(..., description="Column configuration for concept mapping.")

    @computed_field
    @property
    def regex(self) -> str:
        """Returns the regex pattern if defined in the mapping pattern."""
        if self.pattern.regex is not None:
            return self.pattern.regex

        regex_parts = [
            self.pattern.dataset or "(.+?)",
            self.pattern.version or "(.+?)",
            self.pattern.table or "(.+?)",
            self.pattern.event or "(.+?)",
            self.pattern.code or "(.+?)",
        ]
        if self.pattern.unit:
            regex_parts += [self.pattern.unit]

        return "(?i)" + "//".join(regex_parts)

    filters: list[str] = Field(
        default_factory=list, description="The list of filter configurations for the mapping."
    )
