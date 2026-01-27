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

    """
    dataset: str | None = Field(None, description="Dataset name to match.")
    version: str | None = Field(None, description="Dataset version to match.")
    table: str | None = Field(None, description="Table name to match.")
    event: str | None = Field(None, description="Event name to match.")
    code: str | None = Field(None, description="Code value to match.")
    regex: str | None = Field(None, description="Regular expression pattern to match.")



class MappingConfig(BaseModel):
    pattern: MappingPatternConfig = Field(..., description="Pattern configuration for concept mapping.")
    columns: MappingColumnConfig = Field(..., description="Column configuration for concept mapping.")

    @computed_field
    @property
    def regex(self) -> str:
        """Returns the regex pattern if defined in the mapping pattern."""
        if self.pattern.regex is not None:
            return self.pattern.regex

        return "//".join(
            (
                self.pattern.dataset or "(.+?)",
                self.pattern.version or "(.+?)",
                self.pattern.table or "(.+?)",
                self.pattern.event or "(.+?)",
                self.pattern.code or "(.+?)",
            )
        )
