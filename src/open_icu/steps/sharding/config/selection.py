from pydantic import BaseModel, Field


class SelectionConfig(BaseModel):
    """Generic selection config for filtering entities such as concepts or subjects."""

    include: list[str] = Field(
        default_factory=list,
        description="Items to explicitly include.",
    )

    include_all: bool = Field(
        default=False,
        description="Whether to start from all available items.",
    )

    exclude: list[str] = Field(
        default_factory=list,
        description="Items to exclude.",
    )
