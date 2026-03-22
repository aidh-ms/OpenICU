from typing import Literal

from pydantic import BaseModel, Field

from open_icu.config.base import BaseDatasetConfig


class BaseTable(BaseModel):
    pass







class DerivedDatasetConceptConfig(BaseDatasetConfig):
    """Configuration for a derived dataset-specific concept.

    Inherits from BaseDatasetConfig and adds attributes specific to derived concepts.
    """
    __open_icu_config_type__ = "concept"

    type: Literal["derived"] = Field(
        "derived", description="Type of concept: 'base', 'derived', or 'complex'."
    )
