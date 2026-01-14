"""Extraction step configuration models.

This module defines the configuration structure for the extraction step,
including dataset path specifications and custom extraction settings.
"""
from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class CustomConfig(BaseModel):
    """Custom configuration specific to the extraction step.

    Attributes:
        extraction: Name of the extraction step
    """

    extraction: str = Field(description="Name of the extraction step.")



class ConceptConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the extraction step.

    Combines base step configuration with extraction-specific settings.
    """

    pass
