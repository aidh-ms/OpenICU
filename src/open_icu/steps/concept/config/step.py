"""Concept step configuration models.

This module defines the configuration structure for the concept step,
including dataset path specifications and custom extraction settings.
"""
from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class CustomConfig(BaseModel):
    """Custom configuration specific to the concept step.

    Attributes:
        extraction_step: Name of the extraction step
    """

    extraction_step: str = Field(description="Name of the extraction step.")



class ConceptStepConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the concept step.

    Combines base step configuration with concept-specific settings.
    """

    pass
