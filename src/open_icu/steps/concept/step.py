"""Extraction step implementation for converting ICU data to MEDS format.

This module implements the ExtractionStep class that orchestrates the extraction
of data from source CSV files, applies transformations via callbacks, performs
joins, and outputs MEDS-compliant Parquet files.
"""

from pathlib import Path

from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.config.step import ConceptStepConfig
from open_icu.steps.concept.registry import concept_config_registry
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)


class ConceptStep(ConfigurableBaseStep[ConceptStepConfig, ConceptConfig]):
    """Data extraction step for transforming source ICU data to MEDS format.

    Reads CSV files specified in TableConfig objects, applies pre/post callbacks,
    performs table joins, extracts events with field mappings, and writes
    MEDS-compliant Parquet files to the workspace directory.
    """
    @classmethod
    def load(cls, project: OpenICUProject, config_path: Path) -> "ConceptStep":
        """Load an extraction step from a configuration file.

        Args:
            project: The OpenICU project to operate within
            config_path: Path to the extraction configuration YAML file

        Returns:
            An initialized ExtractionStep instance
        """
        config = ConceptStepConfig.load(config_path)
        return cls(project, config, concept_config_registry)


    def extract(self) -> None:
        pass
