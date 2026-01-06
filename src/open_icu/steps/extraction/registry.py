"""Registry for dataset table configurations.

This module provides a singleton registry for managing TableConfig objects
used in the extraction step. The global dataset_config_registery instance
stores all loaded table configurations.
"""

from open_icu.config.registry import BaseConfigRegistry
from open_icu.steps.extraction.config.table import TableConfig


class DatasetConfigRegistry(BaseConfigRegistry[TableConfig]):
    """Registry for table configuration objects.

    Specialized registry for storing and retrieving TableConfig instances
    that define how to extract data from source tables and transform them
    into MEDS events.
    """

    pass

dataset_config_registery = DatasetConfigRegistry()
"""Global singleton instance of the dataset configuration registry."""
