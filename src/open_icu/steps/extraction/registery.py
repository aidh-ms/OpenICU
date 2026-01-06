"""Registry for dataset table configurations.

This module provides a singleton registry for managing TableConfig objects
used in the extraction step. The global dataset_config_registery instance
stores all loaded table configurations.
"""

from open_icu.config.registery import BaseConfigRegistery
from open_icu.steps.extraction.config.table import TableConfig


class DatasetConfigRegistery(BaseConfigRegistery[TableConfig]):
    """Registry for table configuration objects.

    Specialized registry for storing and retrieving TableConfig instances
    that define how to extract data from source tables and transform them
    into MEDS events.
    """

    pass

dataset_config_registery = DatasetConfigRegistery()
"""Global singleton instance of the dataset configuration registry."""
