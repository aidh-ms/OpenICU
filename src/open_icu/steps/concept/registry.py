"""Registry for concept configurations.

This module provides a singleton registry for managing ConceptConfig objects
used in the concept step. The global concept_config_registry instance
stores all loaded concept configurations.
"""

from open_icu.config.registry import BaseConfigRegistry
from open_icu.steps.concept.config.concept import ConceptConfig


class ConceptConfigRegistry(BaseConfigRegistry[ConceptConfig]):
    """Registry for concept configuration objects.

    Specialized registry for storing and retrieving ConceptConfig instances
    that define how to extract data from source tables and transform them
    into MEDS events.
    """

    pass

concept_config_registry = ConceptConfigRegistry()
"""Global singleton instance of the concept configuration registry."""
