from importlib.resources import files
from pathlib import Path

from open_icu.config.registry import load_configs
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.registry import concept_config_registry
from open_icu.steps.extraction.registry import dataset_config_registry


def auto_load_configs():
    """
    Automatically load concept and dataset configurations from the package's
    'configs' directory. This function searches for concept and dataset
    configuration files in the package's 'configs' directory and registers
    them with the appropriate registries.
    """

    module_path = Path(str(files("open_icu")))
    config_path = module_path.parent.parent / "configs"

    if not (config_path.exists() and config_path.is_dir()):
        return

    mapping_paths = []
    for dataset_path in (config_path / "datasets").iterdir():
        for version_path in dataset_path.iterdir():
            if version_path.is_dir():
                dataset_config_registry.load(version_path / "tables")
                mapping_paths.append(version_path / "mappings")

    concepts = load_configs(
        config_path / "concepts",
        config_type=ConceptConfig,

        dataset_paths=mapping_paths,
    )
    for concept in concepts:
        concept_config_registry.register(concept)
