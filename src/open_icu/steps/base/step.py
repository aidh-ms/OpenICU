"""Abstract base class for configurable processing steps.

This module defines the core abstraction for processing steps in OpenICU's
data pipeline, providing a template for extraction, transformation, and
dataset generation workflows.
"""

import shutil
from abc import ABCMeta, abstractmethod
from pathlib import Path

from open_icu.metrics import get_metrics
from open_icu.config.base import BaseConfig
from open_icu.config.registry import BaseConfigRegistry
from open_icu.steps.base.config import BaseStepConfig
from open_icu.storage.project import OpenICUProject
from open_icu.storage.workspace import WorkspaceDir

metrics = get_metrics()


class ConfigurableBaseStep[SCT: BaseStepConfig, CT: BaseConfig](metaclass=ABCMeta):
    """Abstract base class for configurable data processing steps.

    Provides a standardized workflow for data processing steps including:
    - Configuration loading and management
    - Workspace and dataset directory setup
    - Data extraction (abstract method to be implemented by subclasses)
    - Post-processing hooks
    - Result collection into MEDS datasets

    Type Parameters:
        SCT: Step configuration type (must inherit from BaseStepConfig)
        CT: Configuration type for registries (must inherit from BaseConfig)

    Attributes:
        _project: The OpenICU project containing this step
        _config: Configuration for this step
        _registery: Configuration registry for loading external configs
        _workspace_dir: Workspace directory for intermediate files
        _dataset: Output dataset for final results
        _step_name: Normalized name of this step (lowercase)
    """
    def __init__(self, project: OpenICUProject, config: SCT, registry: BaseConfigRegistry[CT]) -> None:
        """Initialize the processing step.

        Args:
            project: The OpenICU project to operate within
            config: Configuration for this step
            registery: Configuration registry for loading external configs
        """
        self._project = project
        self._config = config
        self._registry = registry
        self._workspace_dir = None
        self._dataset = None
        self._step_name = self._config.name.lower()

    @classmethod
    @abstractmethod
    def load(cls, project: OpenICUProject, config_path: Path) -> "ConfigurableBaseStep[SCT, CT]":
        """Load a step instance from a configuration file.

        Args:
            project: The OpenICU project to operate within
            config_path: Path to the step configuration file

        Returns:
            An initialized step instance
        """
        pass

    @abstractmethod
    def extract(self) -> None:
        """Execute the core data extraction logic.

        This method must be implemented by subclasses to perform the actual
        data extraction, transformation, and writing to the workspace directory.
        """
        pass

    def run(self) -> WorkspaceDir:
        """Execute the complete step workflow.

        Orchestrates the full processing pipeline:
        1. Load and save configurations
        2. Set up workspace and dataset directories
        3. Execute extraction (if not skipping due to existing output)
        4. Run post-processing hooks
        5. Collect results into the dataset

        Returns:
            The workspace directory containing intermediate results

        Note:
            Skip execution if overwrite=False and both workspace and dataset exist
        """
        skip = (
            not self._config.overwrite
            and (self._project.workspace_path / self._step_name).exists()
            and (self._project.datasets_path / self._step_name).exists()
        )

        self.setup_config()
        self.setup_project()
        if not skip:
            self.extract()
            self.hooks()
            self.collect()

        assert isinstance(self._workspace_dir, WorkspaceDir)
        return self._workspace_dir

    def setup_config(self) -> None:
        """Load external configuration files into the registry.

        Processes each ConfigFileConfig from the step configuration, loading
        YAML files into the registry with specified filtering and overwrite
        behavior. Saves the consolidated configuration to the project's
        configs directory.
        """
        for config in self._config.config_files:
            self._registry.load(
                config.path,
                overwrite=config.overwrite,
                includes=config.includes,
                excludes=config.excludes
            )
        metrics.set(self._registry.keys())
        self._registry.save(self._project.configs_path)

    def setup_project(self) -> None:
        """Create workspace and dataset directories for this step.

        Initializes the workspace directory (for intermediate files) and
        dataset directory (for final MEDS output) within the project structure.
        """
        self._workspace_dir = self._project.add_workspace_dir(
            name=self._step_name,
            overwrite=self._config.overwrite,
        )

        self._dataset = self._project.add_dataset(
            name=self._step_name,
            overwrite=self._config.overwrite,
        )

    def hooks(self) -> None:
        """Execute post-extraction hooks.

        Placeholder for running registered hooks after extraction completes.
        Currently not implemented.
        """
        # TODO run hooks from registery after extraction
        pass

    def collect(self) -> None:
        """Collect workspace results into the final MEDS dataset.

        Copies all Parquet files from the workspace directory to the dataset's
        data directory, then writes dataset metadata and code vocabulary files
        to complete the MEDS-compliant output.
        """
        if self._workspace_dir is None or self._dataset is None:
            return

        for file_path in self._workspace_dir.content:
            relative_path = file_path.relative_to(self._workspace_dir._path)
            dest_path = self._dataset.data_path / relative_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            shutil.copy(file_path, dest_path)

        self._dataset.write_metadata(self._config.dataset.metadata)
        self._dataset.write_codes()
