import shutil

from open_icu.steps.base.step import BaseStep
from open_icu.steps.collect.config import CollectionStepConfig
from open_icu.steps.registery import register_step


@register_step
class CollectionStep(BaseStep[CollectionStepConfig]):

    def pre_run(self) -> None:
        super().pre_run()

        self._dataset = self._context.project.add_dataset(
            name=self._config.dataset.name,
            overwrite=self._config.dataset.overwrite,
        )

    def run(self) -> None:
        files = []

        for config in self._config.collecting:
            workspace_dir = self._context.project.workspace.get(config.name)
            if workspace_dir is None:
                raise ValueError(f"Workspace directory '{config.name}' not found in project.")

            # TODO: Add logic for filtering and deduplication (when overwriting)
            files += workspace_dir.content

        for file_path in files:
            relative_path = file_path.relative_to(workspace_dir._path)
            dest_path = self._dataset.data_path / relative_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # TODO: add option for symlinking
            shutil.copy(file_path, dest_path)
