from pathlib import Path

from open_icu.pipeline.config import PipelineConfig
from open_icu.pipeline.context import PipelineContext


class Pipeline:
    def __init__(self, config: PipelineConfig) -> None:
        self._config = config

        self._context = PipelineContext(
            project=self._config.project.project,
        )

        self._steps = [
            step_cfg.create_step(self._context)
            for step_cfg in self._config.steps
        ]

    @classmethod
    def load(cls, path: Path) -> "Pipeline":
        config = PipelineConfig.load(path)
        return cls(config)

    def setup(self) -> None:
        for step in self._steps:
            step.setup()

    def teardown(self) -> None:
        for step in self._steps:
            step.teardown()

    def run(self) -> None:
        self.setup()

        for step in self._steps:
            step.pre_run()
            step.run()
            step.post_run()

        self.teardown()
