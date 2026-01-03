from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field

from open_icu.config.base import BaseConfig
from open_icu.pipeline.context import PipelineContext
from open_icu.steps.base.step import BaseStep
from open_icu.steps.registery import registery
from open_icu.storage.project import OpenICUProject


class PipelineStepConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(..., description="Name of the step.")
    config: dict[str, Any] = Field(
        default_factory=dict, description="Configuration parameters for the step."
    )

    def create_step(self, context: PipelineContext) -> BaseStep:
        step_cls = registery.get(self.name)
        if step_cls is None:
            raise ValueError(f"Step '{self.name}' is not registered.")
        return step_cls(context, self.config)


class PipelineProjectConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    path: Path = Field(..., description="Path to the OpenICU project.")
    overwrite: bool = Field(
        default=False, description="Whether to overwrite existing project data."
    )

    @computed_field
    @property
    def project(self) -> OpenICUProject:
        return OpenICUProject(
            self.path,
            overwrite=self.overwrite
        )


class PipelineConfig(BaseConfig):
    project: PipelineProjectConfig = Field(..., description="Configuration for the OpenICU project.")
    steps: list[PipelineStepConfig] = Field(
        default_factory=list, description="List of steps in the pipeline."
    )
