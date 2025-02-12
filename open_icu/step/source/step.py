from pathlib import Path
from typing import Iterator

from dependency_injector import providers

from open_icu.conf import load_yaml_configs
from open_icu.db.sql import SQLDataFrameDatabaseExtractor
from open_icu.di.container import dynamic_container, wire
from open_icu.step.base import BaseStep
from open_icu.step.proto import StepProto
from open_icu.step.source.conf import SourceConfig
from open_icu.type.subject import SubjectData


class SourceStep(BaseStep):
    """
    A step that extracts subjects from a source.

    Parameters
    ----------
    source_configs : Path | list[SourceConfig]
        The source configurations.
    fail_silently : bool, default: False
        Whether to fail silently or not.
    parent : StepProto, default: None
        The parent step.
    """

    def __init__(
        self, source_configs: Path | list[SourceConfig], fail_silently: bool = False, parent: StepProto | None = None
    ) -> None:
        super().__init__(fail_silently, parent)

        self._source_configs: list[SourceConfig] = []
        if isinstance(source_configs, list):
            self._source_configs = source_configs
        elif isinstance(source_configs, Path):
            self._source_configs = load_yaml_configs(source_configs, SourceConfig)

        container = dynamic_container
        for source_config in self._source_configs:
            setattr(
                container,
                f"db_{source_config.name}",
                providers.Singleton(SQLDataFrameDatabaseExtractor, conncetion_uri=source_config.connection_uri),
            )
        wire()

    def __call__(self) -> Iterator[SubjectData]:
        """
        Starts the pipeline. and adds on subject at a time.

        Yields
        ------
        Iterator[SubjectData]
            An iterator of the subjects.
        """
        for source_config in self._source_configs:
            yield from source_config.service()
