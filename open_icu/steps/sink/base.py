from abc import ABCMeta
from pathlib import Path

from open_icu.steps.base import BaseStep
from open_icu.types.conf.concept import ConceptConfig
from open_icu.types.conf.sink import SinkConfig


class SinkStep(BaseStep[SinkConfig], metaclass=ABCMeta):
    """
    A abstract class for sink steps.

    Parameters
    ----------
    sink_path : Path
        The path to the sink directory.
    configs : Path | list[SinkConfig] | None
        The path to the configuration files or a list of configurations.
    concept_configs : Path | list[ConceptConfig] | None
        The path to the concept configuration files or a list of configurations.
    parent : BaseStep | None
        The parent step.
    """

    def __init__(
        self,
        sink_path: Path,
        configs: Path | list[SinkConfig] | None = None,
        concept_configs: Path | list[ConceptConfig] | None = None,
        parent: BaseStep | None = None,
    ) -> None:
        super().__init__(configs=configs, concept_configs=concept_configs, parent=parent)

        self._sink_path = sink_path

        self._sink_conigs: list[SinkConfig] = []
        if isinstance(configs, list):
            self._sink_conigs = configs
        elif self._config_path is not None:
            self._sink_conigs = self._read_config(self._config_path / "sink", SinkConfig)
