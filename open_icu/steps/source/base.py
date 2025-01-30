from importlib import import_module
from pathlib import Path
from typing import Iterator

import pandas as pd

from open_icu.steps.base import BaseStep
from open_icu.steps.source.concept import ConceptExtractor
from open_icu.steps.source.sample import Sampler, SamplesSampler
from open_icu.types.base import SubjectData
from open_icu.types.conf.concept import ConceptConfig
from open_icu.types.conf.source import SourceConfig


class SourceStep(BaseStep[SourceConfig]):
    """
    A step that generates SubjectData instances from a list of sources.

    Parameters
    ----------
    configs : Path | list[SourceConfig] | None
        The path to the source configuration files or a list of SourceConfig objects.
    concept_configs : Path | list[ConceptConfig] | None
        The path to the concept configuration files or a list of ConceptConfig objects.
    parent : BaseStep | None
    """

    def __init__(
        self,
        configs: Path | list[SourceConfig] | None = None,
        concept_configs: Path | list[ConceptConfig] | None = None,
        parent: BaseStep | None = None,
    ) -> None:
        super().__init__(configs=configs, concept_configs=concept_configs, parent=parent)

        self._source_configs: dict[str, SourceConfig] = {}
        if isinstance(configs, list):
            self._source_configs = {conf.name: conf for conf in configs}
        elif self._config_path is not None:
            self._source_configs = {
                conf.name: conf for conf in self._read_config(self._config_path / "sources", SourceConfig)
            }

    def __call__(self) -> Iterator[SubjectData]:
        """
        A method that generates SubjectData instances from a list of sources.
        This method was overridden from the BaseStep class.

        Yields
        ------
        SubjectData
            A data object containing the subject ID, source name, and any additional data.
        """
        for source_config in self._source_configs.values():
            sampler: Sampler
            if source_config.sample.samples:
                sampler = SamplesSampler(source_config)
            else:
                module_name, cls_name = source_config.sample.sampler.rsplit(".", 1)
                module = import_module(module_name)
                cls = getattr(module, cls_name)
                sampler = cls(source_config)

            for subject_data in sampler.sample():
                subject_data = self.pre_process(subject_data)

                self.validate(subject_data)
                subject_data = self.process(subject_data)
                if self.filter(subject_data):
                    continue

                subject_data = self.post_process(subject_data)

                yield subject_data

    def process(self, subject_data: SubjectData) -> SubjectData:
        """
        A method that processes the SubjectData instance and add data corresponding to concepts.

        Parameters
        ----------
        subject_data : SubjectData
            The SubjectData instance to process.

        Returns
        -------
        SubjectData
            The processed SubjectData instance.
        """
        source = self._source_configs[subject_data.source]
        for concept in self._concept_configs:
            for concept_source in concept.sources:
                if concept_source.source != source.name:
                    continue

                module_name, cls_name = concept_source.extractor.rsplit(".", 1)
                module = import_module(module_name)
                cls = getattr(module, cls_name)
                extractor: ConceptExtractor = cls(subject_data.id, source, concept, concept_source)

                concept_data = extractor()
                if concept_data is None:
                    continue

                if subject_data.data.get(concept.name, None) is None:
                    subject_data.data[concept.name] = concept_data.copy()  # type: ignore[assignment]
                else:
                    subject_data.data[concept.name] = pd.concat([subject_data.data[concept.name], concept_data.copy()])  # type: ignore[assignment]

        return subject_data
