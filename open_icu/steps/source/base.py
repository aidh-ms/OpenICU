from importlib import import_module
from pathlib import Path
from typing import Iterator

import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.base import BaseStep
from open_icu.steps.source.concept import ConceptExtractor
from open_icu.steps.source.sample import Sampler
from open_icu.types.base import FHIRSchema, SubjectData
from open_icu.types.conf.concept import Concept
from open_icu.types.conf.source import SourceConfig


class SourceStep(BaseStep):
    def __init__(self, config_path: Path | None = None, parent: BaseStep | None = None) -> None:
        super().__init__(config_path, parent)

        self._source_conigs: dict[str, SourceConfig] = {}
        self._concepts = []
        if config_path is not None:
            self._source_conigs = {conf.name: conf for conf in self._read_config(config_path / "sources", SourceConfig)}
            self._concepts = self._read_config(config_path / "concepts", Concept)

    def __call__(self) -> Iterator[SubjectData]:
        for source_config in self._source_conigs.values():
            module_name, cls_name = source_config.sample.sampler.rsplit(".", 1)
            module = import_module(module_name)
            cls = getattr(module, cls_name)
            sampler: Sampler = cls(source_config)

            for subject_data in sampler.sample():
                subject_data = self.pre_process(subject_data)

                self.validate(subject_data)
                subject_data = self.process(subject_data)
                if not self.filter(subject_data):
                    continue

                subject_data = self.post_process(subject_data)

                yield subject_data

    def process(self, subject_data: SubjectData) -> SubjectData:
        source = self._source_conigs[subject_data.source]
        for concept in self._concepts:
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
                    subject_data.data[concept.name] = pd.concat(
                        [subject_data.data[concept.name], concept_data.copy()]
                    ).pipe(DataFrame[FHIRSchema])

        return subject_data
