from pathlib import Path

import pandas as pd

from open_icu.conf import load_yaml_configs
from open_icu.step.base import BaseStep
from open_icu.step.concept.conf import ConceptConfig
from open_icu.step.proto import StepProto
from open_icu.type.subject import SubjectData


class ConceptStep(BaseStep):
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
        self, concept_configs: Path | list[ConceptConfig], fail_silently: bool = False, parent: StepProto | None = None
    ) -> None:
        super().__init__(fail_silently, parent)

        self._concept_configs: list[ConceptConfig] = []
        if isinstance(concept_configs, list):
            self._concept_configs = concept_configs
        elif isinstance(concept_configs, Path):
            self._concept_configs = load_yaml_configs(concept_configs, ConceptConfig)

    def process(self, subject_data: SubjectData) -> SubjectData:
        for concept in self._concept_configs:
            for concept_source in concept.sources:
                if concept_source.source != subject_data.source:
                    continue

                concept_data = concept_source.service(concept, subject_data.id)
                if concept_data is None:
                    continue

                if subject_data.data.get(concept.name, None) is None:
                    subject_data.data[concept.name] = concept_data.copy()  # type: ignore[assignment]
                else:
                    subject_data.data[concept.name] = pd.concat([subject_data.data[concept.name], concept_data.copy()])  # type: ignore[assignment]

        return subject_data
