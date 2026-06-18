"""Tests for concept step configuration models (concept, simple/derived/complex)."""

from pathlib import Path

import pytest

from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.config.derived import DerivedDatasetConceptConfig
from open_icu.steps.concept.config.simple import MappingConfig, SimpleDatasetConceptConfig


class TestConceptConfig:
    def test_code_combines_name_and_unit(self) -> None:
        concept = ConceptConfig(name="heart_rate", version="1.0.0", unit="bpm")
        assert concept.code == "heart_rate//bpm"

    def test_load_discovers_dataset_concepts_by_filename(self, tmp_path: Path) -> None:
        concept_file = tmp_path / "heart_rate.yml"
        concept_file.write_text("name: heart_rate\nversion: 1.0.0\nunit: bpm\n")

        mapping_dir = tmp_path / "mimic-iv" / "3.1" / "concept"
        mapping_dir.mkdir(parents=True)
        (mapping_dir / "heart_rate.yml").write_text(
            "type: simple\n"
            "mappings:\n"
            "  - pattern:\n"
            "      table: chartevents\n"
            "      code: (220045//Heart Rate)\n"
            "    columns:\n"
            "      numeric_value: col(numeric_value)\n"
        )

        concept = ConceptConfig.load(concept_file, dataset_paths=[mapping_dir])

        assert len(concept.dataset_concepts) == 1
        dataset_concept = concept.get_dataset_concept("mimic-iv")
        assert isinstance(dataset_concept, SimpleDatasetConceptConfig)
        assert dataset_concept.dataset == "mimic-iv"
        assert dataset_concept.version == "3.1"
        assert concept.get_dataset_concept("eicu-crd") is None

    def test_load_skips_invalid_dataset_concept(self, tmp_path: Path) -> None:
        concept_file = tmp_path / "heart_rate.yml"
        concept_file.write_text("name: heart_rate\nversion: 1.0.0\nunit: bpm\n")

        mapping_dir = tmp_path / "mimic-iv" / "3.1" / "concept"
        mapping_dir.mkdir(parents=True)
        (mapping_dir / "heart_rate.yml").write_text("type: nonsense\n")

        concept = ConceptConfig.load(concept_file, dataset_paths=[mapping_dir])
        assert concept.dataset_concepts == []


class TestSimpleConcept:
    def test_dataset_and_version_injected_into_mappings(self) -> None:
        config = SimpleDatasetConceptConfig.model_validate(
            {
                "name": "heart_rate",
                "version": "3.1",
                "dataset": "mimic-iv",
                "mappings": [
                    {
                        "pattern": {"table": "chartevents", "code": "(220045//Heart Rate)"},
                        "columns": {"numeric_value": "col(numeric_value)"},
                    }
                ],
            }
        )
        pattern = config.mappings[0].pattern
        assert pattern.dataset == "mimic-iv"
        assert pattern.version == "3.1"

    def test_regex_built_from_pattern_parts(self) -> None:
        mapping = MappingConfig.model_validate(
            {
                "pattern": {"dataset": "mimic-iv", "table": "chartevents", "code": "(220045//Heart Rate)"},
                "columns": {},
            }
        )
        assert mapping.regex == "mimic-iv//chartevents//(220045//Heart Rate)"

    def test_regex_uses_wildcards_for_missing_parts(self) -> None:
        mapping = MappingConfig.model_validate({"pattern": {"code": "(220045)"}, "columns": {}})
        assert mapping.regex == "(.+?)//(.+?)//(220045)"

    def test_explicit_regex_wins(self) -> None:
        mapping = MappingConfig.model_validate({"pattern": {"regex": "^custom$", "code": "ignored"}, "columns": {}})
        assert mapping.regex == "^custom$"


class TestComplexConcept:
    def test_transformer_imported_and_called(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        (tmp_path / "fake_transformers.py").write_text(
            "class Recorder:\n"
            "    calls = []\n"
            "    def __init__(self, concept, config, **kwargs):\n"
            "        self.kwargs = kwargs\n"
            "    def __call__(self, project):\n"
            "        Recorder.calls.append((project, self.kwargs))\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))

        from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig

        config = ComplexDatasetConceptConfig(
            name="windows",
            version="3.1",
            dataset="mimic-iv",
            concept_transformer="fake_transformers.Recorder",
            kwargs={"window": "1h"},
            concepts=["ventilation_start.1.0.0", "ventilation_end.1.0.0"],
        )

        assert config.dependencies == {
            "openicu.config.concept.ventilation_start.1.0.0",
            "openicu.config.concept.ventilation_end.1.0.0",
        }

        config.fn("project-sentinel")  # ty: ignore[invalid-argument-type]
        import fake_transformers  # ty: ignore[unresolved-import]

        assert fake_transformers.Recorder.calls == [("project-sentinel", {"window": "1h"})]


class TestDerivedConcept:
    def make_derived(self, **overrides) -> DerivedDatasetConceptConfig:
        data = {
            "name": "bmi",
            "version": "3.1",
            "dataset": "mimic-iv",
            "table": {"concept": "patient_weight.1.0.0", "columns": ["subject_id", "time", "numeric_value"]},
            "join": [
                {
                    "concept": "patient_height.1.0.0",
                    "columns": ["subject_id", "time", "numeric_value"],
                }
            ],
            "event": {"numeric_value": "col(bmi)"},
            **overrides,
        }
        return DerivedDatasetConceptConfig.model_validate(data)

    def test_dependencies_include_table_and_join_concepts(self) -> None:
        derived = self.make_derived()
        assert derived.dependencies == {
            "openicu.config.concept.patient_weight.1.0.0",
            "openicu.config.concept.patient_height.1.0.0",
        }

    def test_join_defaults(self) -> None:
        derived = self.make_derived()
        join = derived.join[0]
        assert join.both_on == ["subject_id", "time"]
        assert join.how == "outer"
        assert join.join_params == {"on": ["subject_id", "time"]}
