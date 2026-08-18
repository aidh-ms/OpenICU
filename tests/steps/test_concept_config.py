"""Tests for concept step configuration models (concept, simple/derived/complex)."""

from pathlib import Path

import pytest

from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.derived import DerivedDatasetConceptConfig
from open_icu.steps.concept.config.simple import MappingConfig, SimpleDatasetConceptConfig


class TestConceptConfig:
    def test_code_combines_name_and_unit(self) -> None:
        concept = ConceptConfig(name="heart_rate", version="1.0.0", unit="bpm")
        assert concept.code == "heart_rate//bpm"

    def test_load_discovers_dataset_concepts_by_filename(self, tmp_path: Path) -> None:
        concept_file = tmp_path / "heart_rate.yml"
        concept_file.write_text("name: heart_rate\nversion: 1.0.0\nunit: bpm\n")

        mapping_dir = tmp_path / "mimic-iv" / "3.1" / "mappings"
        mapping_dir.mkdir(parents=True)
        (mapping_dir / "heart_rate.yml").write_text(
            "type: simple\n"
            "mappings:\n"
            "  - pattern:\n"
            "      table: chartevents\n"
            "      evnt: Heart Rate\n"
            "      code: (220045//Heart Rate)\n"
            "    columns:\n"
            "      numeric_value: col(numeric_value)\n"
        )

        concept = ConceptConfig.load(concept_file, dataset_paths=[mapping_dir])

        assert len(concept.dataset_concepts) == 1
        dataset_concept = concept.get_dataset_concept("mimic-iv", "3.1")
        assert isinstance(dataset_concept, SimpleDatasetConceptConfig)
        assert dataset_concept.dataset == "mimic-iv"
        assert dataset_concept.version == "3.1"
        assert concept.get_dataset_concept("eicu-crd", "2.0") is None

    def test_load_skips_invalid_dataset_concept(self, tmp_path: Path) -> None:
        concept_file = tmp_path / "heart_rate.yml"
        concept_file.write_text("name: heart_rate\nversion: 1.0.0\nunit: bpm\n")

        mapping_dir = tmp_path / "mimic-iv" / "3.1" / "mappings"
        mapping_dir.mkdir(parents=True)
        (mapping_dir / "heart_rate.yml").write_text("type: nonsense\n")

        concept = ConceptConfig.load(concept_file, dataset_paths=[mapping_dir])
        assert concept.dataset_concepts == []

    def test_regex_built_from_pattern_parts(self) -> None:
        mapping = MappingConfig.model_validate(
            {
                "pattern": {"dataset": "mimic-iv", "table": "chartevents", "code": "(220045//Heart Rate)"},
                "columns": {},
            }
        )
        assert mapping.pattern.code == "(220045//Heart Rate)"

    def test_regex_uses_wildcards_for_missing_parts(self) -> None:
        mapping = MappingConfig.model_validate({"pattern": {"table": "example", "code": "(220045)", }, "columns": {}})
        assert mapping.pattern.code == "(220045)"


class TestConceptDefault:
    """A dataset-agnostic `default` standing in where a dataset has no mapping."""

    COMPLEX_DEFAULT = (
        "default:\n"
        "  type: complex\n"
        "  concept_transformer: open_icu.steps.concept.transformer.sofa.SofaTransformer\n"
        "  concepts:\n"
        "    - sofa_renal.1.0.0\n"
    )

    def write_concept(self, tmp_path: Path, body: str = "") -> Path:
        concept_file = tmp_path / "sofa.yml"
        concept_file.write_text(f"name: sofa\nversion: 1.0.0\nunit: points\n{body}")
        return concept_file

    def mapping_dir(self, tmp_path: Path, dataset: str = "mimic-iv", version: str = "3.1") -> Path:
        path = tmp_path / dataset / version / "mappings"
        path.mkdir(parents=True)
        return path

    def test_default_applies_where_a_dataset_has_no_mapping(self, tmp_path: Path) -> None:
        concept = ConceptConfig.load(self.write_concept(tmp_path, self.COMPLEX_DEFAULT))

        for dataset, version in [("mimic-iv", "3.1"), ("eicu-crd", "2.0")]:
            resolved = concept.get_dataset_concept(dataset, version)
            assert isinstance(resolved, ComplexDatasetConceptConfig)
            # identity comes from the dataset it was materialised for
            assert (resolved.dataset, resolved.version) == (dataset, version)
            assert resolved.dependencies == {"openicu.config.concept.sofa_renal.1.0.0"}

    def test_materialised_default_is_stable_and_linked_to_its_concept(self, tmp_path: Path) -> None:
        concept = ConceptConfig.load(self.write_concept(tmp_path, self.COMPLEX_DEFAULT))

        first = concept.get_dataset_concept("mimic-iv", "3.1")
        assert first is concept.get_dataset_concept("mimic-iv", "3.1")
        # the transformer is constructed with its parent concept, so the link
        # must survive materialisation or build_transformer passes None
        assert isinstance(first, ComplexDatasetConceptConfig)
        assert first._parent_concept is concept

    def test_a_dataset_mapping_overrides_the_default(self, tmp_path: Path) -> None:
        mapping_dir = self.mapping_dir(tmp_path)
        (mapping_dir / "sofa.yml").write_text(
            "type: simple\n"
            "mappings:\n"
            "  - pattern:\n"
            "      table: chartevents\n"
            "      code: (227428)\n"
            "    columns:\n"
            "      numeric_value: col(numeric_value)\n"
        )

        concept = ConceptConfig.load(
            self.write_concept(tmp_path, self.COMPLEX_DEFAULT), dataset_paths=[mapping_dir]
        )

        # the charted score wins for this dataset, the default still stands elsewhere
        assert isinstance(concept.get_dataset_concept("mimic-iv", "3.1"), SimpleDatasetConceptConfig)
        assert isinstance(concept.get_dataset_concept("eicu-crd", "2.0"), ComplexDatasetConceptConfig)

    def test_a_tombstone_suppresses_the_default(self, tmp_path: Path) -> None:
        mapping_dir = self.mapping_dir(tmp_path)
        (mapping_dir / "sofa.yml").write_text("deleted: true\n")

        concept = ConceptConfig.load(
            self.write_concept(tmp_path, self.COMPLEX_DEFAULT), dataset_paths=[mapping_dir]
        )

        assert concept.get_dataset_concept("mimic-iv", "3.1") is None
        assert concept.get_dataset_concept("eicu-crd", "2.0") is not None

    def test_no_default_means_no_fallback(self, tmp_path: Path) -> None:
        concept = ConceptConfig.load(self.write_concept(tmp_path))
        assert concept.get_dataset_concept("mimic-iv", "3.1") is None

    def test_a_simple_default_is_rejected(self, tmp_path: Path) -> None:
        body = (
            "default:\n"
            "  type: simple\n"
            "  mappings:\n"
            "    - pattern:\n"
            "        table: chartevents\n"
            "        code: (220045)\n"
            "      columns: {}\n"
        )
        with pytest.raises(ValueError, match="derived.*complex"):
            ConceptConfig.load(self.write_concept(tmp_path, body))

    def test_a_malformed_default_is_rejected_at_load(self, tmp_path: Path) -> None:
        # `complex` without a transformer cannot be built; catching it here beats
        # failing per dataset at run time
        body = "default:\n  type: complex\n  concepts:\n    - sofa_renal.1.0.0\n"
        with pytest.raises(ValueError, match="invalid 'default'"):
            ConceptConfig.load(self.write_concept(tmp_path, body))


class TestComplexConcept:
    def test_transformer_imported_and_called(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        (tmp_path / "fake_transformers.py").write_text(
            "class Recorder:\n"
            "    calls = []\n"
            "    def __init__(self, concept, config, step, **kwargs):\n"
            "        self.kwargs = kwargs\n"
            "    def __call__(self):\n"
            "        Recorder.calls.append((self.kwargs))\n"
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

        config.build_transformer("test_step")()  # ty: ignore[invalid-argument-type]
        import fake_transformers  # ty: ignore[unresolved-import]

        assert fake_transformers.Recorder.calls == [{"window": "1h"}]


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
        assert join.how == "full"
        assert join.join_params == {"on": ["subject_id", "time"]}
