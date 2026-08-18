"""Tests for the complex-concept transformer base.

Covers the machinery every complex concept inherits and none of them should
have to re-test: resolving dependencies through the registry, the strict and
tolerant policies for one that cannot be resolved, and the MEDS shape of what
gets written. ``transform`` itself is trivial here — what is under test is
everything around it.
"""

from datetime import datetime
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.step import ConceptStep
from open_icu.steps.concept.transformer.base import BaseConceptTransformer

T0 = datetime(2024, 1, 1, 0, 0)
DATASET = "testdb"


class Step:
    """Minimal stand-in for the concept step: a registry and an output root."""

    def __init__(self, root: Path, *concepts: ConceptConfig) -> None:
        self._registry = {concept.identifier: concept for concept in concepts}
        self._root = root

    def concept_output_dir(self, concept: ConceptConfig) -> Path:
        return self._root / concept.name / concept.version


class Recording(BaseConceptTransformer):
    """Records what was resolved and passes the first dependency through."""

    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        self.resolved = sorted(dependencies)
        first = dependencies[self.resolved[0]]
        return first.select("subject_id", "time", "numeric_value")


class TolerantRecording(Recording):
    strict_dependencies = False


class TextProducing(BaseConceptTransformer):
    """Produces a text_value and no numeric_value."""

    strict_dependencies = False

    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        first = next(iter(dependencies.values()))
        return first.select("subject_id", "time", text_value=pl.lit("present"))


@pytest.fixture
def concept() -> ConceptConfig:
    return ConceptConfig(
        name="derived",
        version="1.0.0",
        unit="points",
        extension_columns={"dataset": 'col("dataset")'},
    )


def source(name: str, version: str = "1.0.0", unit: str = "u") -> ConceptConfig:
    return ConceptConfig(name=name, version=version, unit=unit)


def write_output(step: Step, concept: ConceptConfig, *values: float) -> None:
    directory = step.concept_output_dir(concept)
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "subject_id": [1] * len(values),
            "time": [T0] * len(values),
            "numeric_value": list(values),
        },
        schema={"subject_id": pl.Int64, "time": pl.Datetime(time_unit="us"), "numeric_value": pl.Float32},
    ).write_parquet(directory / f"{DATASET}.parquet")


def config(*identifiers: str) -> ComplexDatasetConceptConfig:
    return ComplexDatasetConceptConfig(
        name="derived",
        version="1.0",
        dataset=DATASET,
        concept_transformer="unused",
        concepts=list(identifiers),
    )


def output_of(step: Step, concept: ConceptConfig) -> pl.DataFrame:
    return pl.read_parquet(step.concept_output_dir(concept) / f"{DATASET}.parquet")


def test_dependencies_are_resolved_and_keyed_by_name(tmp_path: Path, concept: ConceptConfig) -> None:
    creatinine, urine = source("creatinine"), source("urine_output")
    step = Step(tmp_path, creatinine, urine)
    write_output(step, creatinine, 2.0)
    write_output(step, urine, 300.0)

    transformer = Recording(concept, config(creatinine.identifier, urine.identifier), cast(ConceptStep, step))
    transformer()

    # keyed by unversioned name, so a transform can refer to inputs without pinning a version
    assert transformer.resolved == ["creatinine", "urine_output"]


def test_output_carries_the_meds_columns(tmp_path: Path, concept: ConceptConfig) -> None:
    creatinine = source("creatinine")
    step = Step(tmp_path, creatinine)
    write_output(step, creatinine, 2.0)

    Recording(concept, config(creatinine.identifier), cast(ConceptStep, step))()
    written = output_of(step, concept)

    assert written["code"].to_list() == [concept.code]
    assert written["numeric_value"].to_list() == [2.0]
    assert written.schema["numeric_value"] == pl.Float32
    assert written["text_value"].to_list() == [None]  # filled in even though transform produced none
    assert written["dataset"].to_list() == [DATASET]  # extension column


def test_a_transform_may_produce_text_instead_of_a_number(tmp_path: Path, concept: ConceptConfig) -> None:
    marker = source("marker")
    step = Step(tmp_path, marker)
    write_output(step, marker, 1.0)

    TextProducing(concept, config(marker.identifier), cast(ConceptStep, step))()
    written = output_of(step, concept)

    assert written["text_value"].to_list() == ["present"]
    assert written["numeric_value"].to_list() == [None]


class TestStrictDependencies:
    """The default policy: a declared dependency that cannot be resolved is fatal."""

    def test_unknown_identifier_raises(self, tmp_path: Path, concept: ConceptConfig) -> None:
        step = Step(tmp_path)
        with pytest.raises(ValueError):
            Recording(concept, config("absent.1.0.0"), cast(ConceptStep, step))()

    def test_missing_output_raises(self, tmp_path: Path, concept: ConceptConfig) -> None:
        creatinine = source("creatinine")
        step = Step(tmp_path, creatinine)  # registered, but never computed for this dataset
        with pytest.raises(FileNotFoundError):
            Recording(concept, config(creatinine.identifier), cast(ConceptStep, step))()


class TestTolerantDependencies:
    """The opt-in policy for concepts that degrade to the inputs a dataset has."""

    def test_a_missing_dependency_is_skipped(self, tmp_path: Path, concept: ConceptConfig) -> None:
        creatinine, urine = source("creatinine"), source("urine_output")
        step = Step(tmp_path, creatinine, urine)
        write_output(step, creatinine, 2.0)  # urine never computed for this dataset

        transformer = TolerantRecording(
            concept, config(creatinine.identifier, urine.identifier), cast(ConceptStep, step)
        )
        transformer()

        assert transformer.resolved == ["creatinine"]
        assert output_of(step, concept)["numeric_value"].to_list() == [2.0]

    def test_nothing_is_written_when_no_dependency_resolves(self, tmp_path: Path, concept: ConceptConfig) -> None:
        urine = source("urine_output")
        step = Step(tmp_path, urine)

        TolerantRecording(concept, config(urine.identifier), cast(ConceptStep, step))()

        # the concept is simply not computable for this dataset; no empty parquet is left behind
        assert not (step.concept_output_dir(concept) / f"{DATASET}.parquet").exists()


def test_duplicate_dependency_names_are_rejected(tmp_path: Path, concept: ConceptConfig) -> None:
    """Two versions of one concept collide on name, and a transform addresses
    its inputs by name — so neither can be chosen without making the output
    depend on the order the mapping stored its dependencies in."""
    first, second = source("creatinine", version="1.0.0"), source("creatinine", version="2.0.0")
    step = Step(tmp_path, first, second)
    write_output(step, first, 2.0)
    write_output(step, second, 9.0)

    with pytest.raises(ValueError, match="creatinine"):
        Recording(concept, config(first.identifier, second.identifier), cast(ConceptStep, step))()


def test_dependencies_are_resolved_in_a_stable_order(tmp_path: Path, concept: ConceptConfig) -> None:
    """Resolution order does not depend on how the mapping stores its concepts."""
    creatinine, urine = source("creatinine"), source("urine_output")
    step = Step(tmp_path, creatinine, urine)
    write_output(step, creatinine, 2.0)
    write_output(step, urine, 300.0)

    forwards = Recording(concept, config(creatinine.identifier, urine.identifier), cast(ConceptStep, step))
    backwards = Recording(concept, config(urine.identifier, creatinine.identifier), cast(ConceptStep, step))
    forwards()
    backwards()

    assert forwards.resolved == backwards.resolved
