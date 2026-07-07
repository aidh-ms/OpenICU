"""Tests for the general windowed-concept transformer base.

Exercises the reusable machinery independently of SOFA, on the two shapes it
was designed against — a single-stream rolling aggregate and a triggered
lookaround over two streams — plus missing-input robustness. This is what
keeps the base honest as a general primitive rather than a SOFA detail.
"""

from datetime import datetime, timedelta

import polars as pl
import pytest

from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.transformers import (
    Exists,
    Locf,
    RollingMax,
    RollingMean,
    WindowedConceptTransformer,
)

T0 = datetime(2024, 1, 1, 0, 0)


@pytest.fixture
def concept() -> ConceptConfig:
    return ConceptConfig(name="feature", version="1.0.0", unit="u")


@pytest.fixture
def complex_config() -> ComplexDatasetConceptConfig:
    return ComplexDatasetConceptConfig(
        name="feature",
        version="1.0",
        dataset="testdb",
        concept_transformer="unused.in.transform",
    )


def frame(*rows: tuple[int, datetime, float | None]) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "subject_id": [r[0] for r in rows],
            "time": [r[1] for r in rows],
            "numeric_value": [r[2] for r in rows],
        },
        schema={"subject_id": pl.Int64, "time": pl.Datetime(time_unit="us"), "numeric_value": pl.Float32},
    )


class RollingMeanFeature(WindowedConceptTransformer):
    inputs = {"lactate": RollingMean("6h")}

    def compute(self) -> pl.Expr:
        return pl.col("lactate")


class AntibioticNearCulture(WindowedConceptTransformer):
    inputs = {"culture": Locf(), "antibiotic": Exists("24h")}
    triggers = {"culture"}

    def compute(self) -> pl.Expr:
        return pl.col("antibiotic").cast(pl.Float32)


class TwoInputMax(WindowedConceptTransformer):
    inputs = {"a": Locf(), "b": RollingMax("24h")}

    def compute(self) -> pl.Expr:
        return pl.max_horizontal(pl.col("a").fill_null(0), pl.col("b").fill_null(0)).cast(pl.Float32)


def test_single_stream_rolling_mean(concept: ConceptConfig, complex_config: ComplexDatasetConceptConfig) -> None:
    transformer = RollingMeanFeature(concept, complex_config)
    lactate = frame((1, T0, 2.0), (1, T0 + timedelta(hours=1), 4.0), (1, T0 + timedelta(hours=10), 10.0))
    out = transformer.transform({"lactate": lactate}).collect().sort("time")
    # mean(2), mean(2,4), then only the 10 remains within the trailing 6h
    assert out["numeric_value"].to_list() == [2.0, 3.0, 10.0]


def test_triggers_restrict_output_and_lookaround(
    concept: ConceptConfig, complex_config: ComplexDatasetConceptConfig
) -> None:
    transformer = AntibioticNearCulture(concept, complex_config)
    culture = frame((1, T0 + timedelta(hours=2), 1.0), (1, T0 + timedelta(days=3), 1.0))
    antibiotic = frame((1, T0, 1.0))  # 2h before the first culture only
    out = transformer.transform({"culture": culture, "antibiotic": antibiotic}).collect().sort("time")
    # only culture timestamps are emitted (the antibiotic-only time is not a trigger)
    assert out.height == 2
    # antibiotic within 24h of the first culture, not the second
    assert out["numeric_value"].to_list() == [1.0, 0.0]


def test_missing_declared_input_is_treated_as_absent(
    concept: ConceptConfig, complex_config: ComplexDatasetConceptConfig
) -> None:
    transformer = TwoInputMax(concept, complex_config)
    out = transformer.transform({"a": frame((1, T0, 3.0))}).collect()  # 'b' not supplied
    assert out["numeric_value"].to_list() == [3.0]
