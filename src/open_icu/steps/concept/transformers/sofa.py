"""SOFA score components as thin windowed-concept transformers.

Each SOFA organ sub-score is a piecewise-constant grade (0-4) of one or two
inputs, (re)evaluated in continuous time at every contributing measurement.
The alignment/windowing machinery lives in
:class:`~open_icu.steps.concept.transformers.windowed.WindowedConceptTransformer`;
a component only declares its ``inputs`` (with the aggregation that carries
each onto the grid) and a ``score`` expression over the aligned columns.

Following ricu's thresholds but not its hourly binning: creatinine is carried
forward (LOCF) so the score tracks the current value rather than a 24h worst,
and urine output is summed over a trailing 24h window where a recorded ``0``
is real anuria but a window with no records contributes nothing (missing !=
zero). A component with no data scores 0, so components sum cleanly into a
total SOFA (itself an ordinary ``derived`` concept over the sub-scores).
"""

import polars as pl

from open_icu.steps.concept.transformers.windowed import (
    Locf,
    RollingSum,
    WindowedConceptTransformer,
)


class SofaComponent(WindowedConceptTransformer):
    """A single SOFA organ sub-score (0-4); missing component contributes 0."""

    def score(self) -> pl.Expr:
        """Return the 0-4 sub-score expression over the aligned input columns."""
        raise NotImplementedError

    def compute(self) -> pl.Expr:
        return self.score().fill_null(0).cast(pl.Float32)


class SofaRenalTransformer(SofaComponent):
    """Renal sub-score from serum creatinine (mg/dL) and 24h urine output (mL)."""

    inputs = {
        "creatinine": Locf(),
        "urine_output": RollingSum("24h"),
    }

    def score(self) -> pl.Expr:
        creatinine = pl.col("creatinine")
        urine24 = pl.col("urine_output")
        return pl.max_horizontal(
            pl.when(creatinine >= 5.0)
            .then(4)
            .when(creatinine >= 3.5)
            .then(3)
            .when(creatinine >= 2.0)
            .then(2)
            .when(creatinine >= 1.2)
            .then(1)
            .otherwise(0),
            pl.when(urine24 < 200).then(4).when(urine24 < 500).then(3).otherwise(0),
        )


class SofaCoagulationTransformer(SofaComponent):
    """Coagulation sub-score from platelet count (10^3/uL)."""

    inputs = {"platelet_count": Locf()}

    def score(self) -> pl.Expr:
        platelets = pl.col("platelet_count")
        return (
            pl.when(platelets < 20)
            .then(4)
            .when(platelets < 50)
            .then(3)
            .when(platelets < 100)
            .then(2)
            .when(platelets < 150)
            .then(1)
            .otherwise(0)
        )


class SofaLiverTransformer(SofaComponent):
    """Liver sub-score from total bilirubin (mg/dL)."""

    inputs = {"total_bilirubin": Locf()}

    def score(self) -> pl.Expr:
        bilirubin = pl.col("total_bilirubin")
        return (
            pl.when(bilirubin >= 12.0)
            .then(4)
            .when(bilirubin >= 6.0)
            .then(3)
            .when(bilirubin >= 2.0)
            .then(2)
            .when(bilirubin >= 1.2)
            .then(1)
            .otherwise(0)
        )


class SofaCnsTransformer(SofaComponent):
    """Central-nervous-system sub-score from the Glasgow Coma Scale total (3-15)."""

    inputs = {"GCS_total": Locf()}

    def score(self) -> pl.Expr:
        gcs = pl.col("GCS_total")
        return (
            pl.when(gcs < 6).then(4).when(gcs < 10).then(3).when(gcs < 13).then(2).when(gcs < 15).then(1).otherwise(0)
        )


class SofaCardiovascularTransformer(SofaComponent):
    """Cardiovascular sub-score from MAP (mmHg) and vasopressor rates (mcg/kg/min).

    Vasopressor rates are carried forward (LOCF), so — as with the urine=0
    convention for renal — an infusion that stops should be recorded as a rate
    of 0; otherwise the last rate persists. A ``null`` comparison is treated as
    "not met" (matching ricu's ``is_true``), so a tier is only taken when at
    least one of its conditions is genuinely satisfied.
    """

    inputs = {
        "mean_arterial_pressure": Locf(),
        "dopamine_rate": Locf(),
        "dobutamine_rate": Locf(),
        "epinephrine_rate": Locf(),
        "norepinephrine_rate": Locf(),
    }

    def score(self) -> pl.Expr:
        mean_pressure = pl.col("mean_arterial_pressure")
        dopamine = pl.col("dopamine_rate")
        dobutamine = pl.col("dobutamine_rate")
        epinephrine = pl.col("epinephrine_rate")
        norepinephrine = pl.col("norepinephrine_rate")
        return (
            pl.when((dopamine > 15) | (epinephrine > 0.1) | (norepinephrine > 0.1))
            .then(4)
            .when(
                (dopamine > 5)
                | ((epinephrine > 0) & (epinephrine <= 0.1))
                | ((norepinephrine > 0) & (norepinephrine <= 0.1))
            )
            .then(3)
            .when(((dopamine > 0) & (dopamine <= 5)) | (dobutamine > 0))
            .then(2)
            .when(mean_pressure < 70)
            .then(1)
            .otherwise(0)
        )


class SofaRespirationTransformer(SofaComponent):
    """Respiration sub-score from PaO2/FiO2 (mmHg / %), gated on mechanical ventilation.

    The 100 and 200 (points 4 and 3) tiers require ventilation; when ventilation
    status is unknown the score degrades to the ventilation-independent tiers
    (<=2), matching ricu's ``is_true(pafi < x & vent)`` behaviour with missing
    ``vent``. FiO2 is a percentage, so PaO2/FiO2 * 100 yields the ratio in mmHg.
    """

    inputs = {
        "O2_partial_pressure": Locf(),
        "fraction_of_inspired_oxygen": Locf(),
        "mechanical_ventilation_windows": Locf(),
    }

    def score(self) -> pl.Expr:
        pao2 = pl.col("O2_partial_pressure")
        fio2 = pl.col("fraction_of_inspired_oxygen")
        ventilated = pl.col("mechanical_ventilation_windows") > 0
        pafi = pl.when(fio2 > 0).then(pao2 / fio2 * 100).otherwise(None)
        return (
            pl.when((pafi < 100) & ventilated)
            .then(4)
            .when((pafi < 200) & ventilated)
            .then(3)
            .when(pafi < 300)
            .then(2)
            .when(pafi < 400)
            .then(1)
            .otherwise(0)
        )
