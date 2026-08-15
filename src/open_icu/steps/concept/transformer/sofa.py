"""SOFA sub-scores and total as thin windowed-concept transformers.

Each organ sub-score is a piecewise-constant grade (0-4) over one or two
inputs, (re)evaluated in continuous time at every contributing measurement.
All alignment and windowing lives in
:class:`~open_icu.steps.concept.transformer.windowed.WindowedConceptTransformer`;
a component only declares which inputs it needs, with the aggregation that
carries each onto the grid, and a ``score`` expression over the aligned
columns.

Thresholds follow ricu, the windowing does not: instead of hourly binning,
every input is carried forward only for as long as it can reasonably be
called current (``window``, 24h by default), so a component reflects what is
known *now* rather than a fixed calendar bin, and stale data expires instead
of persisting indefinitely. Urine output is summed over the same trailing
window under :class:`SegmentedRollingSum`, so a recorded 0 is real anuria
while a window that is not fully observed contributes nothing.

A component emits no event where none of its inputs is currently known, so an
absent component is distinguishable from a component scoring 0. The total
(:class:`SofaTransformer`) then sums the most recent value of each sub-score
within its own window, treating an unknown component as 0 — the usual
convention, and the reason components must not emit spurious zeros.
"""

import polars as pl

from open_icu.steps.concept.transformer.windowed import (
    Aggregation,
    GradedConceptTransformer,
    Locf,
    SegmentedRollingSum,
    WindowedLocf,
    WindowedSumTransformer,
)


class SofaComponent(GradedConceptTransformer):
    """A single SOFA organ sub-score (0-4).

    Subclasses implement ``build_inputs`` (so the lookback stays configurable)
    and ``score``. ``window`` comes from the mapping's ``kwargs`` and defaults
    to 24h.
    """

    def score(self) -> pl.Expr:
        """Return the 0-4 sub-score expression over the aligned input columns."""
        raise NotImplementedError

    def grade(self) -> pl.Expr:
        return self.score()


class SofaRenalTransformer(SofaComponent):
    """Renal sub-score from serum creatinine (mg/dL) and windowed urine output (mL).

    Urine output is segmented on ``urine_output_gap`` (default: the component
    window), so the volume sub-score is graded only across a window that was
    actually observed end to end and whose stream is still running.
    """

    def build_inputs(self) -> dict[str, Aggregation]:
        return {
            "creatinine": WindowedLocf(self.window),
            "urine_output": SegmentedRollingSum(
                self.window, gap=self._kwargs.get("urine_output_gap", self.window)
            ),
        }

    def score(self) -> pl.Expr:
        creatinine = pl.col("creatinine")
        urine = pl.col("urine_output")
        return pl.max_horizontal(
            pl.when(creatinine >= 5.0)
            .then(4)
            .when(creatinine >= 3.5)
            .then(3)
            .when(creatinine >= 2.0)
            .then(2)
            .when(creatinine >= 1.2)
            .then(1)
            .when(creatinine.is_not_null())
            .then(0)
            .otherwise(None),
            pl.when(urine < 200)
            .then(4)
            .when(urine < 500)
            .then(3)
            .when(urine.is_not_null())
            .then(0)
            .otherwise(None),
        )


class SofaCoagulationTransformer(SofaComponent):
    """Coagulation sub-score from platelet count (10^3/uL)."""

    def build_inputs(self) -> dict[str, Aggregation]:
        return {"platelet_count": WindowedLocf(self.window)}

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

    def build_inputs(self) -> dict[str, Aggregation]:
        return {"total_bilirubin": WindowedLocf(self.window)}

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

    def build_inputs(self) -> dict[str, Aggregation]:
        return {"GCS_total": WindowedLocf(self.window)}

    def score(self) -> pl.Expr:
        gcs = pl.col("GCS_total")
        return (
            pl.when(gcs < 6).then(4).when(gcs < 10).then(3).when(gcs < 13).then(2).when(gcs < 15).then(1).otherwise(0)
        )


class SofaCardiovascularTransformer(SofaComponent):
    """Cardiovascular sub-score from MAP (mmHg) and vasopressor rates (mcg/kg/min).

    Vasopressor rates are carried forward like any other input, so — as with
    the urine=0 convention for renal — a stopped infusion must be recorded as
    a rate of 0; otherwise the last rate persists for the length of the window.
    A null comparison counts as "not met" (ricu's ``is_true``), so a tier is
    taken only where one of its conditions is genuinely satisfied.
    """

    def build_inputs(self) -> dict[str, Aggregation]:
        return {
            name: WindowedLocf(self.window)
            for name in (
                "mean_arterial_pressure",
                "dopamine_rate",
                "dobutamine_rate",
                "epinephrine_rate",
                "norepinephrine_rate",
            )
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


class SofaRespiratoryTransformer(SofaComponent):
    """Respiration sub-score from PaO2/FiO2 (mmHg / %), gated on ventilation.

    The 4 and 3 tiers require mechanical ventilation; where ventilation status
    is unknown the score degrades to the ventilation-independent tiers (<=2),
    matching ricu's ``is_true(pafi < x & vent)`` with a missing ``vent``. FiO2
    is a percentage, so PaO2/FiO2 * 100 yields the ratio in mmHg.

    Ventilation is a state rather than a measurement — it is carried forward
    without expiry, since a patient ventilated for days may generate no new
    ventilation record. Set ``ventilation_window`` to expire it instead.
    """

    def build_inputs(self) -> dict[str, Aggregation]:
        ventilation_window = self._kwargs.get("ventilation_window")
        return {
            "O2_partial_pressure": WindowedLocf(self.window),
            "fraction_of_inspired_oxygen": WindowedLocf(self.window),
            "mechanical_ventilation_windows": (
                WindowedLocf(ventilation_window) if ventilation_window else Locf()
            ),
        }

    def _pafi(self) -> pl.Expr:
        fio2 = pl.col("fraction_of_inspired_oxygen")
        return pl.when(fio2 > 0).then(pl.col("O2_partial_pressure") / fio2 * 100).otherwise(None)

    def observed(self) -> pl.Expr:
        # ventilation alone grades nothing; both gas values are required
        return self._pafi().is_not_null()

    def score(self) -> pl.Expr:
        pafi = self._pafi()
        ventilated = pl.col("mechanical_ventilation_windows") > 0
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


class SofaTransformer(WindowedSumTransformer):
    """Total SOFA: the most recent value of each sub-score within ``window``.

    Purely declarative — list the six sub-score concepts as the mapping's
    dependencies (or under ``kwargs.terms``) and set ``window`` to control how
    long a sub-score stays current. Re-evaluated whenever any sub-score is
    updated; a sub-score with no value in the window contributes 0.
    """
