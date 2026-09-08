"""KDIGO acute kidney injury staging as windowed-concept transformers.

KDIGO stages AKI on two independent criteria — serum creatinine and urine
output — and takes the higher of the two. They are implemented here as two
separate concepts, :class:`AkiCreatinineTransformer` and
:class:`AkiUrineOutputTransformer`, with :class:`AkiTransformer` combining
them. Keeping them apart is not just tidiness: urine output is charted far
less reliably than creatinine and its availability differs sharply between
databases, so a great deal of published ICU work stages AKI on creatinine
alone. Splitting lets a study select its criterion, lets the combined stage
fall back cleanly to whichever criterion a dataset actually supports, and
makes the difference between the two measurable rather than hidden inside one
score.

Both criteria are graded in continuous time at every contributing measurement,
so a stage is a step function over the stay rather than a value per calendar
day. Two departures from the guideline text are worth stating:

- *Baseline* is the lowest creatinine in the preceding ``baseline_window``
  (7 days by default), the usual operationalisation where a premorbid value is
  unavailable. Early in a stay that window is mostly empty and the baseline is
  effectively the admission value, so AKI present on admission is not
  detectable — a known limitation of every retrospective implementation, not
  of this one.
- The paediatric criterion (eGFR < 35 mL/min/1.73m^2 in patients under 18) is
  not implemented.
"""

import polars as pl

from open_icu.steps.concept.transformer.windowed import (
    Aggregation,
    Exists,
    GradedConceptTransformer,
    Locf,
    RollingMin,
    SegmentedRollingSum,
    WindowedLocf,
    WindowedMaxTransformer,
)


class AkiComponent(GradedConceptTransformer):
    """One KDIGO criterion, graded 0-3."""

    def stage(self) -> pl.Expr:
        """Return the 0-3 stage expression over the aligned input columns."""
        raise NotImplementedError

    def grade(self) -> pl.Expr:
        return self.stage()


class AkiCreatinineTransformer(AkiComponent):
    """KDIGO stage from serum creatinine (mg/dL) and renal replacement therapy.

    Creatinine is carried onto the grid three times: as the current value, as
    the minimum over ``baseline_window`` (the baseline the ratios are taken
    against) and as the minimum over ``acute_window`` (against which the
    absolute 0.3 mg/dL rise is measured). The two windows are separate in the
    guideline and must stay separate here — a slow rise over a week is a ratio
    criterion, a 0.3 mg/dL jump only counts inside 48 hours.

    The absolute stage-3 threshold (creatinine reaching 4.0 mg/dL) is gated on
    the patient also meeting a stage-1 criterion, per the guideline footnote,
    so that stable chronic kidney disease does not stage as AKI. Set
    ``require_acute_rise=False`` for the ungated reading used by some
    implementations, which will raise sensitivity at the cost of staging
    unchanged chronic elevation as stage 3.

    Renal replacement therapy is optional: where the concept is unavailable
    for a dataset the remaining criteria still grade.
    """

    default_baseline_window = "7d"
    default_acute_window = "48h"

    #: many datasets have no renal-replacement concept; the remaining criteria still grade
    optional_inputs = {"renal_replacement_therapy"}

    def build_inputs(self) -> dict[str, Aggregation]:
        return {
            "creatinine": WindowedLocf(self.window),
            "creatinine_baseline": RollingMin(
                self._kwargs.get("baseline_window", self.default_baseline_window)
            ).of("creatinine"),
            "creatinine_acute_minimum": RollingMin(
                self._kwargs.get("acute_window", self.default_acute_window)
            ).of("creatinine"),
            "renal_replacement_therapy": Exists(self.window),
        }

    def observed(self) -> pl.Expr:
        # the RRT column is a boolean and never null, so the default
        # "any input carries a value" would hold everywhere
        return pl.col("creatinine").is_not_null() | pl.col("renal_replacement_therapy")

    def stage(self) -> pl.Expr:
        creatinine = pl.col("creatinine")
        ratio = creatinine / pl.col("creatinine_baseline")
        rise = creatinine - pl.col("creatinine_acute_minimum")
        acute = (ratio >= 1.5) | (rise >= 0.3)
        if not self._kwargs.get("require_acute_rise", True):
            acute = pl.lit(True)
        return (
            pl.when(pl.col("renal_replacement_therapy"))
            .then(3)
            .when(ratio >= 3.0)
            .then(3)
            .when((creatinine >= 4.0) & acute)
            .then(3)
            .when(ratio >= 2.0)
            .then(2)
            .when(acute)
            .then(1)
            .when(creatinine.is_not_null())
            .then(0)
            .otherwise(None)
        )


class AkiUrineOutputTransformer(AkiComponent):
    """KDIGO stage from urine output (mL) per body weight (kg).

    Each duration in the guideline is its own trailing window — 6h and 12h for
    the 0.5 mL/kg/h thresholds, 24h for 0.3 mL/kg/h, 12h for anuria — so urine
    output is carried onto the grid once per duration under
    :class:`SegmentedRollingSum`. That gating is what makes the criterion
    trustworthy: a window is graded only where it was observed end to end and
    the charting stream is still running, so a gap in documentation cannot be
    read as oliguria, while a genuinely empty window is anuria.

    Thresholds are on the *mean* rate over each window, which is the standard
    operationalisation; the guideline's "for N hours" strictly means
    continuously below, and a window averaging under the threshold need not
    have been under it throughout.

    Body weight is required. Where it is missing the criterion emits nothing
    rather than guessing; set ``default_weight_kg`` to fall back to a fixed
    weight, at the cost of a rate that is only as good as that assumption.
    """

    #: window -> hours, for converting a windowed volume to a mean rate
    durations = {"6h": 6, "12h": 12, "24h": 24}

    #: a weight record is not an evaluation point, and all three urine columns
    #: share the same presence, so one of them suffices
    triggers = {"urine_output_6h"}

    default_weight_concept = "patient_weight"

    @property
    def weight_concept(self) -> str:
        """Name of the body-weight concept; the mapping's ``weight_concept``."""
        return self._kwargs.get("weight_concept", self.default_weight_concept)

    def build_inputs(self) -> dict[str, Aggregation]:
        gap = self._kwargs.get("urine_output_gap")
        inputs: dict[str, Aggregation] = {
            f"urine_output_{window}": SegmentedRollingSum(window, gap=gap or window).of("urine_output")
            for window in self.durations
        }
        inputs["weight"] = Locf().of(self.weight_concept)
        return inputs

    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        if self._kwargs.get("default_weight_kg") is None:
            self._require_dependencies(
                dependencies,
                self.weight_concept,
                hint=(
                    "Urine-output staging is a rate in mL/kg/h and so needs body weight. Declare the "
                    "weight concept under `concepts`, set `kwargs.weight_concept` if it is named "
                    "differently, or set `kwargs.default_weight_kg` to stage against an assumed weight."
                ),
            )
        return super().transform(dependencies)

    def _weight(self) -> pl.Expr:
        default = self._kwargs.get("default_weight_kg")
        weight = pl.col("weight")
        return weight if default is None else weight.fill_null(float(default))

    def _rate(self, window: str) -> pl.Expr:
        """Mean urine output in mL/kg/h over the trailing ``window``."""
        weight = self._weight()
        return (
            pl.when(weight > 0)
            .then(pl.col(f"urine_output_{window}") / (weight * self.durations[window]))
            .otherwise(None)
        )

    def observed(self) -> pl.Expr:
        return pl.any_horizontal(*[self._rate(window).is_not_null() for window in self.durations])

    def stage(self) -> pl.Expr:
        return (
            # anuria for >=12h: a fully observed 12h window with no urine at all
            pl.when(pl.col("urine_output_12h") <= 0)
            .then(3)
            .when(self._rate("24h") < 0.3)
            .then(3)
            .when(self._rate("12h") < 0.5)
            .then(2)
            .when(self._rate("6h") < 0.5)
            .then(1)
            .otherwise(0)
        )


class AkiTransformer(WindowedMaxTransformer):
    """KDIGO AKI stage: the higher of the criterion stages within ``window``.

    Declarative — list the criterion concepts as the mapping's dependencies
    (or under ``kwargs.terms``). A criterion with no stage inside the window
    contributes nothing rather than 0, so a dataset whose urine-output
    charting cannot support the volume criterion still stages on creatinine
    alone instead of being pulled down to it.
    """
