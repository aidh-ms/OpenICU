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
    RollingMax,
    SameHourLocf,
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

    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        if not self._kwargs.get("ricu_hourly_platelets", False):
            return super().transform(dependencies)

        platelets = dependencies.get("platelet_count")
        admission = dependencies.get("icu_admission")
        if platelets is None or admission is None:
            raise ValueError(
                "ricu_hourly_platelets requires platelet_count and icu_admission"
            )

        admission_time = (
            admission
            .select(
                "subject_id",
                "stay_id",
                pl.col("time").alias("__admission_time"),
            )
            .group_by("subject_id", "stay_id")
            .agg(pl.col("__admission_time").min())
        )

        hourly_platelets = (
            platelets
            .filter(
                pl.col("numeric_value").is_not_null()
                & pl.col("numeric_value").is_between(5, 1200)
            )
            .join(admission_time, on=["subject_id", "stay_id"], how="inner")
            .with_columns(
                (
                    (pl.col("time") - pl.col("__admission_time"))
                    .dt.total_minutes()
                    .floordiv(60)
                    .cast(pl.Int64)
                ).alias("__hour")
            )
            .with_columns(
                (
                    pl.col("__admission_time")
                    + pl.duration(hours=pl.col("__hour"))
                ).alias("time")
            )
            .group_by("subject_id", "stay_id", "time")
            .agg(pl.col("numeric_value").min().alias("numeric_value"))
        )

        dependencies = dict(dependencies)
        dependencies["platelet_count"] = hourly_platelets
        return super().transform(dependencies)

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
        return {"gcs_total": WindowedLocf(self.window)}

    def score(self) -> pl.Expr:
        gcs = pl.col("gcs_total")
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

    PaO2 and FiO2 are matched over a 2h window by default, matching ricu's
    ``pafi`` callback. Missing FiO2 is treated as room air (21%).

    Ventilation is a state rather than a measurement — it is carried forward
    without expiry, since a patient ventilated for days may generate no new
    ventilation record. Set ``ventilation_window`` to expire it instead.
    """

    triggers = {
        "O2_partial_pressure",
        "fraction_of_inspired_oxygen",
        "mechanical_ventilation_windows",
    }

    @staticmethod
    def _hourly_gas(
        lf: pl.LazyFrame,
        admission: pl.LazyFrame,
        aggregation: str,
    ) -> pl.LazyFrame:
        """Aggregate a gas measurement to ICU-admission-relative hourly bins."""
        value = pl.col("numeric_value")

        if aggregation == "min":
            aggregate_value = value.min()
        elif aggregation == "max":
            aggregate_value = value.max()
        else:
            raise ValueError(f"unsupported hourly aggregation: {aggregation}")

        return (
            lf.filter(pl.col("numeric_value").is_not_null())
            .select(
                pl.col("subject_id").cast(pl.Int64),
                pl.col("stay_id").cast(pl.String),
                pl.col("time").cast(pl.Datetime(time_unit="us")),
                value.cast(pl.Float32),
            )
            .join(
                admission,
                on=["subject_id", "stay_id"],
                how="inner",
            )
            .with_columns(
                (
                    (
                        (pl.col("time") - pl.col("__icu_admission"))
                        .dt.total_minutes()
                        .floordiv(60)
                    )
                    * 60
                ).alias("__relative_minutes")
            )
            .with_columns(
                (
                    pl.col("__icu_admission")
                    + pl.duration(minutes=pl.col("__relative_minutes"))
                ).alias("time")
            )
            .group_by("subject_id", "stay_id", "time")
            .agg(aggregate_value.alias("numeric_value"))
            .sort("subject_id", "stay_id", "time")
        )

    def transform(
        self,
        dependencies: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        if not self._kwargs.get("ricu_hourly_pafi", False):
            return super().transform(dependencies)

        required = {
            "O2_partial_pressure",
            "fraction_of_inspired_oxygen",
            "icu_admission",
        }
        if not required.issubset(dependencies):
            return super().transform(dependencies)

        admission = dependencies["icu_admission"].select(
            pl.col("subject_id").cast(pl.Int64),
            pl.col("stay_id").cast(pl.String),
            pl.col("time")
            .cast(pl.Datetime(time_unit="us"))
            .alias("__icu_admission"),
        )

        dependencies = dict(dependencies)

        dependencies["O2_partial_pressure"] = self._hourly_gas(
            dependencies["O2_partial_pressure"],
            admission,
            "min",
        )

        dependencies["fraction_of_inspired_oxygen"] = self._hourly_gas(
            dependencies["fraction_of_inspired_oxygen"],
            admission,
            "max",
        )

        pao2 = dependencies["O2_partial_pressure"].rename(
            {"numeric_value": "__pao2"}
        )
        fio2 = dependencies["fraction_of_inspired_oxygen"].rename(
            {"numeric_value": "__fio2"}
        )

        pafi_window = self._kwargs.get("pafi_window", "2h")
        keys = ["subject_id", "stay_id"]

        # RICU match_vals():
        # 1. FiO2 timestamps anchored with the latest preceding PaO2 <= 2h.
        # 2. PaO2 timestamps anchored with the latest preceding FiO2 <= 2h.
        # 3. Union both directions; missing FiO2 defaults to room air (21%).
        fio2_anchored = (
            fio2.sort(*keys, "time")
            .join_asof(
                pao2.sort(*keys, "time"),
                on="time",
                by=keys,
                strategy="backward",
                tolerance=pafi_window,
            )
            .filter(pl.col("__pao2").is_not_null())
            .select(*keys, "time", "__pao2", "__fio2")
        )

        pao2_anchored = (
            pao2.sort(*keys, "time")
            .join_asof(
                fio2.sort(*keys, "time"),
                on="time",
                by=keys,
                strategy="backward",
                tolerance=pafi_window,
            )
            .with_columns(pl.col("__fio2").fill_null(21.0))
            .select(*keys, "time", "__pao2", "__fio2")
        )

        dependencies["__ricu_pafi"] = (
            pl.concat([fio2_anchored, pao2_anchored])
            .unique()
            .filter(
                pl.col("__pao2").is_not_null()
                & pl.col("__fio2").is_not_null()
                & (pl.col("__fio2") > 0)
            )
            .with_columns(
                (pl.col("__pao2") / pl.col("__fio2") * 100)
                .cast(pl.Float32)
                .alias("numeric_value")
            )
            .select(*keys, "time", "numeric_value")
            .sort(*keys, "time")
        )

        # Admission and the raw gas concepts were only needed to construct
        # RICU-compatible P/F events.
        dependencies.pop("icu_admission", None)
        dependencies.pop("O2_partial_pressure", None)
        dependencies.pop("fraction_of_inspired_oxygen", None)

        self.triggers = {
            "__ricu_pafi",
            "mechanical_ventilation_windows",
        }

        return super().transform(dependencies)

    def build_inputs(self) -> dict[str, Aggregation]:
        ventilation_window = self._kwargs.get("ventilation_window")
        pafi_window = self._kwargs.get("pafi_window", "2h")
        if self._kwargs.get("ventilation_same_hour", False):
            ventilation = SameHourLocf()
        elif ventilation_window:
            ventilation = WindowedLocf(ventilation_window)
        else:
            ventilation = Locf()

        if self._kwargs.get("ricu_hourly_pafi", False):
            return {
                "__ricu_pafi": SameHourLocf(),
                "mechanical_ventilation_windows": ventilation,
            }

        return {
            "O2_partial_pressure": WindowedLocf(pafi_window),
            "fraction_of_inspired_oxygen": WindowedLocf(pafi_window),
            "mechanical_ventilation_windows": ventilation,
        }

    def _pafi(self) -> pl.Expr:
        if self._kwargs.get("ricu_hourly_pafi", False):
            return pl.col("__ricu_pafi")

        pao2 = pl.col("O2_partial_pressure")
        fio2 = pl.col("fraction_of_inspired_oxygen").fill_null(21.0)
        gas_measured = (
            self.measured("O2_partial_pressure")
            | self.measured("fraction_of_inspired_oxygen")
        )
        return (
            pl.when(gas_measured & pao2.is_not_null() & (fio2 > 0))
            .then(pao2 / fio2 * 100)
            .otherwise(None)
        )

    def observed(self) -> pl.Expr:
        return (
            self._pafi().is_not_null()
            | pl.col("mechanical_ventilation_windows").is_not_null()
        )

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
    """Total SOFA: the worst value of each sub-score within ``window``.

    For each component, the maximum score within the trailing window is used
    before the six component scores are summed. ``window`` defaults to 24h.
    A component with no value in the window contributes 0.
    """

    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        if not self.inputs:
            terms = self._kwargs.get("terms") or list(dependencies)
            self.inputs = {name: RollingMax(self.window) for name in terms}
        return super().transform(dependencies)
