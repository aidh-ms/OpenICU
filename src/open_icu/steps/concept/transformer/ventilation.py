"""Mechanical ventilation window reconstruction."""

from __future__ import annotations

import polars as pl

from open_icu.steps.concept.transformer.base import BaseConceptTransformer


class MechanicalVentilationWindowsTransformer(BaseConceptTransformer):
    """Reconstruct hourly mechanical-ventilation indicators from start/end events.

    This mirrors the eICU RICU ``vent_ind`` semantics used for SOFA:

    - pair every ventilation start with the next end within 6 hours;
    - if no end matches, reproduce RICU's start-offset-plus-6h duration;
    - only episodes lasting at least 30 minutes are retained;
    - floor the start and duration independently to one-hour resolution;
    - expand the resulting window inclusively to hourly events.

    The one-minute shift of end timestamps is used for matching, as in RICU;
    the actual duration is calculated from the original end timestamp.
    """

    def __init__(
        self,
        *args,
        match_window: str = "6h",
        min_length_minutes: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._match_window = match_window
        self._min_length_minutes = min_length_minutes

    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        starts = (
            dependencies["ventilation_start"]
            .filter(pl.col("numeric_value") > 0)
            .select(
                pl.col("subject_id").cast(pl.Int64),
                pl.col("stay_id").cast(pl.Int64),
                pl.col("time").cast(pl.Datetime("us")).alias("__start"),
                pl.col("table").cast(pl.String),
            )
            .sort("subject_id", "stay_id", "__start")
        )

        ends = (
            dependencies["ventilation_end"]
            .filter(pl.col("numeric_value") > 0)
            .select(
                pl.col("subject_id").cast(pl.Int64),
                pl.col("stay_id").cast(pl.Int64),
                pl.col("time").cast(pl.Datetime("us")).alias("__end"),
            )
            .with_columns(
                (pl.col("__end") - pl.duration(minutes=1)).alias("__end_match")
            )
            .sort("subject_id", "stay_id", "__end_match")
        )

        admissions = (
            dependencies["icu_admission"]
            .filter(pl.col("numeric_value") > 0)
            .select(
                pl.col("subject_id").cast(pl.Int64),
                pl.col("stay_id").cast(pl.Int64),
                pl.col("time").cast(pl.Datetime("us")).alias("__admission"),
            )
            .unique(["subject_id", "stay_id"])
        )

        matched = (
            starts
            .join(
                admissions,
                on=["subject_id", "stay_id"],
                how="left",
            )
            .sort("subject_id", "stay_id", "__start")
            .join_asof(
                ends,
                left_on="__start",
                right_on="__end_match",
                by=["subject_id", "stay_id"],
                strategy="forward",
                tolerance=self._match_window,
            )
        )

        duration = (
            pl.when(pl.col("__end").is_not_null())
            .then(pl.col("__end") - pl.col("__start"))
            .otherwise(
                pl.col("__start").dt.offset_by(self._match_window)
                - pl.col("__admission")
            )
        )

        # RICU applies the >=30 min filter before changing to the 1h interval.
        # It then changes to the final interval and aggregates equal start
        # timestamps using the maximum duration before expanding the windows.
        matched = (
            matched
            .with_columns(duration.alias("__duration"))
            .filter(
                pl.col("__duration")
                >= pl.duration(minutes=self._min_length_minutes)
            )
            .with_columns(
                (
                    (
                        pl.col("__start") - pl.col("__admission")
                    )
                    .dt.total_minutes()
                    .floordiv(60)
                    .cast(pl.Int64)
                ).alias("__start_offset_hours"),
                (
                    pl.col("__duration")
                    .dt.total_minutes()
                    .floordiv(60)
                    .cast(pl.Int64)
                ).alias("__hours"),
            )
            .with_columns(
                (
                    pl.col("__admission")
                    + pl.duration(hours=pl.col("__start_offset_hours"))
                ).alias("__start_hour")
            )
            .group_by("stay_id", "__start_hour")
            .agg(
                pl.col("subject_id").first().alias("subject_id"),
                pl.col("__admission").first().alias("__admission"),
                pl.col("__start_offset_hours").first().alias("__start_offset_hours"),
                pl.col("__hours").max().alias("__hours"),
                pl.col("table").sort().first().alias("table"),
            )
            .with_columns(
                (
                    pl.col("__start_offset_hours")
                    + pl.col("__hours")
                ).alias("__end_offset_hours")
            )
            # RICU expand(win_tbl) clamps an interval ending before ICU
            # admission to hour 0 before expanding it.
            .with_columns(
                pl.when(pl.col("__end_offset_hours") < 0)
                .then(0)
                .otherwise(pl.col("__end_offset_hours"))
                .alias("__end_offset_hours")
            )
            .with_columns(
                pl.int_ranges(
                    pl.col("__start_offset_hours"),
                    pl.col("__end_offset_hours") + 1,
                ).alias("__hour_offset")
            )
            .explode("__hour_offset")
            .with_columns(
                (
                    pl.col("__admission")
                    + pl.duration(hours=pl.col("__hour_offset"))
                ).alias("time")
            )
        )

        # Several source starts can describe the same ventilation hour.
        # Collapse them to the single boolean state RICU exposes. Keep one
        # deterministic contributing source table for OpenICU provenance.
        return (
            matched
            .group_by("stay_id", "time")
            .agg(
                pl.col("subject_id").first().alias("subject_id"),
                pl.col("table").sort().first().alias("table"),
            )
            .with_columns(pl.lit(1.0, dtype=pl.Float32).alias("numeric_value"))
            .select("subject_id", "stay_id", "time", "numeric_value", "table")
            .sort("subject_id", "stay_id", "time")
        )
