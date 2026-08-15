from typing import TYPE_CHECKING

import polars as pl

from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.transformer.base import BaseConceptTransformer

if TYPE_CHECKING:
    from open_icu.steps.concept.config.concept import ConceptConfig
    from open_icu.steps.concept.step import ConceptStep


class SOFARenalTransformer(BaseConceptTransformer):
    def __init__(
        self,
        concept: "ConceptConfig",
        complex_config: ComplexDatasetConceptConfig,
        step: "ConceptStep",
        **kwargs
    ):
        self._concept = concept
        self._complex_config = complex_config
        self._step: "ConceptStep" = step
        self._kwargs = kwargs

    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        creatinine_lf = dependencies.get("creatinine")
        assert creatinine_lf is not None
        creatinine_lf = creatinine_lf.select(["subject_id", "time", "numeric_value"]).sort("subject_id", "time")

        urine_output_lf = dependencies.get("urine_output")
        assert urine_output_lf is not None

        uo_gap_hours = self._kwargs.get("urine_output_gap_hours", 24)
        urine_output_lf = urine_output_lf.select(["subject_id", "time", "numeric_value"]).sort("subject_id", "time").with_columns(
            (pl.col("time") - pl.col("time").shift(1).over("subject_id")
                > pl.duration(hours=uo_gap_hours))
            .fill_null(True)  # first reading per subject always starts a segment
            .alias("new_segment")
        ).with_columns(
            pl.col("new_segment").cum_sum().over("subject_id").alias("segment_id")
        ).with_columns(
            pl.col("time").min().over("subject_id", "segment_id").alias("segment_start"),
            pl.col("time").alias("last_uo_time"),
        )

        timeline = (
            pl.concat([creatinine_lf.select("subject_id", "time"),
                    urine_output_lf.select("subject_id", "time")])
            .unique()
            .sort("subject_id", "time")
        )

        creatinine_score_lf = timeline.join_asof(
            creatinine_lf, on="time", by="subject_id",
            strategy="backward", tolerance="24h",
        ).with_columns(
            pl.when(pl.col("numeric_value") >= 5.0).then(4)
            .when(pl.col("numeric_value") >= 3.5).then(3)
            .when(pl.col("numeric_value") >= 2.0).then(2)
            .when(pl.col("numeric_value") >= 1.2).then(1)
            .when(pl.col("numeric_value").is_not_null()).then(0)
            .otherwise(None).alias("creatinine_score"),
        ).select("subject_id", "time", "creatinine_score")

        uo_sum_lf = (
            pl.concat([
                urine_output_lf.select("subject_id", "time", "numeric_value", pl.lit(False).alias("is_query")),
                timeline.select(
                    "subject_id", "time",
                    pl.lit(0.0, dtype=pl.Float32).alias("numeric_value"),
                    pl.lit(True).alias("is_query"),
                ),
            ])
            .sort("subject_id", "time")
            .with_columns(
                pl.col("numeric_value")
                .rolling_sum_by(by="time", window_size="24h", closed="right")
                .over("subject_id")
                .alias("uo_sum_24h")
            )
            .filter(pl.col("is_query"))
            .select("subject_id", "time", "uo_sum_24h")
        )

        uo_status_lf = timeline.join_asof(
            urine_output_lf.select("subject_id", "time", "segment_start", "last_uo_time"),
            on="time", by="subject_id", strategy="backward",
        ).with_columns(
            (
                (pl.col("time") - pl.col("segment_start") >= pl.duration(hours=24))
                & (pl.col("time") - pl.col("last_uo_time") <= pl.duration(hours=uo_gap_hours))
            ).fill_null(False).alias("uo_window_complete")
        ).select("subject_id", "time", "uo_window_complete")

        uo_score_lf = (
            uo_sum_lf
            .join(uo_status_lf, on=["subject_id", "time"], how="left")
            .with_columns(
                pl.when(~pl.col("uo_window_complete")).then(None)
                .when(pl.col("uo_sum_24h") < 200).then(4)
                .when(pl.col("uo_sum_24h") < 500).then(3)
                .otherwise(0)
                .alias("uo_score")
            )
            .select("subject_id", "time", "uo_score")
        )

        lf = (
            timeline
            .join(creatinine_score_lf, on=["subject_id", "time"], how="left")
            .join(uo_score_lf, on=["subject_id", "time"], how="left")
            .with_columns(
                pl.max_horizontal("creatinine_score", "uo_score").alias("numeric_value")
            )
        )

        return lf.select("subject_id", "time", "numeric_value")
