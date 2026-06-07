"""Core OpenICU -> YAIB/RICU dynamic table transformation."""

from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import Literal

import polars as pl

from .concepts import DYNAMIC_VARS, RICU_TO_OPENICU
from .io import find_concept_file, scan_mimic_icustays, scan_openicu_concept
from .ricu_meta import RicuConceptMeta

AggregationMode = Literal["mean", "ricu"]
MissingConcepts = Literal["warn", "fail", "ignore"]


def _range_filter_expr(ricu_meta: RicuConceptMeta, ricu_name: str) -> pl.Expr:
    lower, upper = ricu_meta.range_for(ricu_name)
    expr = pl.col("numeric_value").is_not_null()
    if lower is not None:
        expr = expr & (pl.col("numeric_value") >= float(lower))
    if upper is not None:
        expr = expr & (pl.col("numeric_value") <= float(upper))
    return expr


def _agg_expr(ricu_name: str, output_col: str, aggregate: str) -> pl.Expr:
    aggregate = aggregate.lower()
    value = pl.col("numeric_value")
    if aggregate == "mean":
        return value.mean().alias(output_col)
    if aggregate == "median":
        return value.median().alias(output_col)
    if aggregate == "sum":
        return value.sum().alias(output_col)
    if aggregate == "min":
        return value.min().alias(output_col)
    if aggregate == "max":
        return value.max().alias(output_col)
    if aggregate == "first":
        return value.first().alias(output_col)
    if aggregate == "last":
        return value.last().alias(output_col)
    raise ValueError(f"Unsupported aggregation for {ricu_name!r}: {aggregate!r}")


def map_events_to_stays(events: pl.LazyFrame, stays: pl.LazyFrame) -> pl.LazyFrame:
    """Map subject-level OpenICU concept events to ICU stays.

    An event is assigned to a stay iff
    ``event.time >= intime`` and ``event.time <= outtime``.
    """
    return (
        events.join(stays, on="subject_id", how="inner")
        .filter(
            (pl.col("time") >= pl.col("intime"))
            & (pl.col("outtime").is_null() | (pl.col("time") <= pl.col("outtime")))
        )
        .with_columns(
            (
                (pl.col("time") - pl.col("intime"))
                .dt.total_seconds()
                .cast(pl.Float64)
                / 3600.0
            ).alias("diff_hours")
        )
        .with_columns(pl.col("diff_hours").round(0).cast(pl.Int64).alias("time"))
        .filter(pl.col("time") >= 0)
        .select("stay_id", "time", "numeric_value")
    )


def aggregate_concept_hourly(
    *,
    concept_file: str | Path,
    stays: pl.LazyFrame,
    ricu_name: str,
    ricu_meta: RicuConceptMeta,
    aggregation_mode: AggregationMode = "mean",
) -> pl.LazyFrame:
    """Load, range-filter, stay-map and aggregate one dynamic concept."""
    events = scan_openicu_concept(concept_file).filter(_range_filter_expr(ricu_meta, ricu_name))
    mapped = map_events_to_stays(events, stays)
    aggregate = "mean"
    if aggregation_mode == "ricu":
        aggregate = ricu_meta.aggregate_for(ricu_name, default="mean")

    return (
        mapped.group_by("stay_id", "time")
        .agg(_agg_expr(ricu_name=ricu_name, output_col=ricu_name, aggregate=aggregate))
        .select("stay_id", "time", ricu_name)
    )


def make_yaib_grid(stays: pl.LazyFrame, max_hours: int = 168) -> pl.LazyFrame:
    """Create a YAIB-like hourly stay grid.

    YAIB's base cohort maps dynamic variables to a grid derived from stay windows.
    This function creates rows ``time = 0, 1, ..., min(los_hours_ceil, max_hours)``
    for every ICU stay.
    """
    return (
        stays.with_columns(
            (
                (pl.col("outtime") - pl.col("intime"))
                .dt.total_seconds()
                .cast(pl.Float64)
                / 3600.0
            ).alias("los_hours")
        )
        .with_columns(
            pl.when(pl.col("los_hours").is_null() | (pl.col("los_hours") < 0))
            .then(0)
            .otherwise(pl.col("los_hours").ceil().cast(pl.Int64))
            .alias("los_hours_ceil")
        )
        .with_columns(pl.min_horizontal("los_hours_ceil", pl.lit(max_hours)).alias("end_time"))
        .select("stay_id", pl.int_ranges(0, pl.col("end_time") + 1).alias("time"))
        .explode("time")
        .with_columns(pl.col("time").cast(pl.Int64))
    )


def outer_join_concepts(concept_tables: list[pl.LazyFrame]) -> pl.LazyFrame:
    if not concept_tables:
        raise ValueError("No concept tables to join.")
    return reduce(
        lambda left, right: left.join(right, on=["stay_id", "time"], how="full", coalesce=True),
        concept_tables,
    )


def build_dynamic_table(
    *,
    concept_root: str | Path,
    icustays_csv: str | Path,
    ricu_concept_dict: str | Path | None = None,
    dataset: str = "mimic-iv",
    version: str | None = None,
    dynamic_vars: list[str] | None = None,
    concept_mapping: dict[str, str] | None = None,
    aggregation_mode: AggregationMode = "mean",
    include_grid: bool = True,
    max_hours: int = 168,
    missing_concepts: MissingConcepts = "warn",
) -> pl.LazyFrame:
    """Build the wide YAIB/RICU-like dynamic table.

    Returns a lazy frame with columns ``stay_id``, ``time`` and one column per
    successfully loaded dynamic concept abbreviation.
    """
    vars_ = dynamic_vars or DYNAMIC_VARS
    mapping = concept_mapping or RICU_TO_OPENICU
    ricu_meta = RicuConceptMeta.from_json(ricu_concept_dict)
    stays = scan_mimic_icustays(icustays_csv)

    concept_tables: list[pl.LazyFrame] = []
    missing: list[tuple[str, str]] = []

    for ricu_name in vars_:
        openicu_name = mapping.get(ricu_name)
        if openicu_name is None:
            missing.append((ricu_name, "no OpenICU mapping"))
            continue

        concept_file = find_concept_file(concept_root, openicu_name, dataset=dataset, version=version)
        if concept_file is None:
            missing.append((ricu_name, f"missing parquet for OpenICU concept {openicu_name!r}"))
            continue

        concept_tables.append(
            aggregate_concept_hourly(
                concept_file=concept_file,
                stays=stays,
                ricu_name=ricu_name,
                ricu_meta=ricu_meta,
                aggregation_mode=aggregation_mode,
            )
        )

    if missing:
        msg = "Missing concepts:\n" + "\n".join(f"- {k}: {reason}" for k, reason in missing)
        if missing_concepts == "fail":
            raise FileNotFoundError(msg)
        if missing_concepts == "warn":
            print(f"WARNING: {msg}")

    wide = outer_join_concepts(concept_tables)

    if include_grid:
        grid = make_yaib_grid(stays, max_hours=max_hours)
        wide = grid.join(wide, on=["stay_id", "time"], how="left")

    ordered_cols = ["stay_id", "time"] + [v for v in vars_ if v in wide.collect_schema().names()]
    return wide.select(ordered_cols).sort("stay_id", "time")


def write_dynamic_table(
    *,
    output_path: str | Path,
    **kwargs,
) -> None:
    """Build and write the dynamic table as parquet."""
    lf = build_dynamic_table(**kwargs)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(out)
