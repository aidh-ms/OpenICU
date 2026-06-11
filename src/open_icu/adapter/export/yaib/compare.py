"""Compare OpenICU-generated dynamic tables with YAIB/RICU references."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .concepts import DYNAMIC_VARS


def scan_dyn(path: str | Path) -> pl.LazyFrame:
    """Scan a dynamic wide table parquet."""
    return pl.scan_parquet(path)


def schema_report(
    openicu: pl.LazyFrame,
    reference: pl.LazyFrame,
    dynamic_vars: list[str] | None = None,
) -> dict[str, list[str]]:
    """Report missing/unexpected dynamic columns in both tables."""
    vars_ = dynamic_vars or DYNAMIC_VARS
    expected = ["stay_id", "time"] + vars_
    open_cols = openicu.collect_schema().names()
    ref_cols = reference.collect_schema().names()

    return {
        "missing_in_openicu": [c for c in expected if c not in open_cols],
        "missing_in_reference": [c for c in expected if c not in ref_cols],
        "extra_in_openicu": [c for c in open_cols if c not in expected],
        "extra_in_reference": [c for c in ref_cols if c not in expected],
        "common_dynamic_vars": [c for c in vars_ if c in open_cols and c in ref_cols],
    }


def table_summary(df: pl.LazyFrame, name: str) -> pl.DataFrame:
    """Basic row/stay/time summary for one dynamic table."""
    return df.select(
        [
            pl.lit(name).alias("table"),
            pl.len().alias("n_rows"),
            pl.col("stay_id").n_unique().alias("n_stays"),
            pl.col("time").min().alias("min_time"),
            pl.col("time").max().alias("max_time"),
        ]
    ).collect()


def key_overlap_report(openicu: pl.LazyFrame, reference: pl.LazyFrame) -> pl.DataFrame:
    """Compare overlap of stay_id/time keys."""
    open_keys = openicu.select(["stay_id", "time"]).unique()
    ref_keys = reference.select(["stay_id", "time"]).unique()

    n_open = open_keys.select(pl.len()).collect().item()
    n_ref = ref_keys.select(pl.len()).collect().item()
    n_inner = open_keys.join(ref_keys, on=["stay_id", "time"], how="inner").select(pl.len()).collect().item()
    n_only_open = open_keys.join(ref_keys, on=["stay_id", "time"], how="anti").select(pl.len()).collect().item()
    n_only_ref = ref_keys.join(open_keys, on=["stay_id", "time"], how="anti").select(pl.len()).collect().item()

    return pl.DataFrame(
        {
            "n_openicu_keys": [n_open],
            "n_reference_keys": [n_ref],
            "n_common_keys": [n_inner],
            "n_only_openicu_keys": [n_only_open],
            "n_only_reference_keys": [n_only_ref],
        }
    )


def stay_overlap_report(openicu: pl.LazyFrame, reference: pl.LazyFrame) -> pl.DataFrame:
    """Compare overlap of stay_ids."""
    open_stays = openicu.select("stay_id").unique()
    ref_stays = reference.select("stay_id").unique()

    n_open = open_stays.select(pl.len()).collect().item()
    n_ref = ref_stays.select(pl.len()).collect().item()
    n_inner = open_stays.join(ref_stays, on="stay_id", how="inner").select(pl.len()).collect().item()
    n_only_open = open_stays.join(ref_stays, on="stay_id", how="anti").select(pl.len()).collect().item()
    n_only_ref = ref_stays.join(open_stays, on="stay_id", how="anti").select(pl.len()).collect().item()

    return pl.DataFrame(
        {
            "n_openicu_stays": [n_open],
            "n_reference_stays": [n_ref],
            "n_common_stays": [n_inner],
            "n_only_openicu_stays": [n_only_open],
            "n_only_reference_stays": [n_only_ref],
        }
    )


def coverage_report(
    openicu: pl.LazyFrame,
    reference: pl.LazyFrame,
    dynamic_vars: list[str] | None = None,
) -> pl.DataFrame:
    """Compare non-null counts per concept."""
    vars_ = dynamic_vars or DYNAMIC_VARS
    open_cols = set(openicu.collect_schema().names())
    ref_cols = set(reference.collect_schema().names())
    vars_ = [c for c in vars_ if c in open_cols and c in ref_cols]

    if not vars_:
        return pl.DataFrame(
            {
                "concept": [],
                "openicu_non_null": [],
                "reference_non_null": [],
                "diff_non_null": [],
                "ratio_openicu_to_reference": [],
            }
        )

    open_counts = (
        openicu.select([pl.col(c).is_not_null().sum().alias(c) for c in vars_])
        .collect()
        .transpose(include_header=True, header_name="concept", column_names=["openicu_non_null"])
    )
    ref_counts = (
        reference.select([pl.col(c).is_not_null().sum().alias(c) for c in vars_])
        .collect()
        .transpose(include_header=True, header_name="concept", column_names=["reference_non_null"])
    )

    return (
        open_counts.join(ref_counts, on="concept", how="inner")
        .with_columns(
            [
                (pl.col("openicu_non_null") - pl.col("reference_non_null")).alias("diff_non_null"),
                pl.when(pl.col("reference_non_null") > 0)
                .then(pl.col("openicu_non_null") / pl.col("reference_non_null"))
                .otherwise(None)
                .alias("ratio_openicu_to_reference"),
            ]
        )
        .sort("concept")
    )


def joined_on_common_keys(
    openicu: pl.LazyFrame,
    reference: pl.LazyFrame,
    dynamic_vars: list[str] | None = None,
) -> pl.LazyFrame:
    """Join both tables on stay_id/time and suffix reference columns with _ref."""
    vars_ = dynamic_vars or DYNAMIC_VARS
    open_cols = set(openicu.collect_schema().names())
    ref_cols = set(reference.collect_schema().names())
    vars_ = [c for c in vars_ if c in open_cols and c in ref_cols]

    return openicu.select(["stay_id", "time"] + vars_).join(
        reference.select(["stay_id", "time"] + vars_),
        on=["stay_id", "time"],
        how="inner",
        suffix="_ref",
    )


def missingness_report(
    openicu: pl.LazyFrame,
    reference: pl.LazyFrame,
    dynamic_vars: list[str] | None = None,
) -> pl.DataFrame:
    """Compare value presence/absence per concept on common stay_id/time keys."""
    vars_ = dynamic_vars or DYNAMIC_VARS
    joined = joined_on_common_keys(openicu, reference, vars_)
    cols = joined.collect_schema().names()
    vars_ = [c for c in vars_ if c in cols and f"{c}_ref" in cols]

    rows: list[pl.DataFrame] = []
    for c in vars_:
        row = joined.select(
            [
                pl.lit(c).alias("concept"),
                (pl.col(c).is_not_null() & pl.col(f"{c}_ref").is_not_null()).sum().alias("both_non_null"),
                (pl.col(c).is_not_null() & pl.col(f"{c}_ref").is_null()).sum().alias("only_openicu"),
                (pl.col(c).is_null() & pl.col(f"{c}_ref").is_not_null()).sum().alias("only_reference"),
                (pl.col(c).is_null() & pl.col(f"{c}_ref").is_null()).sum().alias("both_null"),
            ]
        ).collect()
        rows.append(row)

    return pl.concat(rows, how="vertical").sort("concept") if rows else pl.DataFrame()


def value_diff_report(
    openicu: pl.LazyFrame,
    reference: pl.LazyFrame,
    dynamic_vars: list[str] | None = None,
) -> pl.DataFrame:
    """Compare numeric differences for rows where both tables have a value."""
    vars_ = dynamic_vars or DYNAMIC_VARS
    joined = joined_on_common_keys(openicu, reference, vars_)
    cols = joined.collect_schema().names()
    vars_ = [c for c in vars_ if c in cols and f"{c}_ref" in cols]

    rows: list[pl.DataFrame] = []
    for c in vars_:
        diff = (pl.col(c) - pl.col(f"{c}_ref")).abs()
        rel = diff / pl.col(f"{c}_ref").abs()
        row = joined.filter(pl.col(c).is_not_null() & pl.col(f"{c}_ref").is_not_null()).select(
            [
                pl.lit(c).alias("concept"),
                pl.len().alias("n_both_non_null"),
                diff.mean().alias("mean_abs_diff"),
                diff.median().alias("median_abs_diff"),
                diff.max().alias("max_abs_diff"),
                rel.filter(pl.col(f"{c}_ref") != 0).mean().alias("mean_rel_diff"),
            ]
        ).collect()
        rows.append(row)

    return pl.concat(rows, how="vertical").sort("concept") if rows else pl.DataFrame()


def worst_examples(
    openicu: pl.LazyFrame,
    reference: pl.LazyFrame,
    concept: str,
    n: int = 20,
) -> pl.DataFrame:
    """Return largest absolute differences for one concept on common keys."""
    joined = joined_on_common_keys(openicu, reference, [concept])
    return (
        joined.filter(pl.col(concept).is_not_null() & pl.col(f"{concept}_ref").is_not_null())
        .with_columns(
            [
                (pl.col(concept) - pl.col(f"{concept}_ref")).alias("diff"),
                (pl.col(concept) - pl.col(f"{concept}_ref")).abs().alias("abs_diff"),
            ]
        )
        .sort("abs_diff", descending=True)
        .limit(n)
        .collect()
    )


def write_reports(
    output_dir: str | Path,
    reports: dict[str, pl.DataFrame],
) -> None:
    """Write report DataFrames as CSV files."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, df in reports.items():
        df.write_csv(out / f"{name}.csv")
