"""Compare OpenICU-generated dynamic tables with YAIB/RICU references."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .concepts import DYNAMIC_VARS


def scan_dyn(path: str | Path) -> pl.LazyFrame:
    return pl.scan_parquet(path)


def normalize_reference_columns(
    reference: pl.LazyFrame,
    *,
    id_col: str = "stay_id",
    time_col: str = "time",
) -> pl.LazyFrame:
    """Normalize a reference dynamic table to stay_id/time names.

    Use this for RICU exports where the time column is called charttime.
    """
    rename: dict[str, str] = {}
    if id_col != "stay_id":
        rename[id_col] = "stay_id"
    if time_col != "time":
        rename[time_col] = "time"
    lf = reference.rename(rename) if rename else reference
    return lf.with_columns([
        pl.col("stay_id").cast(pl.Int64),
        pl.col("time").cast(pl.Int64),
    ])


def schema_report(openicu: pl.LazyFrame, reference: pl.LazyFrame, dynamic_vars: list[str] | None = None) -> dict[str, list[str]]:
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
    return df.select([
        pl.lit(name).alias("table"),
        pl.len().alias("n_rows"),
        pl.col("stay_id").n_unique().alias("n_stays"),
        pl.col("time").min().alias("min_time"),
        pl.col("time").max().alias("max_time"),
    ]).collect()


def key_overlap_report(openicu: pl.LazyFrame, reference: pl.LazyFrame) -> pl.DataFrame:
    open_keys = openicu.select(["stay_id", "time"]).unique()
    ref_keys = reference.select(["stay_id", "time"]).unique()
    n_open = open_keys.select(pl.len()).collect().item()
    n_ref = ref_keys.select(pl.len()).collect().item()
    n_inner = open_keys.join(ref_keys, on=["stay_id", "time"], how="inner").select(pl.len()).collect().item()
    n_only_open = open_keys.join(ref_keys, on=["stay_id", "time"], how="anti").select(pl.len()).collect().item()
    n_only_ref = ref_keys.join(open_keys, on=["stay_id", "time"], how="anti").select(pl.len()).collect().item()
    return pl.DataFrame({
        "n_openicu_keys": [n_open],
        "n_reference_keys": [n_ref],
        "n_common_keys": [n_inner],
        "n_only_openicu_keys": [n_only_open],
        "n_only_reference_keys": [n_only_ref],
    })


def stay_overlap_report(openicu: pl.LazyFrame, reference: pl.LazyFrame) -> pl.DataFrame:
    open_stays = openicu.select("stay_id").unique()
    ref_stays = reference.select("stay_id").unique()
    return pl.DataFrame({
        "n_openicu_stays": [open_stays.select(pl.len()).collect().item()],
        "n_reference_stays": [ref_stays.select(pl.len()).collect().item()],
        "n_common_stays": [open_stays.join(ref_stays, on="stay_id", how="inner").select(pl.len()).collect().item()],
        "n_only_openicu_stays": [open_stays.join(ref_stays, on="stay_id", how="anti").select(pl.len()).collect().item()],
        "n_only_reference_stays": [ref_stays.join(open_stays, on="stay_id", how="anti").select(pl.len()).collect().item()],
    })


def coverage_report(openicu: pl.LazyFrame, reference: pl.LazyFrame, dynamic_vars: list[str] | None = None) -> pl.DataFrame:
    vars_ = dynamic_vars or DYNAMIC_VARS
    open_cols = set(openicu.collect_schema().names())
    ref_cols = set(reference.collect_schema().names())
    rows: list[dict[str, object]] = []
    for c in vars_:
        if c not in open_cols or c not in ref_cols:
            continue
        open_non_null = openicu.select(pl.col(c).is_not_null().sum()).collect().item()
        ref_non_null = reference.select(pl.col(c).is_not_null().sum()).collect().item()
        rows.append({
            "concept": c,
            "openicu_non_null": int(open_non_null),
            "reference_non_null": int(ref_non_null),
            "diff_non_null": int(open_non_null) - int(ref_non_null),
        })
    return pl.DataFrame(rows).sort("concept") if rows else pl.DataFrame()


def joined_on_common_keys(openicu: pl.LazyFrame, reference: pl.LazyFrame, dynamic_vars: list[str] | None = None) -> pl.LazyFrame:
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


def missingness_report(openicu: pl.LazyFrame, reference: pl.LazyFrame, dynamic_vars: list[str] | None = None) -> pl.DataFrame:
    vars_ = dynamic_vars or DYNAMIC_VARS
    joined = joined_on_common_keys(openicu, reference, vars_)
    cols = joined.collect_schema().names()
    rows: list[pl.DataFrame] = []
    for c in vars_:
        if c not in cols or f"{c}_ref" not in cols:
            continue
        rows.append(joined.select([
            pl.lit(c).alias("concept"),
            (pl.col(c).is_not_null() & pl.col(f"{c}_ref").is_not_null()).sum().alias("both_non_null"),
            (pl.col(c).is_not_null() & pl.col(f"{c}_ref").is_null()).sum().alias("only_openicu"),
            (pl.col(c).is_null() & pl.col(f"{c}_ref").is_not_null()).sum().alias("only_reference"),
            (pl.col(c).is_null() & pl.col(f"{c}_ref").is_null()).sum().alias("both_null"),
        ]).collect())
    return pl.concat(rows, how="vertical").sort("concept") if rows else pl.DataFrame()


def value_diff_report(openicu: pl.LazyFrame, reference: pl.LazyFrame, dynamic_vars: list[str] | None = None) -> pl.DataFrame:
    vars_ = dynamic_vars or DYNAMIC_VARS
    joined = joined_on_common_keys(openicu, reference, vars_)
    cols = joined.collect_schema().names()
    rows: list[pl.DataFrame] = []
    for c in vars_:
        if c not in cols or f"{c}_ref" not in cols:
            continue
        diff = (pl.col(c) - pl.col(f"{c}_ref")).abs()
        rows.append(
            joined.filter(pl.col(c).is_not_null() & pl.col(f"{c}_ref").is_not_null())
            .select([
                pl.lit(c).alias("concept"),
                pl.len().alias("n_both_non_null"),
                diff.mean().alias("mean_abs_diff"),
                diff.median().alias("median_abs_diff"),
                diff.max().alias("max_abs_diff"),
                (diff / pl.col(f"{c}_ref").abs()).filter(pl.col(f"{c}_ref") != 0).mean().alias("mean_rel_diff"),
            ]).collect()
        )
    return pl.concat(rows, how="vertical").sort("concept") if rows else pl.DataFrame()


def worst_examples(openicu: pl.LazyFrame, reference: pl.LazyFrame, concept: str, n: int = 20) -> pl.DataFrame:
    joined = joined_on_common_keys(openicu, reference, [concept])
    ref_col = f"{concept}_ref"
    return (
        joined.filter(pl.col(concept).is_not_null() & pl.col(ref_col).is_not_null())
        .with_columns((pl.col(concept) - pl.col(ref_col)).abs().alias("abs_diff"))
        .select(["stay_id", "time", pl.col(concept).alias("openicu_value"), pl.col(ref_col).alias("reference_value"), "abs_diff"])
        .sort("abs_diff", descending=True)
        .limit(n)
        .collect()
    )


def to_long_non_null(df: pl.LazyFrame, dynamic_vars: list[str] | None = None, value_name: str = "value") -> pl.LazyFrame:
    vars_ = dynamic_vars or DYNAMIC_VARS
    cols = set(df.collect_schema().names())
    present = [c for c in vars_ if c in cols]
    return (
        df.select(["stay_id", "time"] + present)
        .unpivot(index=["stay_id", "time"], on=present, variable_name="concept", value_name=value_name)
        .filter(pl.col(value_name).is_not_null())
    )


def reference_only_values(openicu: pl.LazyFrame, reference: pl.LazyFrame, dynamic_vars: list[str] | None = None) -> pl.LazyFrame:
    open_long = to_long_non_null(openicu, dynamic_vars, value_name="openicu_value")
    ref_long = to_long_non_null(reference, dynamic_vars, value_name="reference_value")
    return ref_long.join(open_long.select(["stay_id", "time", "concept"]), on=["stay_id", "time", "concept"], how="anti")


def openicu_only_values(openicu: pl.LazyFrame, reference: pl.LazyFrame, dynamic_vars: list[str] | None = None) -> pl.LazyFrame:
    open_long = to_long_non_null(openicu, dynamic_vars, value_name="openicu_value")
    ref_long = to_long_non_null(reference, dynamic_vars, value_name="reference_value")
    return open_long.join(ref_long.select(["stay_id", "time", "concept"]), on=["stay_id", "time", "concept"], how="anti")


def write_reports(
    *,
    openicu_path: str | Path,
    reference_path: str | Path,
    output_dir: str | Path,
    dynamic_vars: list[str] | None = None,
) -> None:
    """Write standard CSV comparison reports."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    openicu = scan_dyn(openicu_path)
    reference = scan_dyn(reference_path)
    pl.concat([table_summary(openicu, "openicu"), table_summary(reference, "reference")]).write_csv(out / "table_summary.csv")
    key_overlap_report(openicu, reference).write_csv(out / "key_overlap.csv")
    stay_overlap_report(openicu, reference).write_csv(out / "stay_overlap.csv")
    coverage_report(openicu, reference, dynamic_vars).write_csv(out / "coverage.csv")
    missingness_report(openicu, reference, dynamic_vars).write_csv(out / "missingness.csv")
    value_diff_report(openicu, reference, dynamic_vars).write_csv(out / "value_diff.csv")
