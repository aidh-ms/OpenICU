"""Compare result Concept Parquet outputs from two OpenICU runs.

This module is intentionally independent from the RICU conversion logic. It can
compare outputs generated from old hand-written configs with outputs generated
from RICU-derived configs.

For simple concepts, a Parquet comparison is usually sufficient. For derived and
complex concepts, the same output comparison still applies, but this module can
also inspect OpenICU dataset concept YAML files and verify that required input
concept outputs exist.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import polars as pl
import yaml


@dataclass(slots=True)
class DependencyCheck:
    """Availability status for one dependency of a derived/complex concept."""

    concept: str
    dataset_file: str
    exists_in_old: bool | None
    exists_in_new: bool
    old_path: str | None
    new_path: str


@dataclass(slots=True)
class ConceptOutputDiff:
    """Comparison result for one relative Parquet output path."""

    path: str
    status: str
    concept: str | None = None
    version: str | None = None
    dataset_file: str | None = None
    concept_type: str | None = None
    old_rows: int | None = None
    new_rows: int | None = None
    old_schema: dict[str, str] | None = None
    new_schema: dict[str, str] | None = None
    schema_diff: list[str] = field(default_factory=list)
    missing_in_new: int | None = None
    missing_in_old: int | None = None
    changed_values: int | None = None
    dependency_checks: list[DependencyCheck] = field(default_factory=list)
    details: list[str] = field(default_factory=list)


def collect_parquet_files(root: Path) -> dict[str, Path]:
    """Collect Parquet files under a root directory by relative POSIX path."""

    return {
        path.relative_to(root).as_posix(): path
        for path in root.rglob("*.parquet")
        if path.is_file()
    }


def parse_concept_output_path(rel_path: str) -> tuple[str | None, str | None, str | None]:
    """Parse `<concept>/<version>/<dataset>.parquet` relative output paths."""

    parts = Path(rel_path).parts
    if len(parts) < 3:
        return None, None, None

    concept = parts[0]
    version = parts[1]
    dataset_file = parts[-1]
    return concept, version, dataset_file


def schema_to_str_dict(schema: dict[str, pl.DataType]) -> dict[str, str]:
    return {name: str(dtype) for name, dtype in schema.items()}


def compare_schema(old_schema: dict[str, str], new_schema: dict[str, str]) -> list[str]:
    diffs: list[str] = []

    old_cols = set(old_schema)
    new_cols = set(new_schema)

    for col in sorted(old_cols - new_cols):
        diffs.append(f"removed column: {col} ({old_schema[col]})")

    for col in sorted(new_cols - old_cols):
        diffs.append(f"added column: {col} ({new_schema[col]})")

    for col in sorted(old_cols & new_cols):
        if old_schema[col] != new_schema[col]:
            diffs.append(f"type changed: {col}: {old_schema[col]} -> {new_schema[col]}")

    return diffs


def _safe_read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return data if isinstance(data, dict) else {}


def _normalize_dependency_name(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return str(value)
    return value.removeprefix("concept/")


def extract_dependencies_from_dataset_concept_config(data: dict[str, Any]) -> list[str]:
    """Extract input dependencies from one dataset concept YAML payload.

    Supported structures:
    - derived: `table.concept` and all `join[].concept`
    - complex: top-level `concepts`

    Simple concepts return an empty list.
    """

    concept_type = data.get("type")
    dependencies: list[str] = []

    if concept_type == "derived":
        table = data.get("table")
        if isinstance(table, dict):
            dependency = _normalize_dependency_name(table.get("concept"))
            if dependency:
                dependencies.append(dependency)

        joins = data.get("join", [])
        if isinstance(joins, list):
            for join in joins:
                if isinstance(join, dict):
                    dependency = _normalize_dependency_name(join.get("concept"))
                    if dependency:
                        dependencies.append(dependency)

    elif concept_type == "complex":
        concepts = data.get("concepts", [])
        if isinstance(concepts, str):
            concepts = [concepts]
        if isinstance(concepts, list):
            for concept in concepts:
                dependency = _normalize_dependency_name(concept)
                if dependency:
                    dependencies.append(dependency)

    # Stable order while preserving first occurrence.
    seen: set[str] = set()
    unique: list[str] = []
    for dependency in dependencies:
        if dependency not in seen:
            seen.add(dependency)
            unique.append(dependency)
    return unique


def load_concept_config_index(config_root: Path | None) -> dict[str, dict[str, Any]]:
    """Index dataset concept YAML files by concept folder/stem.

    Expected OpenICU layout:
    `config/dataset/<dataset>/<version>/concept/<concept>.yml`

    The returned dict maps concept name to metadata. If the same concept occurs
    for several datasets, the last one found wins for `type`, but dependencies
    are merged. This is sufficient for report classification.
    """

    if config_root is None or not config_root.exists():
        return {}

    index: dict[str, dict[str, Any]] = {}
    for path in sorted(list(config_root.glob("dataset/*/*/concept/*.yml")) + list(config_root.glob("dataset/*/*/concept/*.yaml"))):
        data = _safe_read_yaml(path)
        concept = path.stem
        item = index.setdefault(
            concept,
            {
                "concept_type": None,
                "dependencies": [],
                "config_paths": [],
            },
        )
        item["concept_type"] = data.get("type", item["concept_type"])
        item["config_paths"].append(path.as_posix())

        for dependency in extract_dependencies_from_dataset_concept_config(data):
            if dependency not in item["dependencies"]:
                item["dependencies"].append(dependency)

    return index


def make_dependency_checks(
    *,
    concept: str | None,
    dataset_file: str | None,
    old_root: Path | None,
    new_root: Path,
    config_index: dict[str, dict[str, Any]],
) -> list[DependencyCheck]:
    """Check whether derived/complex dependencies have result Parquets."""

    if not concept or not dataset_file:
        return []

    metadata = config_index.get(concept, {})
    dependencies = metadata.get("dependencies", [])
    if not dependencies:
        return []

    checks: list[DependencyCheck] = []
    for dependency in dependencies:
        new_path = new_root / dependency / "1.0.0" / dataset_file
        old_path = old_root / dependency / "1.0.0" / dataset_file if old_root else None
        checks.append(
            DependencyCheck(
                concept=dependency,
                dataset_file=dataset_file,
                exists_in_old=old_path.exists() if old_path else None,
                exists_in_new=new_path.exists(),
                old_path=old_path.as_posix() if old_path else None,
                new_path=new_path.as_posix(),
            )
        )
    return checks


def normalize_df_for_set_compare(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize dtypes that make equality comparison noisy."""

    exprs = []
    for col, dtype in df.schema.items():
        if dtype in {pl.Categorical, pl.Enum}:
            exprs.append(pl.col(col).cast(pl.Utf8).alias(col))
    if exprs:
        df = df.with_columns(exprs)
    return df


def drop_ignored_columns(df: pl.DataFrame, ignore_columns: list[str] | None) -> pl.DataFrame:
    """Drop columns that should be excluded from schema and value comparison.

    Missing ignored columns are silently skipped. This is useful when old
    concept outputs do not yet contain extension columns such as ``dataset``
    or ``table`` while newly generated outputs do.
    """

    if not ignore_columns:
        return df

    existing = [column for column in ignore_columns if column in df.columns]
    if not existing:
        return df

    return df.drop(existing)


def normalize_code_for_compare(df: pl.DataFrame, mode: str | None) -> pl.DataFrame:
    """Normalize the ``code`` column before comparing outputs.

    The normalization is intentionally applied to the actual ``code`` column,
    not to an auxiliary column. This means existing commands using
    ``--key ... code`` automatically compare the normalized representation.

    Supported modes
    ---------------
    ``hyphen-to-underscore``:
        Replace ``-`` by ``_``. This treats e.g.
        ``C-reactive_protein//mg/L`` and ``C_reactive_protein//mg/L`` as equal.
    ``basic``:
        Lower-case, replace hyphens and whitespace by underscores.
    """

    if mode is None or "code" not in df.columns:
        return df

    expr = pl.col("code")
    if mode == "hyphen-to-underscore":
        expr = expr.str.replace_all("-", "_")
    elif mode == "basic":
        expr = expr.str.to_lowercase().str.replace_all("-", "_").str.replace_all(r"\s+", "_")
    else:
        raise ValueError(
            "Unknown code normalization mode: "
            f"{mode!r}. Expected one of: 'hyphen-to-underscore', 'basic'."
        )

    return df.with_columns(expr.alias("code"))


def prepare_df_for_comparison(
    df: pl.DataFrame,
    *,
    ignore_columns: list[str] | None,
    normalize_code: str | None,
) -> pl.DataFrame:
    """Apply all user-selected comparison normalizations."""

    df = drop_ignored_columns(df, ignore_columns)
    df = normalize_code_for_compare(df, normalize_code)
    return df


def compare_without_keys(old_df: pl.DataFrame, new_df: pl.DataFrame, rel_path: str) -> ConceptOutputDiff:
    old_schema = schema_to_str_dict(old_df.schema)
    new_schema = schema_to_str_dict(new_df.schema)
    schema_diff = compare_schema(old_schema, new_schema)

    concept, version, dataset_file = parse_concept_output_path(rel_path)
    result = ConceptOutputDiff(
        path=rel_path,
        status="unchanged",
        concept=concept,
        version=version,
        dataset_file=dataset_file,
        old_rows=old_df.height,
        new_rows=new_df.height,
        old_schema=old_schema,
        new_schema=new_schema,
        schema_diff=schema_diff,
    )

    if schema_diff:
        result.status = "schema_changed"
        result.details.extend(schema_diff)
        return result

    old_norm = normalize_df_for_set_compare(old_df)
    new_norm = normalize_df_for_set_compare(new_df)

    # Sorting by every column only works when all columns are sortable. If not,
    # Polars raises and we fall back to unique + anti-join directly.
    try:
        old_norm = old_norm.sort(old_norm.columns)
        new_norm = new_norm.sort(new_norm.columns)
    except Exception:
        pass

    if old_norm.equals(new_norm):
        return result

    result.status = "changed"
    old_unique = old_norm.unique()
    new_unique = new_norm.unique()

    common_cols = [col for col in old_unique.columns if col in new_unique.columns]
    if set(old_unique.columns) == set(new_unique.columns) and common_cols:
        missing_in_new = old_unique.join(new_unique, on=common_cols, how="anti")
        missing_in_old = new_unique.join(old_unique, on=common_cols, how="anti")
        result.missing_in_new = missing_in_new.height
        result.missing_in_old = missing_in_old.height
    else:
        result.missing_in_new = None
        result.missing_in_old = None

    if old_df.height != new_df.height:
        result.details.append(f"row count changed: {old_df.height} -> {new_df.height}")
    if result.missing_in_new is not None:
        result.details.append(f"rows only in old: {result.missing_in_new}")
    if result.missing_in_old is not None:
        result.details.append(f"rows only in new: {result.missing_in_old}")
    return result


def _changed_expr_for_column(old_col: str, new_col: str, dtype: pl.DataType, tolerance: float) -> pl.Expr:
    both_null = pl.col(old_col).is_null() & pl.col(new_col).is_null()
    if dtype.is_float():
        return ((pl.col(old_col) - pl.col(new_col)).abs() > tolerance) & ~both_null
    return (pl.col(old_col) != pl.col(new_col)) & ~both_null


def compare_with_keys(
    old_df: pl.DataFrame,
    new_df: pl.DataFrame,
    rel_path: str,
    key_columns: list[str],
    tolerance: float,
) -> ConceptOutputDiff:
    old_schema = schema_to_str_dict(old_df.schema)
    new_schema = schema_to_str_dict(new_df.schema)
    schema_diff = compare_schema(old_schema, new_schema)

    concept, version, dataset_file = parse_concept_output_path(rel_path)
    result = ConceptOutputDiff(
        path=rel_path,
        status="unchanged",
        concept=concept,
        version=version,
        dataset_file=dataset_file,
        old_rows=old_df.height,
        new_rows=new_df.height,
        old_schema=old_schema,
        new_schema=new_schema,
        schema_diff=schema_diff,
    )

    missing_keys = [col for col in key_columns if col not in old_df.columns or col not in new_df.columns]
    if missing_keys:
        result.status = "missing_key_columns"
        result.details.append(f"missing key columns: {missing_keys}")
        return result

    if schema_diff:
        result.status = "schema_changed"
        result.details.extend(schema_diff)
        return result

    compare_columns = [col for col in old_df.columns if col not in key_columns and col in new_df.columns]

    old_keys = old_df.select(key_columns).unique()
    new_keys = new_df.select(key_columns).unique()
    missing_in_new = old_keys.join(new_keys, on=key_columns, how="anti")
    missing_in_old = new_keys.join(old_keys, on=key_columns, how="anti")
    result.missing_in_new = missing_in_new.height
    result.missing_in_old = missing_in_old.height

    old_unique_keys = old_keys.height
    new_unique_keys = new_keys.height
    if old_unique_keys != old_df.height:
        result.details.append(f"old has duplicate keys: rows={old_df.height}, unique_keys={old_unique_keys}")
    if new_unique_keys != new_df.height:
        result.details.append(f"new has duplicate keys: rows={new_df.height}, unique_keys={new_unique_keys}")

    # If keys are not unique, inner join can fan out. This is still useful for
    # diagnostics, but the duplicate warning above is essential.
    joined = old_df.join(new_df, on=key_columns, how="inner", suffix="__new")

    changed_count = 0
    for col in compare_columns:
        new_col = f"{col}__new"
        if new_col not in joined.columns:
            continue
        changed_expr = _changed_expr_for_column(col, new_col, old_df.schema[col], tolerance)
        n_changed = joined.select(changed_expr.sum().alias("n")).item()
        if n_changed:
            changed_count += int(n_changed)
            result.details.append(f"{col}: {n_changed} changed values")

    result.changed_values = changed_count

    if old_df.height != new_df.height:
        result.details.append(f"row count changed: {old_df.height} -> {new_df.height}")
    if missing_in_new.height:
        result.details.append(f"keys only in old: {missing_in_new.height}")
    if missing_in_old.height:
        result.details.append(f"keys only in new: {missing_in_old.height}")

    if old_df.height != new_df.height or missing_in_new.height or missing_in_old.height or changed_count:
        result.status = "changed"

    return result


def compare_parquet_file(
    *,
    old_path: Path,
    new_path: Path,
    rel_path: str,
    key_columns: list[str] | None,
    tolerance: float,
    ignore_columns: list[str] | None = None,
    normalize_code: str | None = None,
) -> ConceptOutputDiff:
    old_df = pl.read_parquet(old_path)
    new_df = pl.read_parquet(new_path)

    old_df = prepare_df_for_comparison(
        old_df,
        ignore_columns=ignore_columns,
        normalize_code=normalize_code,
    )
    new_df = prepare_df_for_comparison(
        new_df,
        ignore_columns=ignore_columns,
        normalize_code=normalize_code,
    )

    if key_columns:
        return compare_with_keys(old_df, new_df, rel_path, key_columns, tolerance)
    return compare_without_keys(old_df, new_df, rel_path)


def attach_metadata_and_dependencies(
    *,
    result: ConceptOutputDiff,
    old_root: Path | None,
    new_root: Path,
    config_index: dict[str, dict[str, Any]],
    check_dependencies: bool,
) -> ConceptOutputDiff:
    if result.concept is None:
        result.concept, result.version, result.dataset_file = parse_concept_output_path(result.path)

    if result.concept and result.concept in config_index:
        result.concept_type = config_index[result.concept].get("concept_type")

    if check_dependencies and result.concept_type in {"derived", "complex"}:
        checks = make_dependency_checks(
            concept=result.concept,
            dataset_file=result.dataset_file,
            old_root=old_root,
            new_root=new_root,
            config_index=config_index,
        )
        result.dependency_checks = checks
        missing_new = [check.concept for check in checks if not check.exists_in_new]
        if missing_new:
            result.details.append(f"missing new dependency outputs: {missing_new}")
            if result.status == "unchanged":
                result.status = "dependency_missing"
    return result


def compare_output_dirs(
    *,
    old_root: str | Path,
    new_root: str | Path,
    key_columns: list[str] | None = None,
    tolerance: float = 1e-9,
    config_root: str | Path | None = None,
    check_dependencies: bool = True,
    concept_type_filter: set[str] | None = None,
    ignore_columns: list[str] | None = None,
    normalize_code: str | None = None,
) -> list[ConceptOutputDiff]:
    """Compare two OpenICU concept output directories.

    Parameters
    ----------
    old_root, new_root:
        Roots containing `<concept>/1.0.0/<dataset>.parquet` files.
    key_columns:
        Optional row-alignment keys. Without keys, full rows are compared as sets.
    config_root:
        Optional OpenICU config directory. If supplied, report entries are
        enriched with `concept_type` and derived/complex dependency checks.
    concept_type_filter:
        Optional set like `{ "derived", "complex" }` to return only selected
        concept types. Files with unknown type are excluded when a filter is set.
    ignore_columns:
        Columns to remove from both old and new before schema/value comparison.
        Useful for extension columns that only exist in one output generation.
    normalize_code:
        Optional normalization mode for the `code` column. Supported values are
        `hyphen-to-underscore` and `basic`.
    """

    old_root = Path(old_root).resolve()
    new_root = Path(new_root).resolve()
    config_root_path = Path(config_root).resolve() if config_root is not None else None

    old_files = collect_parquet_files(old_root)
    new_files = collect_parquet_files(new_root)
    config_index = load_concept_config_index(config_root_path)

    results: list[ConceptOutputDiff] = []
    for rel_path in sorted(set(old_files) | set(new_files)):
        old_path = old_files.get(rel_path)
        new_path = new_files.get(rel_path)
        concept, version, dataset_file = parse_concept_output_path(rel_path)

        if old_path is None:
            result = ConceptOutputDiff(
                path=rel_path,
                status="added_file",
                concept=concept,
                version=version,
                dataset_file=dataset_file,
                details=[f"only exists in new: {new_path}"],
            )
        elif new_path is None:
            result = ConceptOutputDiff(
                path=rel_path,
                status="removed_file",
                concept=concept,
                version=version,
                dataset_file=dataset_file,
                details=[f"only exists in old: {old_path}"],
            )
        else:
            try:
                result = compare_parquet_file(
                    old_path=old_path,
                    new_path=new_path,
                    rel_path=rel_path,
                    key_columns=key_columns,
                    tolerance=tolerance,
                    ignore_columns=ignore_columns,
                    normalize_code=normalize_code,
                )
            except Exception as exc:  # pragma: no cover - diagnostic path
                result = ConceptOutputDiff(
                    path=rel_path,
                    status="error",
                    concept=concept,
                    version=version,
                    dataset_file=dataset_file,
                    details=[repr(exc)],
                )

        result = attach_metadata_and_dependencies(
            result=result,
            old_root=old_root,
            new_root=new_root,
            config_index=config_index,
            check_dependencies=check_dependencies,
        )

        if concept_type_filter and result.concept_type not in concept_type_filter:
            continue
        results.append(result)

    return results


def results_to_dicts(results: list[ConceptOutputDiff]) -> list[dict[str, Any]]:
    return [asdict(result) for result in results]


def write_json_report(results: list[ConceptOutputDiff], output: str | Path) -> Path:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        json.dump(results_to_dicts(results), handle, indent=2, ensure_ascii=False)
    return output


def write_markdown_report(results: list[ConceptOutputDiff], output: str | Path) -> Path:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    changed = [result for result in results if result.status != "unchanged"]
    counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
        ctype = result.concept_type or "unknown"
        type_counts[ctype] = type_counts.get(ctype, 0) + 1

    with output.open("w", encoding="utf-8") as handle:
        handle.write("# Concept Output Comparison Report\n\n")
        handle.write("## Summary\n\n")
        handle.write(f"- Compared Parquet files: {len(results)}\n")
        handle.write(f"- Changed/new/removed/error files: {len(changed)}\n")
        handle.write(f"- Unchanged files: {len(results) - len(changed)}\n\n")

        handle.write("## Status counts\n\n")
        for status, count in sorted(counts.items()):
            handle.write(f"- `{status}`: {count}\n")

        handle.write("\n## Concept type counts\n\n")
        for concept_type, count in sorted(type_counts.items()):
            handle.write(f"- `{concept_type}`: {count}\n")

        handle.write("\n## Details\n\n")
        for result in changed:
            handle.write(f"### `{result.path}`\n\n")
            handle.write(f"- Status: `{result.status}`\n")
            if result.concept_type:
                handle.write(f"- Concept type: `{result.concept_type}`\n")
            if result.old_rows is not None or result.new_rows is not None:
                handle.write(f"- Rows: `{result.old_rows}` -> `{result.new_rows}`\n")
            if result.missing_in_new is not None:
                handle.write(f"- Keys/rows only in old: `{result.missing_in_new}`\n")
            if result.missing_in_old is not None:
                handle.write(f"- Keys/rows only in new: `{result.missing_in_old}`\n")
            if result.changed_values is not None:
                handle.write(f"- Changed values: `{result.changed_values}`\n")

            if result.dependency_checks:
                handle.write("\nDependencies:\n")
                for check in result.dependency_checks:
                    handle.write(
                        f"- `{check.concept}`: old=`{check.exists_in_old}`, new=`{check.exists_in_new}`\n"
                    )

            if result.schema_diff:
                handle.write("\nSchema differences:\n")
                for diff in result.schema_diff:
                    handle.write(f"- `{diff}`\n")

            if result.details:
                handle.write("\nDetails:\n")
                for detail in result.details[:80]:
                    handle.write(f"- `{detail}`\n")
                if len(result.details) > 80:
                    handle.write(f"- `... {len(result.details) - 80} more details`\n")
            handle.write("\n")
    return output


def summarize_results(results: list[ConceptOutputDiff]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
        concept_type = result.concept_type or "unknown"
        type_counts[concept_type] = type_counts.get(concept_type, 0) + 1
    return {
        "compared_files": len(results),
        "changed_files": sum(result.status != "unchanged" for result in results),
        "status_counts": counts,
        "concept_type_counts": type_counts,
    }
