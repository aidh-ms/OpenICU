"""CLI entrypoint for comparing OpenICU concept output Parquets."""

from __future__ import annotations

import argparse
from pathlib import Path

from .compare_outputs import (
    compare_output_dirs,
    summarize_results,
    write_json_report,
    write_markdown_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="openicu-ricu-compare-outputs",
        description="Compare old and new OpenICU concept output Parquet files.",
    )
    parser.add_argument("--old", required=True, type=Path, help="Old concept output root.")
    parser.add_argument("--new", required=True, type=Path, help="New concept output root.")
    parser.add_argument(
        "--config-root",
        type=Path,
        default=None,
        help=(
            "Optional OpenICU config root. Enables concept type classification "
            "and dependency checks for derived/complex concepts."
        ),
    )
    parser.add_argument(
        "--key",
        nargs="*",
        default=None,
        help="Optional key columns for row alignment, e.g. --key subject_id time code.",
    )
    parser.add_argument("--tolerance", type=float, default=1e-9)
    parser.add_argument(
        "--ignore-columns",
        nargs="*",
        default=None,
        help=(
            "Columns to drop from both old and new outputs before comparing. "
            "Example: --ignore-columns dataset table"
        ),
    )
    parser.add_argument(
        "--normalize-code",
        choices=["hyphen-to-underscore", "basic"],
        default=None,
        help=(
            "Normalize the code column before comparing. "
            "'hyphen-to-underscore' treats '-' and '_' as equivalent in code. "
            "'basic' additionally lowercases and normalizes whitespace."
        ),
    )
    parser.add_argument(
        "--concept-type",
        nargs="*",
        choices=["simple", "derived", "complex"],
        help="Optional concept types to include when --config-root is supplied.",
    )
    parser.add_argument(
        "--no-dependency-check",
        action="store_true",
        help="Disable derived/complex dependency availability checks.",
    )
    parser.add_argument(
        "--report-md",
        type=Path,
        default=Path("concept_output_diff_report.md"),
        help="Markdown report path.",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=Path("concept_output_diff_report.json"),
        help="JSON report path.",
    )
    parser.add_argument("--show-unchanged", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not args.old.exists():
        raise FileNotFoundError(f"Old output directory does not exist: {args.old}")
    if not args.new.exists():
        raise FileNotFoundError(f"New output directory does not exist: {args.new}")
    if args.config_root is not None and not args.config_root.exists():
        raise FileNotFoundError(f"Config root does not exist: {args.config_root}")

    results = compare_output_dirs(
        old_root=args.old,
        new_root=args.new,
        key_columns=args.key,
        tolerance=args.tolerance,
        config_root=args.config_root,
        check_dependencies=not args.no_dependency_check,
        concept_type_filter=set(args.concept_type) if args.concept_type else None,
        ignore_columns=args.ignore_columns,
        normalize_code=args.normalize_code,
    )

    write_markdown_report(results, args.report_md)
    write_json_report(results, args.report_json)

    for result in results:
        if result.status == "unchanged" and not args.show_unchanged:
            continue
        prefix = f"[{result.status}]"
        if result.concept_type:
            prefix += f"[{result.concept_type}]"
        print(f"{prefix} {result.path}")
        if result.old_rows is not None or result.new_rows is not None:
            print(f"  rows: {result.old_rows} -> {result.new_rows}")
        for check in result.dependency_checks:
            if not check.exists_in_new:
                print(f"  - missing dependency output: {check.concept} -> {check.new_path}")
        for detail in result.details[:10]:
            print(f"  - {detail}")
        if len(result.details) > 10:
            print(f"  ... {len(result.details) - 10} more details")

    summary = summarize_results(results)
    print()
    print(f"Compared parquet files: {summary['compared_files']}")
    print(f"Changed/new/removed/error files: {summary['changed_files']}")
    print(f"Status counts: {summary['status_counts']}")
    print(f"Concept type counts: {summary['concept_type_counts']}")
    print(f"Markdown report: {args.report_md}")
    print(f"JSON report: {args.report_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
