"""Command line entry point for the OpenICU -> YAIB dynamic table converter."""

from __future__ import annotations

import argparse
from pathlib import Path

from .transform import write_dynamic_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert OpenICU concept parquets to a YAIB/RICU-like dyn.parquet."
    )
    parser.add_argument("--concept-root", required=True, help="OpenICU workspace/concept directory")
    parser.add_argument("--icustays-csv", required=True, help="MIMIC-IV icu/icustays.csv.gz")
    parser.add_argument(
        "--ricu-concept-dict",
        default=None,
        help="Path to ricu/inst/extdata/config/concept-dict.json",
    )
    parser.add_argument("--dataset", default="mimic-iv", help="OpenICU dataset filename stem")
    parser.add_argument("--version", default=None, help="OpenICU concept version, e.g. 1.0.0")
    parser.add_argument("--output", required=True, help="Output parquet path")
    parser.add_argument(
        "--aggregation-mode",
        choices=["mean", "ricu"],
        default="mean",
        help="Use mean for every concept or RICU's aggregate field when present.",
    )
    parser.add_argument(
        "--max-hours",
        type=int,
        default=168,
        help="Maximum grid length in hours; YAIB base cohort uses 7*24 = 168.",
    )
    parser.add_argument(
        "--no-grid",
        action="store_true",
        help="Do not expand to the full YAIB-like hourly stay grid.",
    )
    parser.add_argument(
        "--missing-concepts",
        choices=["warn", "fail", "ignore"],
        default="warn",
        help="How to handle dynamic variables without a matching OpenICU parquet.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    write_dynamic_table(
        output_path=Path(args.output),
        concept_root=Path(args.concept_root),
        icustays_csv=Path(args.icustays_csv),
        ricu_concept_dict=Path(args.ricu_concept_dict) if args.ricu_concept_dict else None,
        dataset=args.dataset,
        version=args.version,
        aggregation_mode=args.aggregation_mode,
        include_grid=not args.no_grid,
        max_hours=args.max_hours,
        missing_concepts=args.missing_concepts,
    )
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
