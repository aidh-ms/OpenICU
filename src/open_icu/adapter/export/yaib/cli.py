"""Command-line interface for building OpenICU -> YAIB dynamic tables."""

from __future__ import annotations

import argparse
from pathlib import Path

from .transform import write_dynamic_table


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build a YAIB/RICU-like dyn.parquet from OpenICU concept parquets.")
    parser.add_argument("--concept-root", required=True, help="OpenICU concept output root, e.g. workspace/concept")
    parser.add_argument("--icustays-csv", required=True, help="MIMIC-IV icustays.csv.gz path")
    parser.add_argument("--ricu-concept-dict", default=None, help="RICU concept-dict.json path")
    parser.add_argument("--dataset", default="mimic-iv")
    parser.add_argument("--version", default="1.0.0")
    parser.add_argument("--aggregation-mode", choices=["mean", "ricu"], default="mean")
    parser.add_argument("--output", required=True, help="Output parquet path")
    parser.add_argument("--max-hours", type=int, default=168, help="Maximum grid hour. Use -1 to disable cap.")
    parser.add_argument("--grid-end-rounding", choices=["floor", "ceil"], default="floor")
    parser.add_argument("--no-grid", action="store_true", help="Do not add complete stay/hour grid; keep observed concept keys only.")
    parser.add_argument("--no-icu-window-filter", action="store_true", help="Do not require concept event timestamp to be inside intime/outtime.")
    parser.add_argument("--missing-concepts", choices=["warn", "fail", "ignore"], default="warn")
    args = parser.parse_args(argv)

    max_hours = None if args.max_hours < 0 else args.max_hours

    write_dynamic_table(
        output_path=Path(args.output),
        concept_root=Path(args.concept_root),
        icustays_csv=Path(args.icustays_csv),
        ricu_concept_dict=Path(args.ricu_concept_dict) if args.ricu_concept_dict else None,
        dataset=args.dataset,
        version=args.version,
        aggregation_mode=args.aggregation_mode,
        include_grid=not args.no_grid,
        max_hours=max_hours,
        grid_end_rounding=args.grid_end_rounding,
        filter_to_icu_window=not args.no_icu_window_filter,
        missing_concepts=args.missing_concepts,
    )


if __name__ == "__main__":
    main()
