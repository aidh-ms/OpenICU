"""Command line interface for the RICU to OpenICU converter."""

from __future__ import annotations

import argparse
from pathlib import Path

from .loader import load_concept_dict
from .mapper import RICUToOpenICUMapper, unsupported_report
from .settings import (
    ConverterSettings,
    load_project_event_names,
    load_project_event_names_by_code_column,
)
from .writer import OpenICUYAMLWriter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="openicu-ricu-convert",
        description="Generate OpenICU concept YAML configs from RICU concept-dict.json.",
    )
    parser.add_argument("--concept-dict", required=True, help="Path to RICU concept-dict.json")
    parser.add_argument(
        "--source",
        nargs="+",
        required=True,
        help="RICU source keys to generate, e.g. mimic eicu mimic_demo",
    )
    parser.add_argument(
        "--output-config",
        required=True,
        help="Output config root. Usually the OpenICU config/ directory or a generated config/ directory.",
    )
    parser.add_argument(
        "--settings",
        help="Optional converter settings YAML. Values override built-in defaults.",
    )
    parser.add_argument(
        "--openicu-config",
        help="Optional existing OpenICU config/ directory used to infer table event names.",
    )
    parser.add_argument(
        "--category",
        nargs="*",
        help="Optional RICU categories to include, e.g. vitals chemistry hematology.",
    )
    parser.add_argument(
        "--concept",
        nargs="*",
        help="Optional RICU concept keys to include, e.g. dbp alb abx.",
    )
    parser.add_argument(
        "--include-derived",
        action="store_true",
        help=(
            "Also generate known recursive RICU concepts as OpenICU type: derived. "
            "Unknown recursive concepts are reported unless --complex-stubs is also set."
        ),
    )
    parser.add_argument(
        "--complex-stubs",
        action="store_true",
        help=(
            "For recursive RICU concepts without a known derived rule, generate OpenICU type: complex placeholders. "
            "These need a real transformer implementation before they can run."
        ),
    )
    parser.add_argument(
        "--complex-transformer",
        default="open_icu.concepts.ricu_transformers.UnsupportedRICUTransformer",
        help="Dotted Python path used in generated complex stubs.",
    )
    parser.add_argument(
        "--regex-prefix-mode",
        choices=["none", "contains"],
        help=(
            "How to emit RICU regex code patterns. "
            "'none' keeps (regex); 'contains' emits .*?(regex). "
            "Default comes from settings and is 'none'."
        ),
    )
    parser.add_argument(
        "--logical-columns-mode",
        choices=["preserve", "boolean"],
        help=(
            "How to map logical RICU concepts with set_val(TRUE). "
            "'preserve' emits col(numeric_value)/col(text_value); "
            "'boolean' emits const(1)/const(\"true\"). "
            "Default comes from settings and is 'preserve'."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing generated files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write files; only print a summary.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    settings = ConverterSettings.from_file(args.settings)
    if args.regex_prefix_mode:
        settings.regex_prefix_mode = args.regex_prefix_mode
    if args.logical_columns_mode:
        settings.logical_columns_mode = args.logical_columns_mode

    inferred_by_code_column = {}
    if args.openicu_config:
        inferred_table_events = load_project_event_names(args.openicu_config)
        for dataset, table_map in inferred_table_events.items():
            settings.event_names.setdefault(dataset, {}).update(table_map)
        inferred_by_code_column = load_project_event_names_by_code_column(args.openicu_config)

    concepts = load_concept_dict(args.concept_dict)
    mapper = RICUToOpenICUMapper(
        settings,
        inferred_events_by_code_column=inferred_by_code_column,
    )
    files = mapper.build_files(
        concepts,
        sources=args.source,
        categories=set(args.category) if args.category else None,
        concept_keys=set(args.concept) if args.concept else None,
        include_derived=args.include_derived,
        complex_stubs=args.complex_stubs,
        complex_transformer=args.complex_transformer,
    )

    report = unsupported_report(mapper.unsupported)

    print(f"Loaded concepts: {len(concepts)}")
    print(f"Generated YAML payloads: {len(files)}")
    print(f"Unsupported/report entries: {len(mapper.unsupported)}")

    if args.dry_run:
        preview = sorted(generated.path for generated in files[:20])
        print("\nPreview:")
        for path in preview:
            print(f"  {path}")
        if len(files) > 20:
            print(f"  ... {len(files) - 20} more")
        return 0

    writer = OpenICUYAMLWriter(Path(args.output_config), overwrite=args.overwrite)
    written = writer.write_files(files)
    report_path = writer.write_report(report)
    print(f"Written files: {len(written)}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
