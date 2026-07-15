"""Validation tests for the configurations shipped in configs/.

These tests make sure that every bundled YAML file parses into its Pydantic
model and that every embedded expression string is valid in the expression
DSL (correct syntax, only registered callbacks). Dataset configs are resolved
through the version inheritance mechanism (extends.yml) first, so both
physical files and inherited/merged configs are validated. They guard the
configuration library — the part of OpenICU most contributors will touch.
"""

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from open_icu.callbacks.interpreter import ExprInterpreter
from open_icu.config.inheritance import resolve_effective_configs
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.config.derived import DerivedDatasetConceptConfig
from open_icu.steps.concept.config.simple import SimpleDatasetConceptConfig
from open_icu.steps.extraction.config.table import BaseTableConfig, TableConfig
from open_icu.utils.importer import import_callable

REPO_ROOT = Path(__file__).parents[1]
CONFIG_ROOT = REPO_ROOT / "configs"

# Version dirs may be marker-only (just an extends.yml, no physical tables/
# or mappings/ subdirectory), so collect by version dir rather than globbing
# for the subdirectories themselves.
VERSION_DIRS = sorted(d for d in CONFIG_ROOT.glob("datasets/*/*") if d.is_dir())
TABLE_CONFIG_DIRS = [
    d / "tables" for d in VERSION_DIRS if (d / "tables").is_dir() or (d / "extends.yml").is_file()
]
CONCEPT_FILES = sorted(CONFIG_ROOT.glob("concepts/*/*.yml"))
DATASET_CONCEPT_DIRS = [
    d / "mappings" for d in VERSION_DIRS if (d / "mappings").is_dir() or (d / "extends.yml").is_file()
]


def relative_id(path: Path) -> str:
    return str(path.relative_to(CONFIG_ROOT))


def effective_table_cases() -> list[Any]:
    """One test case per effective table config of each dataset version."""
    cases = []
    for subdir in TABLE_CONFIG_DIRS:
        for name in sorted(resolve_effective_configs(subdir)):
            cases.append(pytest.param(subdir, name, id=f"{relative_id(subdir)}/{name}"))
    return cases


def load_effective_table(subdir: Path, name: str) -> TableConfig:
    """Build a TableConfig from effective data, raising on validation errors."""
    data = resolve_effective_configs(subdir)[name]
    data.setdefault("dataset", subdir.parent.parent.name)
    data.setdefault("version", subdir.parent.name)
    data.setdefault("name", Path(name).name)
    return TableConfig(**data)


def assert_expressions_parse(expressions: list[str], source: str) -> None:
    interpreter = ExprInterpreter()
    for expr in expressions:
        try:
            interpreter.eval(expr)
        except Exception as e:  # noqa: BLE001 - reported with context
            pytest.fail(f"invalid expression in {source}: {expr!r} ({e})")


def assert_regex_compiles(regex: str, source: str) -> None:
    """Validate a mapping regex against the engine that runs it (Polars str.contains).

    Concept mappings filter the extracted ``code`` column with ``pl.col("code").
    str.contains(mapping.regex)``. A malformed regex (e.g. unbalanced parentheses)
    parses fine as YAML and only fails at concept-step runtime, so validate it
    against the real engine here -- not Python's ``re``, whose syntax differs.
    """
    try:
        pl.select(pl.lit("x").str.contains(regex))
    except Exception as e:  # noqa: BLE001 - reported with context
        pytest.fail(f"invalid mapping regex in {source}: {regex!r} ({e})")


def collect_table_expressions(table: BaseTableConfig) -> list[str]:
    expressions = [
        *table.pre_callbacks,
        *table.pre_filters,
        *table.callbacks,
        *table.filters,
        *table.post_join_callbacks,
        *table.post_join_filters,
        *table.transformations,
    ]
    return expressions


def collect_event_expressions(table: TableConfig) -> list[str]:
    expressions: list[str] = []
    for event in table.events:
        expressions += event.pre_callbacks + event.callbacks + event.filters
        expressions += event.transformations + event.output_filters
        expressions += event.code_prefix + event.code_suffix
        expressions += event.columns.code
        for value in (
            event.columns.subject_id,
            event.columns.time,
            event.columns.numeric_value,
            event.columns.text_value,
        ):
            if value is not None:
                expressions.append(value)
        expressions += [v for v in event.columns.extension.values() if v is not None]
    return expressions


@pytest.mark.parametrize(("subdir", "name"), effective_table_cases())
def test_table_config_parses(subdir: Path, name: str) -> None:
    table = load_effective_table(subdir, name)

    assert table.dataset == subdir.parent.parent.name
    assert table.version == subdir.parent.name
    assert table.events, f"{relative_id(subdir)}/{name} defines no events"

    for event in table.events:
        assert event.columns.subject_id is not None, (
            f"{relative_id(subdir)}/{name} event {event.name}: missing subject_id mapping"
        )
        assert event.columns.time is not None, (
            f"{relative_id(subdir)}/{name} event {event.name}: missing time mapping"
        )


@pytest.mark.parametrize(("subdir", "name"), effective_table_cases())
def test_table_config_expressions_are_valid(subdir: Path, name: str) -> None:
    table = load_effective_table(subdir, name)
    source = f"{relative_id(subdir)}/{name}"

    assert_expressions_parse(collect_table_expressions(table), source)
    for join_table in table.join:
        assert_expressions_parse(collect_table_expressions(join_table), source)
    assert_expressions_parse(collect_event_expressions(table), source)


@pytest.mark.parametrize("concept_file", CONCEPT_FILES, ids=relative_id)
def test_concept_parses_with_all_dataset_mappings(concept_file: Path) -> None:
    concept = ConceptConfig.load(concept_file, dataset_paths=DATASET_CONCEPT_DIRS)

    assert concept.name == concept_file.stem, (
        f"{relative_id(concept_file)}: file name and concept name differ ({concept.name})"
    )
    assert_expressions_parse(list(concept.extension_columns.values()), relative_id(concept_file))

    for dataset_concept in concept.dataset_concepts:
        source = f"{relative_id(concept_file)} [{dataset_concept.dataset}]"

        if isinstance(dataset_concept, SimpleDatasetConceptConfig):
            assert dataset_concept.mappings, f"{source}: no mappings defined"
            for mapping in dataset_concept.mappings:
                expressions = [
                    value
                    for value in (mapping.columns.numeric_value, mapping.columns.text_value)
                    if value is not None
                ]
                assert_expressions_parse(expressions + mapping.filters, source)
                assert_regex_compiles(mapping.pattern.code, source)

        if isinstance(dataset_concept, DerivedDatasetConceptConfig):
            for concept_table in (dataset_concept.table, *dataset_concept.join):
                assert_expressions_parse(
                    concept_table.pre_callbacks + concept_table.callbacks + concept_table.post_callbacks,
                    source,
                )
            assert_expressions_parse(dataset_concept.filters, source)

        if isinstance(dataset_concept, ComplexDatasetConceptConfig):
            try:
                import_callable(dataset_concept.concept_transformer)
            except Exception as e:  # noqa: BLE001 - reported with context
                pytest.fail(
                    f"unimportable concept transformer in {source}: {dataset_concept.concept_transformer!r} ({e})"
                )
            if (code_pattern := dataset_concept.kwargs.get("code_pattern")) is not None:
                assert_regex_compiles(code_pattern, source)


def test_demo_dataset_inherits_full_table_set() -> None:
    """eicu-demo must inherit the complete eicu-crd table set via extends.yml."""
    full = set(resolve_effective_configs(CONFIG_ROOT / "datasets" / "eicu-crd" / "2.0" / "tables"))
    demo = set(resolve_effective_configs(CONFIG_ROOT / "datasets" / "eicu-demo" / "2.0" / "tables"))
    assert demo == full

    demo_infusion = load_effective_table(CONFIG_ROOT / "datasets" / "eicu-demo" / "2.0" / "tables", "infusiondrug")
    assert demo_infusion.path == "infusiondrug.csv.gz"  # demo's lowercase file name
    assert demo_infusion.dataset == "eicu-demo"
    assert demo_infusion.events  # inherited from eicu-crd


def test_aumc_inherits_omop_tables() -> None:
    """aumc 1.5.0 is read through its OMOP CDM 5.4 export, so it inherits the
    omop table set unchanged via extends.yml.

    Guards the cross-dataset *model* inheritance: the table definitions come from
    omop, while identity (dataset/version) and the parquet default come from the
    extending aumc directory.
    """
    omop = resolve_effective_configs(CONFIG_ROOT / "datasets" / "omop" / "5.4" / "tables")
    aumc = resolve_effective_configs(CONFIG_ROOT / "datasets" / "aumc" / "1.5.0" / "tables")
    assert set(aumc) == set(omop)
    assert len(omop) == 11

    measurement = load_effective_table(CONFIG_ROOT / "datasets" / "aumc" / "1.5.0" / "tables", "measurement")
    assert measurement.dataset == "aumc"
    assert measurement.version == "1.5.0"
    assert measurement.identifier == "openicu.config.table.aumc.1.5.0.measurement"
    assert measurement.type == "parquet"  # parquet default flows through inheritance
    assert measurement.events  # inherited from omop


def test_mimic_versions_inherit_reference_configs() -> None:
    """mimic-iv 3.1 and the demo must inherit the 2.2 reference configs.

    All full MIMIC-IV releases share the same schema for the configured
    tables, so 3.1 inherits 2.2 unchanged. Only the demo *export* deviates:
    it ships procedureevents' originalamount/originalrate headers uppercase.
    """
    reference = resolve_effective_configs(CONFIG_ROOT / "datasets" / "mimic-iv" / "2.2" / "tables")
    v31 = resolve_effective_configs(CONFIG_ROOT / "datasets" / "mimic-iv" / "3.1" / "tables")
    demo = resolve_effective_configs(CONFIG_ROOT / "datasets" / "mimic-iv-demo" / "2.2" / "tables")
    assert len(reference) >= 19

    # full releases are schema-identical
    assert v31 == reference

    # the demo overrides exactly one table: the procedureevents column casing
    assert {name for name in reference if demo[name] != reference[name]} == {"procedureevents"}
    demo_columns = {c["name"] for c in demo["procedureevents"]["columns"]}
    assert {"ORIGINALAMOUNT", "ORIGINALRATE"} <= demo_columns
    reference_columns = {c["name"] for c in reference["procedureevents"]["columns"]}
    assert {"originalamount", "originalrate"} <= reference_columns
    # everything but the columns is inherited
    assert demo["procedureevents"]["events"] == reference["procedureevents"]["events"]
    assert demo["procedureevents"]["join"] == reference["procedureevents"]["join"]

    # identity comes from the version dir, not from where the files live
    labevents = load_effective_table(CONFIG_ROOT / "datasets" / "mimic-iv" / "3.1" / "tables", "labevents")
    assert labevents.dataset == "mimic-iv"
    assert labevents.version == "3.1"
    assert labevents.identifier == "openicu.config.table.mimic-iv.3.1.labevents"

    demo_labevents = load_effective_table(CONFIG_ROOT / "datasets" / "mimic-iv-demo" / "2.2" / "tables", "labevents")
    assert demo_labevents.dataset == "mimic-iv-demo"
    assert demo_labevents.version == "2.2"


def test_mimic_demo_inherits_concept_mappings() -> None:
    """Concept mappings must resolve for the demo through the marker-only mappings dir."""
    concept = ConceptConfig.load(
        CONFIG_ROOT / "concepts" / "vitals" / "heart_rate.yml",
        dataset_paths=[CONFIG_ROOT / "datasets" / "mimic-iv-demo" / "2.2" / "mappings"],
    )
    dataset_concept = concept.get_dataset_concept("mimic-iv-demo", "2.2")
    assert isinstance(dataset_concept, SimpleDatasetConceptConfig)
    assert dataset_concept.version == "2.2"
    assert dataset_concept.mappings
    assert dataset_concept.dataset == "mimic-iv-demo"


def test_every_dataset_mapping_has_a_concept_definition() -> None:
    """Each effective per-dataset mapping must correspond to a shared concept definition."""
    concept_names = {f.stem for f in CONCEPT_FILES}
    missing = {
        f"{concept_dir.parent.parent.name}: {name}"
        for concept_dir in DATASET_CONCEPT_DIRS
        for name in resolve_effective_configs(concept_dir)
        if Path(name).name not in concept_names
    }
    assert not missing, f"dataset mappings without a concept definition: {sorted(missing)}"


def test_config_inventory_is_nonempty() -> None:
    assert len(effective_table_cases()) >= 50
    assert len(CONCEPT_FILES) >= 80
    # Marker-only versions (extends.yml) count even without a physical
    # mappings/ dir; only empty dirs without a marker (nwicu) are not
    # guaranteed to survive a fresh clone.
    assert len(DATASET_CONCEPT_DIRS) >= 5
