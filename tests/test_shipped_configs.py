"""Validation tests for the configurations shipped in config/.

These tests make sure that every bundled YAML file parses into its Pydantic
model and that every embedded expression string is valid in the expression
DSL (correct syntax, only registered callbacks). They guard the configuration
library — the part of OpenICU most contributors will touch.
"""

from pathlib import Path

import pytest

from open_icu.callbacks.interpreter import ExprInterpreter
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.config.derived import DerivedDatasetConceptConfig
from open_icu.steps.concept.config.simple import SimpleDatasetConceptConfig
from open_icu.steps.extraction.config.table import BaseTableConfig, TableConfig

REPO_ROOT = Path(__file__).parents[1]
CONFIG_ROOT = REPO_ROOT / "config"

TABLE_CONFIG_FILES = sorted(CONFIG_ROOT.glob("dataset/*/*/dataset/*.yml"))
CONCEPT_FILES = sorted(CONFIG_ROOT.glob("concept/*/*.yml"))
DATASET_CONCEPT_DIRS = sorted(d for d in CONFIG_ROOT.glob("dataset/*/*/concept") if d.is_dir())


def relative_id(path: Path) -> str:
    return str(path.relative_to(CONFIG_ROOT))


def assert_expressions_parse(expressions: list[str], source: str) -> None:
    interpreter = ExprInterpreter()
    for expr in expressions:
        try:
            interpreter.eval(expr)
        except Exception as e:  # noqa: BLE001 - reported with context
            pytest.fail(f"invalid expression in {source}: {expr!r} ({e})")


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


@pytest.mark.parametrize("config_file", TABLE_CONFIG_FILES, ids=relative_id)
def test_table_config_parses(config_file: Path) -> None:
    table = TableConfig.load(config_file)

    assert table.dataset == config_file.parents[2].name
    assert table.version == config_file.parents[1].name
    assert table.events, f"{relative_id(config_file)} defines no events"

    for event in table.events:
        assert event.columns.subject_id is not None, (
            f"{relative_id(config_file)} event {event.name}: missing subject_id mapping"
        )
        assert event.columns.time is not None, (
            f"{relative_id(config_file)} event {event.name}: missing time mapping"
        )


@pytest.mark.parametrize("config_file", TABLE_CONFIG_FILES, ids=relative_id)
def test_table_config_expressions_are_valid(config_file: Path) -> None:
    table = TableConfig.load(config_file)

    assert_expressions_parse(collect_table_expressions(table), relative_id(config_file))
    for join_table in table.join:
        assert_expressions_parse(collect_table_expressions(join_table), relative_id(config_file))
    assert_expressions_parse(collect_event_expressions(table), relative_id(config_file))


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

        if isinstance(dataset_concept, DerivedDatasetConceptConfig):
            for concept_table in (dataset_concept.table, *dataset_concept.join):
                assert_expressions_parse(
                    concept_table.pre_callbacks + concept_table.callbacks + concept_table.post_callbacks,
                    source,
                )
            assert_expressions_parse(dataset_concept.filters, source)


def test_every_dataset_mapping_has_a_concept_definition() -> None:
    """Each per-dataset mapping file must correspond to a shared concept definition."""
    concept_names = {f.stem for f in CONCEPT_FILES}
    missing = {
        f"{mapping_file.parents[2].name}: {mapping_file.stem}"
        for concept_dir in DATASET_CONCEPT_DIRS
        for mapping_file in concept_dir.glob("*.yml")
        if mapping_file.stem not in concept_names
    }
    assert not missing, f"dataset mappings without a concept definition: {sorted(missing)}"


def test_config_inventory_is_nonempty() -> None:
    assert len(TABLE_CONFIG_FILES) >= 40
    assert len(CONCEPT_FILES) >= 80
    assert len(DATASET_CONCEPT_DIRS) >= 3
