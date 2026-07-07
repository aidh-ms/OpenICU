"""Tests for dataset configuration inheritance (extends.yml)."""

from pathlib import Path

import pytest

from open_icu.config.inheritance import (
    deep_merge,
    has_extends,
    resolve_effective_configs,
    resolve_version_chain,
)
from open_icu.config.registry import load_configs
from open_icu.steps.extraction.config.table import TableConfig


def make_version(
    root: Path,
    dataset: str,
    version: str,
    files: dict[str, str],
    extends: tuple[str, str] | None = None,
) -> Path:
    """Create a version directory with a tables/ subdir."""
    version_dir = root / dataset / version
    subdir = version_dir / "tables"
    subdir.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (subdir / f"{name}.yml").write_text(content)
    if extends is not None:
        (version_dir / "extends.yml").write_text(f"dataset: {extends[0]}\nversion: '{extends[1]}'\n")
    return version_dir


class TestDeepMerge:
    def test_override_wins_for_scalars_and_lists(self) -> None:
        base = {"path": "a.csv", "columns": [{"name": "x"}], "type": "csv"}
        override = {"path": "b.csv", "columns": [{"name": "y"}]}
        merged = deep_merge(base, override)
        assert merged == {"path": "b.csv", "columns": [{"name": "y"}], "type": "csv"}

    def test_nested_mappings_merge_recursively(self) -> None:
        base = {"event_defaults": {"subject_id": "col(a)", "extension": {"x": "col(x)"}}}
        override = {"event_defaults": {"extension": {"y": "col(y)"}}}
        merged = deep_merge(base, override)
        assert merged["event_defaults"]["subject_id"] == "col(a)"
        assert merged["event_defaults"]["extension"] == {"x": "col(x)", "y": "col(y)"}

    def test_inputs_are_not_mutated(self) -> None:
        base = {"a": {"b": 1}}
        override = {"a": {"c": 2}}
        deep_merge(base, override)
        assert base == {"a": {"b": 1}}
        assert override == {"a": {"c": 2}}


class TestVersionChain:
    def test_no_marker_is_chain_of_one(self, tmp_path: Path) -> None:
        version_dir = make_version(tmp_path, "db", "1.0", {"t": "path: t.csv\n"})
        assert resolve_version_chain(version_dir) == [version_dir]
        assert not has_extends(version_dir / "tables")

    def test_chain_is_base_first(self, tmp_path: Path) -> None:
        base = make_version(tmp_path, "db", "1.0", {"t": "path: t.csv\n"})
        mid = make_version(tmp_path, "db", "1.1", {}, extends=("db", "1.0"))
        leaf = make_version(tmp_path, "db-demo", "1.1", {}, extends=("db", "1.1"))

        assert resolve_version_chain(leaf) == [base, mid, leaf]
        assert has_extends(leaf / "tables")

    def test_missing_base_raises(self, tmp_path: Path) -> None:
        version_dir = make_version(tmp_path, "db", "2.0", {}, extends=("db", "1.0"))
        with pytest.raises(FileNotFoundError, match="does not exist"):
            resolve_version_chain(version_dir)

    def test_malformed_marker_raises(self, tmp_path: Path) -> None:
        version_dir = make_version(tmp_path, "db", "2.0", {})
        (version_dir / "extends.yml").write_text("dataset: db\n")  # missing version
        with pytest.raises(ValueError, match="must define both"):
            resolve_version_chain(version_dir)

    def test_circular_chain_raises(self, tmp_path: Path) -> None:
        make_version(tmp_path, "db", "1.0", {}, extends=("db", "2.0"))
        version_dir = make_version(tmp_path, "db", "2.0", {}, extends=("db", "1.0"))
        with pytest.raises(ValueError, match="Circular extends chain"):
            resolve_version_chain(version_dir)


class TestEffectiveConfigs:
    def test_inherits_absent_files_and_merges_present_ones(self, tmp_path: Path) -> None:
        make_version(
            tmp_path,
            "db",
            "1.0",
            {
                "unchanged": "path: unchanged.csv\ntype: csv\n",
                "renamed": "path: CamelCase.csv\ntype: csv\n",
            },
        )
        version_dir = make_version(
            tmp_path, "db-demo", "1.0", {"renamed": "path: lowercase.csv\n"}, extends=("db", "1.0")
        )

        effective = resolve_effective_configs(version_dir / "tables")

        assert effective["unchanged"] == {"path": "unchanged.csv", "type": "csv"}
        # merged: path overridden, inherited keys kept
        assert effective["renamed"] == {"path": "lowercase.csv", "type": "csv"}

    def test_deleted_tombstone_removes_inherited_config(self, tmp_path: Path) -> None:
        make_version(tmp_path, "db", "1.0", {"notes": "path: notes.csv\n", "keep": "path: keep.csv\n"})
        version_dir = make_version(tmp_path, "db-demo", "1.0", {"notes": "deleted: true\n"}, extends=("db", "1.0"))

        effective = resolve_effective_configs(version_dir / "tables")
        assert "notes" not in effective
        assert "keep" in effective

    def test_recursive_chain_applies_diffs_in_order(self, tmp_path: Path) -> None:
        make_version(tmp_path, "db", "1.0", {"t": "path: a.csv\ntype: csv\n"})
        make_version(tmp_path, "db", "1.1", {"t": "path: b.csv\n"}, extends=("db", "1.0"))
        version_dir = make_version(tmp_path, "db", "2.0", {"t": "type: csvgz\n"}, extends=("db", "1.1"))

        effective = resolve_effective_configs(version_dir / "tables")
        assert effective["t"] == {"path": "b.csv", "type": "csvgz"}

    def test_without_marker_returns_own_files(self, tmp_path: Path) -> None:
        version_dir = make_version(tmp_path, "db", "1.0", {"t": "path: t.csv\n"})
        assert resolve_effective_configs(version_dir / "tables") == {"t": {"path": "t.csv"}}


class TestLoadConfigsWithInheritance:
    def test_identity_comes_from_extending_version(self, tmp_path: Path) -> None:
        make_version(tmp_path, "db", "1.0", {"t": "path: t.csv\n"})
        version_dir = make_version(tmp_path, "db-demo", "2.0", {}, extends=("db", "1.0"))

        configs = load_configs(version_dir / "tables", TableConfig)

        assert len(configs) == 1
        assert configs[0].dataset == "db-demo"
        assert configs[0].version == "2.0"
        assert configs[0].name == "t"
        assert configs[0].identifier == "openicu.config.table.db-demo.2.0.t"
        assert configs[0].path == "t.csv"

    def test_includes_excludes_apply_to_inherited_configs(self, tmp_path: Path) -> None:
        make_version(tmp_path, "db", "1.0", {"a": "path: a.csv\n", "b": "path: b.csv\n"})
        version_dir = make_version(tmp_path, "db-demo", "1.0", {}, extends=("db", "1.0"))

        configs = load_configs(version_dir / "tables", TableConfig, includes=["db-demo.1.0.a"])
        assert [c.name for c in configs] == ["a"]

        configs = load_configs(version_dir / "tables", TableConfig, excludes=["db-demo.1.0.a"])
        assert [c.name for c in configs] == ["b"]

    def test_invalid_inherited_config_is_skipped(self, tmp_path: Path) -> None:
        make_version(tmp_path, "db", "1.0", {"bad": "columns: notalist\n", "good": "path: g.csv\n"})
        version_dir = make_version(tmp_path, "db-demo", "1.0", {}, extends=("db", "1.0"))

        configs = load_configs(version_dir / "tables", TableConfig)
        assert [c.name for c in configs] == ["good"]
