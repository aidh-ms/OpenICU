"""Tests for BaseConfig / BaseDatasetConfig identity, serialization, and loading."""

from pathlib import Path
from typing import ClassVar

import pytest
import yaml

from open_icu.config.base import BaseConfig, BaseDatasetConfig


class DemoConfig(BaseConfig):
    __open_icu_config_type__: ClassVar[str] = "demo"


class DemoDatasetConfig(BaseDatasetConfig):
    __open_icu_config_type__: ClassVar[str] = "demo"


class TestIdentity:
    def test_identifier_format(self) -> None:
        config = DemoConfig(name="Heart_Rate", version="1.0.0")
        assert config.identifier == "openicu.config.demo.heart_rate.1.0.0"
        assert config.identifier_tuple == ("demo", "Heart_Rate", "1.0.0")
        assert str(config) == config.identifier

    def test_uuid_is_deterministic(self) -> None:
        a = DemoConfig(name="x", version="1")
        b = DemoConfig(name="x", version="1")
        c = DemoConfig(name="x", version="2")
        assert a.uuid == b.uuid
        assert a.uuid != c.uuid

    def test_prefix_and_ensure_prefix(self) -> None:
        assert DemoConfig.prefix() == "openicu.config.demo"
        assert DemoConfig.ensure_prefix("foo") == "openicu.config.demo.foo"
        assert DemoConfig.ensure_prefix("openicu.config.demo.foo") == "openicu.config.demo.foo"


class TestSerialization:
    def test_save_load_round_trip(self, tmp_path: Path) -> None:
        config = DemoConfig(name="hr", version="1.0.0")
        config.save(tmp_path)

        saved_file = tmp_path / "demo" / "hr" / "1.0.0.yml"
        assert saved_file.exists()

        loaded = DemoConfig.load(saved_file)
        assert loaded == config

    def test_save_excludes_computed_fields(self, tmp_path: Path) -> None:
        config = DemoConfig(name="hr", version="1.0.0")
        config.save(tmp_path)

        data = yaml.safe_load((tmp_path / "demo" / "hr" / "1.0.0.yml").read_text())
        assert set(data) == {"name", "version"}

    def test_load_kwargs_fill_missing_fields_only(self, tmp_path: Path) -> None:
        path = tmp_path / "config.yml"
        path.write_text("name: from_file\n")

        loaded = DemoConfig.load(path, name="from_kwargs", version="9.9")
        assert loaded.name == "from_file"  # file value wins
        assert loaded.version == "9.9"  # kwarg fills the gap

    def test_load_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            DemoConfig.load(tmp_path / "nope.yml")


class TestDatasetConfigPathInference:
    def test_dataset_version_name_inferred_from_path(self, tmp_path: Path) -> None:
        config_file = tmp_path / "mimic-iv" / "3.1" / "tables" / "labevents.yml"
        config_file.parent.mkdir(parents=True)
        config_file.write_text("{}\n")

        loaded = DemoDatasetConfig.load(config_file)
        assert loaded.dataset == "mimic-iv"
        assert loaded.version == "3.1"
        assert loaded.name == "labevents"
