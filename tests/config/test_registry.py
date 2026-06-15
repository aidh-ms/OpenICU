"""Tests for the configuration registry and directory loading."""

from pathlib import Path
from typing import ClassVar

from open_icu.config.base import BaseConfig
from open_icu.config.registry import BaseConfigRegistry, load_configs


class RegConfig(BaseConfig):
    __open_icu_config_type__: ClassVar[str] = "regtest"


class RegConfigRegistry(BaseConfigRegistry[RegConfig]):
    pass


def make_config_dir(tmp_path: Path, names: list[str]) -> Path:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    for name in names:
        (config_dir / f"{name}.yml").write_text(f"name: {name}\nversion: '1.0'\n")
    return config_dir


class TestRegistry:
    def test_register_and_get(self) -> None:
        registry = RegConfigRegistry()
        config = RegConfig(name="a", version="1")
        registry.register(config)

        assert len(registry) == 1
        assert registry.get(("regtest", "a", "1")) is config
        assert registry.get("a.1") is config  # short form gets the prefix added
        assert registry.get(config.identifier) is config
        assert ("regtest", "a", "1") in registry

    def test_register_no_overwrite_by_default(self) -> None:
        registry = RegConfigRegistry()
        first = RegConfig(name="a", version="1")
        second = RegConfig(name="a", version="1")
        registry.register(first)
        registry.register(second)
        assert registry.get(first.identifier) is first

        registry.register(second, overwrite=True)
        assert registry.get(first.identifier) is second

    def test_unregister(self) -> None:
        registry = RegConfigRegistry()
        config = RegConfig(name="a", version="1")
        registry.register(config)

        assert registry.unregister(config.identifier) is True
        assert registry.unregister(config.identifier) is False
        assert len(registry) == 0

    def test_load_from_directory(self, tmp_path: Path) -> None:
        config_dir = make_config_dir(tmp_path, ["a", "b", "c"])
        registry = RegConfigRegistry()
        registry.load(config_dir)
        assert sorted(registry.keys()) == [
            "openicu.config.regtest.a.1.0",
            "openicu.config.regtest.b.1.0",
            "openicu.config.regtest.c.1.0",
        ]

    def test_load_with_includes_and_excludes(self, tmp_path: Path) -> None:
        config_dir = make_config_dir(tmp_path, ["a", "b", "c"])

        registry = RegConfigRegistry()
        registry.load(config_dir, includes=["a.1.0"])
        assert registry.keys() == ["openicu.config.regtest.a.1.0"]

        registry = RegConfigRegistry()
        registry.load(config_dir, excludes=["openicu.config.regtest.b.1.0"])
        assert sorted(registry.keys()) == [
            "openicu.config.regtest.a.1.0",
            "openicu.config.regtest.c.1.0",
        ]

    def test_save_round_trip(self, tmp_path: Path) -> None:
        registry = RegConfigRegistry()
        registry.register(RegConfig(name="a", version="1"))
        registry.register(RegConfig(name="b", version="2"))
        registry.save(tmp_path / "out")

        reloaded = RegConfigRegistry()
        reloaded.load(tmp_path / "out")
        assert sorted(reloaded.keys()) == sorted(registry.keys())


class TestLoadConfigs:
    def test_skips_invalid_yaml_files(self, tmp_path: Path) -> None:
        config_dir = make_config_dir(tmp_path, ["good"])
        (config_dir / "bad.yml").write_text("name: only_a_name_no_version\n")
        (config_dir / "notes.txt").write_text("not yaml at all")

        configs = load_configs(config_dir, RegConfig)
        assert [c.name for c in configs] == ["good"]

    def test_missing_path_returns_empty(self, tmp_path: Path) -> None:
        assert load_configs(tmp_path / "does-not-exist", RegConfig) == []
