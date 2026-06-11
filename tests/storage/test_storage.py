"""Tests for project, workspace, and MEDS dataset storage."""

import json
from pathlib import Path

import polars as pl
import pytest

from open_icu.storage.base import FileStorage
from open_icu.storage.meds import MEDSDataset
from open_icu.storage.project import OpenICUProject
from open_icu.storage.workspace import WorkspaceDir


class TestFileStorage:
    def test_creates_directory(self, tmp_path: Path) -> None:
        storage = FileStorage(tmp_path / "store")
        assert storage.path.is_dir()

    def test_overwrite_removes_existing_content(self, tmp_path: Path) -> None:
        target = tmp_path / "store"
        target.mkdir()
        (target / "leftover.txt").write_text("old")

        FileStorage(target, overwrite=True)
        assert target.is_dir()
        assert list(target.iterdir()) == []

    def test_no_overwrite_keeps_existing_content(self, tmp_path: Path) -> None:
        target = tmp_path / "store"
        target.mkdir()
        (target / "keep.txt").write_text("data")

        FileStorage(target, overwrite=False)
        assert (target / "keep.txt").exists()

    def test_cleanup(self, tmp_path: Path) -> None:
        storage = FileStorage(tmp_path / "store")
        storage.cleanup()
        assert not storage.path.exists()


class TestWorkspaceDir:
    def test_content_lists_parquet_recursively(self, tmp_path: Path) -> None:
        workspace = WorkspaceDir(tmp_path / "ws")
        nested = workspace.path / "a" / "b"
        nested.mkdir(parents=True)
        pl.DataFrame({"x": [1]}).write_parquet(nested / "data.parquet")
        (workspace.path / "ignore.txt").write_text("not parquet")

        assert workspace.content == [nested / "data.parquet"]


class TestOpenICUProject:
    def test_context_manager_and_paths(self, tmp_path: Path) -> None:
        with OpenICUProject(tmp_path / "project") as project:
            assert project.path.is_dir()
            assert project.datasets_path == project.path / "datasets"
            assert project.workspace_path == project.path / "workspace"
            assert project.configs_path == project.path / "configs"

    def test_add_workspace_and_dataset(self, tmp_path: Path) -> None:
        project = OpenICUProject(tmp_path / "project")

        workspace = project.add_workspace_dir("extraction")
        dataset = project.add_dataset("extraction")

        assert workspace.path == project.workspace_path / "extraction"
        assert workspace.path.is_dir()
        assert dataset.path == project.datasets_path / "extraction"
        assert dataset.data_path.is_dir()
        assert dataset.metadata_path.is_dir()
        assert project.workspace == {"extraction": workspace}
        assert project.datasets == {"extraction": dataset}


class TestMEDSDataset:
    def test_write_metadata(self, tmp_path: Path) -> None:
        dataset = MEDSDataset(tmp_path / "ds")
        dataset.write_metadata({"dataset_name": "test", "dataset_version": "1.0"})

        metadata = json.loads((dataset.metadata_path / "dataset.json").read_text())
        assert metadata["dataset_name"] == "test"
        assert metadata["etl_name"] == "OpenICU"
        assert "meds_version" in metadata
        assert "created_at" in metadata

    def test_write_metadata_rejects_invalid_schema(self, tmp_path: Path) -> None:
        dataset = MEDSDataset(tmp_path / "ds")
        with pytest.raises(Exception):
            dataset.write_metadata({"dataset_name": 123})  # wrong type

    def test_write_codes_collects_unique_codes(self, tmp_path: Path) -> None:
        dataset = MEDSDataset(tmp_path / "ds")
        subdir = dataset.data_path / "table"
        subdir.mkdir()
        pl.DataFrame({"code": ["a//1", "b//2", "a//1"]}).write_parquet(subdir / "x.parquet")
        pl.DataFrame({"code": ["c//3"]}).write_parquet(subdir / "y.parquet")

        dataset.write_codes()

        codes = pl.read_parquet(dataset.metadata_path / "codes.parquet")
        assert sorted(codes["code"].to_list()) == ["a//1", "b//2", "c//3"]
        assert codes["description"].null_count() == 3

    def test_write_codes_with_no_data(self, tmp_path: Path) -> None:
        dataset = MEDSDataset(tmp_path / "ds")
        dataset.write_codes()

        codes = pl.read_parquet(dataset.metadata_path / "codes.parquet")
        assert codes.height == 0
        assert "code" in codes.columns
