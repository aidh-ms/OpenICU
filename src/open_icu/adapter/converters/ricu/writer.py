"""Write generated OpenICU YAML files."""

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from .models import GeneratedFile


class _OpenICUDumper(yaml.SafeDumper):
    """PyYAML dumper with list indentation matching hand-written YAML files."""

    def increase_indent(self, flow: bool = False, indentless: bool = False):  # type: ignore[override]
        return super().increase_indent(flow, False)


def merge_generated_files(files: list[GeneratedFile]) -> list[GeneratedFile]:
    """Merge compatible generated files with the same output path.

    This prevents later GeneratedFile objects from silently overwriting earlier
    ones when multiple RICU concepts normalize to the same OpenICU concept name.
    Dataset-specific simple configs are merged by concatenating their mappings.
    Identical global concept configs are kept once.
    Incompatible collisions raise an error.
    """
    merged: OrderedDict[str, GeneratedFile] = OrderedDict()

    for file in files:
        existing = merged.get(file.path)

        if existing is None:
            merged[file.path] = GeneratedFile(
                path=file.path,
                content=deepcopy(file.content),
            )
            continue

        existing_content = existing.content
        new_content = file.content

        if existing_content == new_content:
            continue

        if (
            existing_content.get("type") == "simple"
            and new_content.get("type") == "simple"
            and isinstance(existing_content.get("mappings"), list)
            and isinstance(new_content.get("mappings"), list)
        ):
            existing_mappings = existing_content["mappings"]
            seen = {repr(mapping) for mapping in existing_mappings}

            for mapping in new_content["mappings"]:
                key = repr(mapping)
                if key not in seen:
                    existing_mappings.append(deepcopy(mapping))
                    seen.add(key)

            continue

        raise ValueError(
            "Generated file path collision with incompatible contents: "
            f"{file.path}"
        )

    return list(merged.values())


class OpenICUYAMLWriter:
    def __init__(self, output_root: str | Path, *, overwrite: bool = False) -> None:
        self.output_root = Path(output_root)
        self.overwrite = overwrite

    def write_files(self, files: list[GeneratedFile]) -> list[Path]:
        written: list[Path] = []

        files = merge_generated_files(files)

        for generated in files:
            path = self.output_root / generated.path
            if path.exists() and not self.overwrite:
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                yaml.dump(
                    generated.content,
                    f,
                    Dumper=_OpenICUDumper,
                    sort_keys=False,
                    allow_unicode=True,
                    width=120,
                )
            written.append(path)
        return written

    def write_report(self, report: dict[str, Any], rel_path: str = "ricu_unsupported.yml") -> Path:
        path = self.output_root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            yaml.dump(report, f, Dumper=_OpenICUDumper, sort_keys=False, allow_unicode=True, width=120)
        return path