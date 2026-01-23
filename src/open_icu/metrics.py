import json
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

_metrics: Optional["OpenICUMetrics"] = None

def get_metrics() -> "OpenICUMetrics":
    global _metrics
    if _metrics is None:
        _metrics = OpenICUMetrics()
        _metrics.basicConfig("/workspaces/output/metrics/metrics.json")
    return _metrics

@dataclass
class Metric:
    names: Set[str] = field(default_factory=set)

    @property
    def count(self) -> int:
        return len(self.names)

    def add(self, name: str) -> None:
        self.names.add(name)

    def set(self, names: Set[str]) -> None:
        self.names = names

    def reset(self) -> None:
        self.names.clear()

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "count": self.count,
            "names": sorted(self.names),
        }
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return str(self.count)

class PipelineArtifact(StrEnum):
    SRC_CONFIGS_AVAILABLE = "src_configs_available"
    SRC_CONFIGS_USED = "src_configs_used"
    SRC_FILES_AVAILABLE = "src_files_available"
    SRC_FILES_USED = "src_files_used"
    EVENTS_AVAILABLE = "events_available"
    EVENTS_CREATED = "events_created"
    CONCEPT_CONFIGS_AVAILABLE = "concept_configs_available"
    CONCEPT_CONFIGS_USED = "concept_configs_used"
    CONCEPTS_CREATED = "concepts_created"

class OpenICUMetrics:
    """Global metrics"""

    _instance: Optional["OpenICUMetrics"] = None
    
    ordering: List[str] = [str(artifact) for artifact in (
        PipelineArtifact.SRC_CONFIGS_AVAILABLE,
        PipelineArtifact.SRC_CONFIGS_USED,
        PipelineArtifact.SRC_FILES_AVAILABLE,
        PipelineArtifact.SRC_FILES_USED,
        PipelineArtifact.EVENTS_AVAILABLE,
        PipelineArtifact.EVENTS_CREATED,
        PipelineArtifact.CONCEPT_CONFIGS_AVAILABLE,
        PipelineArtifact.CONCEPT_CONFIGS_USED,
        PipelineArtifact.CONCEPTS_CREATED,
    )]

    def _init_metrics(self) -> None:
        self.metrics: Dict[str, Metric] = {artifact: Metric() for artifact in OpenICUMetrics.ordering}

    def __new__(cls) -> "OpenICUMetrics":
        if cls._instance is None:
            cls._instance: "OpenICUMetrics" = super().__new__(cls)
            cls._instance._init_metrics()
        return cls._instance

    def basicConfig(self, filename: str, load_if_exists: bool = True) -> None:
        self.filename = Path(filename)

        if load_if_exists and self.filename.exists():
            self._load_from_file()
    
    def add(self, artifact: str, name: str, save: bool = True) -> None:
        self.metrics[artifact].add(name)
        if save: 
            self.save() 

    def set(self, artifact: str, names: Set[str], save: bool = True) -> None:
        self.metrics[artifact].set(names)
        if save: 
            self.save()

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {artifact: self.metrics[artifact].to_dict() for artifact in self.metrics}
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {artifact: self.metrics[artifact].count for artifact in self.metrics}
    
    def reset(self, save: bool: bool = True) -> None:
        for artifact in self.metrics:
            self.metrics[artifact].reset()
        if save: 
            self.save()

    def save(self) -> None:
        if not hasattr(self, "filename"):
            raise RuntimeError("Metrics not configured. Call basicConfig().")

        self.filename.parent.mkdir(parents=True, exist_ok=True)

        with self.filename.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)

    def _load_from_file(self) -> None:
        with self.filename.open("r", encoding="utf-8") as f:
            data: Any = json.load(f)

        for artifact in OpenICUMetrics.ordering:
            metric = Metric()
            metric.names: set[str] = set(data[artifact]["names"])
            self.metrics[artifact] = metric
