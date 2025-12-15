from dataclasses import dataclass, field
from typing import Set, Dict, Optional, Any, List
from pathlib import Path
import json
from enum import Enum

from open_icu.metrics.metrics import PipelineArtifacts as pa
from open_icu.config.dataset.source.config.base import OpenICUBaseModel

_statistics: Optional["OpenICUStatistics"] = None

def get_statistics() -> "OpenICUStatistics":
    global _statistics
    if _statistics is None:
        _statistics = OpenICUStatistics()
    return _statistics

@dataclass
class Metric(OpenICUBaseModel):
    names: Set[str] = field(default_factory=set)

    @property
    def count(self) -> int:
        return len(self.names)

    def add(self, name: str) -> None:
        self.names.add(name)

    def reset(self) -> None:
        self.names.clear()

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "count": self.count,
            "names": sorted(self.names),
        }
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return str(self.count)

class PipelineArtifacts(Enum):
    SRC_CONFIG_AVAILABLE = "src_config_available"
    SRC_CONFIG_USED = "src_config_used"
    SRC_TABLE_AVAILABLE = "src_table_available"
    SRC_TABLE_USED = "src_table_used"
    EVENT_AVAILABLE = "event_available"
    EVENT_CREATED = "event_created"

    def __str__(self) -> str:
        return self.value

class OpenICUStatistics(OpenICUBaseModel):
    """Global metrics"""

    _instance: Optional["OpenICUStatistics"] = None
    
    ordering: List[str] = [str(artifact) for artifact in (
        pa.SRC_CONFIG_AVAILABLE,
        pa.SRC_CONFIG_USED,
        pa.SRC_TABLE_AVAILABLE,
        pa.SRC_TABLE_USED,
        pa.EVENT_AVAILABLE,
        pa.EVENT_CREATED,
    )]

    def _init_metrics(self) -> None:
        self.metrics: Dict[str, Metric] = {artifact: Metric() for artifact in OpenICUStatistics.ordering}

    def __new__(cls) -> "OpenICUStatistics":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_metrics()
        return cls._instance

    
    def basicConfig(self, filename: str, load_if_exists: bool = True,) -> None:
        self.filename = Path(filename)

        if load_if_exists and self.filename.exists():
            self._load_from_file()
    
    def add(self, artifact: str, name: str) -> None:
        self.metrics[artifact].add(name)

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {artifact: self.metrics[artifact] for artifact in self.metrics}
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {artifact: self.metrics[artifact].count for artifact in self.metrics}
    
    def reset(self) -> None:
        for artifact in self.metrics:
            self.metrics[artifact].reset()

    def save(self) -> None:
        if not hasattr(self, "filename"):
            raise RuntimeError("Statistics not configured. Call basicConfig().")

        self.filename.parent.mkdir(parents=True, exist_ok=True)

        with self.filename.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)


    # @classmethod
    # def load(cls, path: str | Path) -> "OpenICUStatistics":
    #     path = Path(path)

    #     instance = cls()

    #     if not path.exists():
    #         return instance
        
    #     with path.open("r", encoding="utf-8") as f:
    #         data = json.load(f)

    #         for artifact in OpenICUStatistics.ordering:
    #             instance.metrics[artifact] = Metric()
    #             instance.metrics[artifact].names = set(data[artifact])

    #     return instance

    def _load_from_file(self) -> None:
        with self.filename.open("r", encoding="utf-8") as f:
            data = json.load(f)

        for artifact in OpenICUStatistics.ordering:
            metric = Metric()
            metric.names = set(data[artifact]["names"])
            self.metrics[artifact] = metric
