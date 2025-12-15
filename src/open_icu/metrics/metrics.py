from dataclasses import dataclass, field
from typing import Set, Dict, Optional, Any, List
from pathlib import Path
import json
from enum import Enum

from open_icu.metrics.metrics import PipelineArtifacts as pa
from open_icu.config.dataset.source.config.base import OpenICUBaseModel

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

    # TODO: More Object-oriented

    # src_config_available_count: int = 0
    # src_config_available_names: Set[str] = field(default_factory=set)

    # src_config_used_count: int = 0
    # src_config_used_names: Set[str] = field(default_factory=set)

    # src_table_available_count: int = 0
    # src_table_available_names: Set[str] = field(default_factory=set)

    # src_table_used_count: int = 0
    # src_table_used_names: Set[str] = field(default_factory=set)

    # event_available_count: int = 0
    # event_available_names: Set[str] = field(default_factory=set)

    # event_created_count: int = 0
    # event_created_names: Set[str] = field(default_factory=set)

    def __new__(cls) -> "OpenICUStatistics":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_metrics()
        return cls._instance
    
    def add(self, artifact: str, name: str) -> None:
        self.metrics[artifact].add(name)
    
    # def set_src_config_available_count(self, n: int) -> None:
    #     self.src_config_available_count = n

    # def add_src_config_available_names(self, name: str) -> None:
    #     if name not in self.src_config_used_names:
    #         self.src_config_available_names.add(name)
    #         self.src_config_available_count = len(self.src_config_available_names)

    # def set_src_config_used_count(self, n: int) -> None:
    #     self.src_config_used_count = n

    # def add_src_config_used_names(self, name: str) -> None:
    #     if name not in self.src_config_used_names:
    #         self.src_config_used_names.add(name)
    #         self.src_config_used_count = len(self.src_config_used_names)
    
    # def set_src_table_available_count(self, n: int) -> None:
    #     self.src_table_available_count = n

    # def add_src_table_available_names(self, name: str) -> None:
    #     if name not in self.src_table_available_names:
    #         self.src_table_available_names.add(name)
    #         self.src_table_available_count = len(self.src_table_available_names)
    
    # def set_src_table_used_count(self, n: int) -> None:
    #     self.src_table_used_count = n

    # def add_src_table_used_names(self, name: str) -> None:
    #     if name not in self.src_table_used_names:
    #         self.src_table_used_names.add(name)
    #         self.src_table_used_count = len(self.src_table_used_names)

    # def set_event_available_count(self, n: int) -> None:
    #     self.event_available_count = n

    # def add_event_available_names(self, name: str) -> None:
    #     if name not in self.event_available_names:
    #         self.event_available_names.add(name)
    #         self.event_available_count = len(self.event_created_names)

    # def set_event_created_count(self, n: int) -> None:
    #     self.event_created_count = n

    # def add_event_created_names(self, name: str) -> None:
    #     if name not in self.event_created_names:
    #         self.event_created_names.add(name)
    #         self.event_created_count = len(self.event_created_names)

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {artifact: self.metrics[artifact] for artifact in self.metrics}

    # def to_dict(self) -> Dict[str, Any] | str | List[Any]:
    #     return {
    #         "src_config_available": {
    #             "count": self.src_config_available_count,
    #             "names": self.src_config_available_names,
    #         },
    #         "src_config_used": {
    #             "count" : self.src_config_used_count,
    #             "names": self.src_config_used_names,
    #         },
    #         "src_table_available": {
    #             "count": self.src_table_available_count,
    #             "names": self.src_table_available_names,
    #         },
    #         "src_table_used": {
    #             "count": self.src_table_used_count,
    #             "names": self.src_table_used_names,
    #         },
    #         "event_available": {
    #             "count": self.event_available_count,
    #             "names": self.event_available_names,
    #         },
    #         "event_created": {
    #             "count": self.event_created_count,
    #             "names": self.event_created_names,
    #         },
    #     }
    
    # ordering has to have an even number of elements with two successively elements having the same structur (name + "_sth")
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {artifact: self.metrics[artifact].count for artifact in self.metrics}

    # def summary(self) -> Dict[str, Any] | str | List[Any]:
    #     return {
    #         "src_config" : f"{self.src_config_used_count}/{self.src_config_available_count}",
    #         "src_table" : f"{self.src_table_used_count}/{self.src_table_available_count}",
    #         "event" : f"{self.event_created_count}/{self.event_available_count}",
    #     }
    
    def reset(self) -> None:
        for artifact in self.metrics:
            self.metrics[artifact].reset()
            
    # def reset(self) -> None:
    #     self.src_config_available_count = 0
    #     self.src_config_used_count = 0
    #     self.src_table_available_count = 0
    #     self.src_table_used_count = 0
    #     self.event_available_count = 0
    #     self.event_created_count = 0

    #     self.src_config_available_names.clear()
    #     self.src_config_used_names.clear()
    #     self.src_table_available_names.clear()
    #     self.src_table_used_names.clear()
    #     self.event_available_names.clear()
    #     self.event_created_names.clear()

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: str | Path) -> "OpenICUStatistics":
        path = Path(path)

        instance = cls()

        if not path.exists():
            return instance
        
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

            for artifact in OpenICUStatistics.ordering:
                instance.metrics[artifact] = Metric()
                instance.metrics[artifact].names = set(data[artifact])

        # instance.src_config_available_names = set(data["src_config_available"]["names"])
        # instance.src_config_used_names = set(data["src_config_used"]["names"])
        # instance.src_table_available_names = set(data["src_table_available"]["names"])
        # instance.src_table_used_names = set(data["src_table_used"]["names"])
        # instance.event_available_names = set(data["event_available"]["names"])
        # instance.event_created_names = set(data["event_created"]["names"])

        # instance.src_config_available_count = data["src_config_available"]["count"]
        # instance.src_config_used_count = data["src_config_used"]["count"]
        # instance.src_table_available_count = data["src_table_available"]["count"]
        # instance.src_table_used_count = data["src_table_used"]["count"]
        # instance.event_available_count = data["event_available"]["count"]
        # instance.event_created_count = data["event_created"]["count"]

        return instance