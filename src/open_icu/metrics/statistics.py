from dataclasses import dataclass, field
from typing import Set, Dict, Optional, Any, List

from open_icu.config.dataset.source.config.base import OpenICUBaseModel

class OpenICUStatistics(OpenICUBaseModel):
    """Global metrics"""

    _instance: Optional["OpenICUStatistics"] = None

    # TODO: More Object-oriented

    src_config_available_count: int = 0
    src_config_available_names: Set[str] = field(default_factory=set)

    src_config_used_count: int = 0
    src_config_used_names: Set[str] = field(default_factory=set)

    src_table_available_count: int = 0
    src_table_available_names: Set[str] = field(default_factory=set)

    src_table_used_count: int = 0
    src_table_used_names: Set[str] = field(default_factory=set)

    event_available_count: int = 0
    event_available_names: Set[str] = field(default_factory=set)

    event_created_count: int = 0
    event_created_names: Set[str] = field(default_factory=set)

    def __new__(cls) -> "OpenICUStatistics":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def set_src_config_available_count(self, n: int) -> None:
        self.src_config_available_count = n

    def add_src_config_available_names(self, name: str) -> None:
        if name not in self.src_config_used_names:
            self.src_config_available_names.add(name)
            self.src_config_available_count = len(self.src_config_available_names)

    def set_src_config_used_count(self, n: int) -> None:
        self.src_config_used_count = n

    def add_src_config_used_names(self, name: str) -> None:
        if name not in self.src_config_used_names:
            self.src_config_used_names.add(name)
            self.src_config_used_count = len(self.src_config_used_names)
    
    def set_src_table_available_count(self, n: int) -> None:
        self.src_table_available_count = n

    def add_src_table_available_names(self, name: str) -> None:
        if name not in self.src_table_available_names:
            self.src_table_available_names.add(name)
            self.src_table_available_count = len(self.src_table_available_names)
    
    def set_src_table_used_count(self, n: int) -> None:
        self.src_table_used_count = n

    def add_src_table_used_names(self, name: str) -> None:
        if name not in self.src_table_used_names:
            self.src_table_used_names.add(name)
            self.src_table_used_count = len(self.src_table_used_names)

    def set_event_available_count(self, n: int) -> None:
        self.event_available_count = n

    def add_event_available_names(self, name: str) -> None:
        if name not in self.event_available_names:
            self.event_available_names.add(name)
            self.event_available_count = len(self.event_created_names)

    def set_event_created_count(self, n: int) -> None:
        self.event_created_count = n

    def add_event_created_names(self, name: str) -> None:
        if name not in self.event_created_names:
            self.event_created_names.add(name)
            self.event_created_count = len(self.event_created_names)

    
    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "src_config_available": {
                "count": self.src_config_available_count,
                "names": self.src_config_available_names,
            },
            "src_config_used": {
                "count" : self.src_config_used_count,
                "names": self.src_config_used_names,
            },
            "src_table_available": {
                "count": self.src_table_available_count,
                "names": self.src_table_available_names,
            },
            "src_table_used": {
                "count": self.src_table_used_count,
                "names": self.src_table_used_names,
            },
            "event_available": {
                "count": self.event_available_count,
                "names": self.event_available_names,
            },
            "event_created": {
                "count": self.event_created_count,
                "names": self.event_created_names,
            },
        }
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "src_config" : f"{self.src_config_used_count}/{self.src_config_available_count}",
            "src_table" : f"{self.src_table_used_count}/{self.src_table_available_count}",
            "event" : f"{self.event_created_count}/{self.event_available_count}",
        }
    
    def reset(self) -> None:
        self.src_config_available_count = 0
        self.src_config_used_count = 0
        self.src_table_available_count = 0
        self.src_table_used_count = 0
        self.event_available_count = 0
        self.event_created_count = 0

        self.src_config_available_names.clear()
        self.src_config_used_names.clear()
        self.src_table_available_names.clear()
        self.src_table_used_names.clear()
        self.event_available_names.clear()
        self.event_created_names.clear()