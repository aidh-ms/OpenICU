import json

from typing import List, Dict, Any
from abc import ABCMeta, abstractmethod

from pydantic import BaseModel

class OpenICUBaseModel(BaseModel, metaclass=ABCMeta):
    @abstractmethod
    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        ...
    
    @abstractmethod
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        ...
        
    def __repr__(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def __str__(self) -> str:
        return json.dumps(self.summary(), indent=2)