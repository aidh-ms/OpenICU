import json

from typing import List, Dict, Any

from pydantic import BaseModel

class FieldBaseModel(BaseModel):
    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        ...
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        ...
        
    def __repr__(self):
        return json.dumps(self.to_dict())

    def __str__(self):
        return self.summary()