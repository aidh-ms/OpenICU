from typing import Any, Dict, List

from pydantic import BaseModel, Field

from open_icu.config.dataset.source.config.base import OpenICUBaseModel

class FieldConfig(OpenICUBaseModel):
    name: str = Field(..., description="The name of the field.")
    type: str = Field(..., description="The type of the field.")
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional parameters for the field."
    )

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "name": self.name,
            "type": self.type,
            "params": self.params,
        }

    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "name": self.name,
            "type": self.type,
            "params_count": len(self.params),
        }

class ConstantFieldConfig(FieldConfig):
    constant: Any = Field(..., description="The constant value for the field.")

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return super().to_dict() | {  # type: ignore
            "constant": self.constant,
        }
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return super().summary() | {  # type: ignore
            "constant": self.constant
        }
