from typing import Any

from pydantic import BaseModel, Field


class CallbackConfig(BaseModel):
    callback: str = Field(..., description="The callback function for the event.")
    params: dict[str, Any] = Field(
        default_factory=dict, description="The parameters for the callback function."
    )
