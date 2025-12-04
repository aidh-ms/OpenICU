from typing import Any, Callable, List, Dict

from polars import LazyFrame
from pydantic import BaseModel, Field, computed_field

from open_icu.transform.callbacks.registry import CallbackRegistry
from open_icu.config.dataset.source.config.base import FieldBaseModel


class CallbackConfig(FieldBaseModel):
    callback: str = Field(..., description="The callback function for the event.")
    params: dict[str, Any] = Field(
        default_factory=dict, description="The parameters for the callback function."
    )

    def model_post_init(self, context: Any) -> None:
        if self.callback not in CallbackRegistry():
            raise ValueError(
                f"Callback '{self.callback}' is not registered in the CallbackRegistry."
            )

        return super().model_post_init(context)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def call(self) -> Callable[[LazyFrame], LazyFrame]:
        callback_class = CallbackRegistry().get(self.callback)
        assert callback_class is not None
        return callback_class(**self.params)

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "callback": self.callback,
            "params" : self.params,
        }
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "callback": self.callback,
            "params_count" : len(self.params),
        }
    
    