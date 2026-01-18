"""Callback configuration for data transformations.

This module defines the CallbackConfig model that wraps callback functions
from the CallbackRegistry, allowing them to be specified in YAML configurations
with parameters.
"""

from typing import Any, Callable

from polars import Expr, LazyFrame
from pydantic import BaseModel, Field, computed_field

from open_icu.callbacks.registry import registry


class CallbackConfig(BaseModel):
    """Configuration for a callback transformation.

    Specifies a callback by name (must be registered in CallbackRegistry)
    and provides parameters to pass to the callback constructor. The callback
    can then be invoked on LazyFrames via the `call` property.

    Attributes:
        callback: Name of the registered callback (e.g., "add", "drop_na")
        params: Dictionary of parameters to pass to the callback constructor
        call: Computed property that returns the instantiated callback function
    """
    callback: str = Field(..., description="The callback function for the event.")
    params: dict[str, Any] = Field(
        default_factory=dict, description="The parameters for the callback function."
    )

    def model_post_init(self, context: Any) -> None:
        """Validate that the callback is registered after model initialization.

        Args:
            context: Pydantic context

        Raises:
            ValueError: If the callback name is not found in CallbackRegistry
        """
        if self.callback not in registry:
            raise ValueError(
                f"Callback '{self.callback}' is not registered in the CallbackRegistry."
            )

        return super().model_post_init(context)

    @computed_field
    @property
    def call(self) -> Callable[[LazyFrame], Expr]:
        """Get the instantiated callback function.

        Retrieves the callback class from the registry and instantiates it
        with the configured parameters.

        Returns:
            A callable that takes a LazyFrame and returns a transformed LazyFrame
        """
        callback_class = registry.get(self.callback)
        assert callback_class is not None
        return callback_class(**self.params)  # type: ignore[invalid-return-type]
