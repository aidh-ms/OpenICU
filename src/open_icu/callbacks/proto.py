from typing import Any, Protocol

from polars import LazyFrame, Expr


class CallbackProtocol(Protocol):
    def __init__(self, **kwargs: Any) -> None:
        ...

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        ...

class ExpressionCallback(CallbackProtocol):
    """Callback that produces a Polars expression.

    Expression callbacks are intended to be embedded in higher-level
    expression contexts, such as an abstract syntax tree (AST). When applied
    to a `LazyFrame`, the expression is materialized as a new column.
    """

    result: str

    def as_expression(self) -> Expr:
        """Return the callback logic as a Polars expression.

        Returns:
            A Polars `Expr` representing the transformation.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        """Materialize the expression as a new column on the LazyFrame.

        Args:
            lf: Input LazyFrame.

        Returns:
            A LazyFrame with the expression added as a column named `result`.
        """
        return lf.with_columns(self.as_expression().alias(self.result))

class FrameCallback(CallbackProtocol):
    """Callback that operates directly on a Polars LazyFrame.

    Frame callbacks apply transformations at the frame level and therefore
    cannot be embedded inside expression-based DSLs or ASTs.
    """

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        """Apply the transformation directly to the LazyFrame.

        Args:
            lf: Input LazyFrame.

        Returns:
            A transformed LazyFrame.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        """Invoke the frame-level transformation.

        Args:
            lf: Input LazyFrame.

        Returns:
            A transformed LazyFrame.
        """
        return self.as_field(lf)
    
class HybridCallback(ExpressionCallback, FrameCallback):
    """Callback that supports both expression-level and frame-level usage.

    Hybrid callbacks can be embedded in expression contexts (via
    `as_expression`) and can also be applied directly to a `LazyFrame`
    (via `as_field`).
    """

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        """Apply the frame-level transformation.

        Args:
            lf: Input LazyFrame.

        Returns:
            A transformed LazyFrame.
        """
        return self.as_field(lf)