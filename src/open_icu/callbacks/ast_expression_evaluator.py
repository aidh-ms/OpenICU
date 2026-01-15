import ast
import operator
from typing import Optional

import polars as pl
from polars import Expr, LazyFrame

from open_icu.callbacks.proto import CallbackProtocol, ExpressionCallback, FrameCallback
from open_icu.callbacks.registry import CallbackRegistry, register_callback_class


@register_callback_class
class AbstractSyntaxTree(CallbackProtocol):
    """Evaluate a Python-like expression via AST and translate it to Polars."""

    def __init__(self, expr: str, output: Optional[str] = None) -> None:
        self.expr = expr
        self.output = output
        self._registry = CallbackRegistry()  # cache once per instance

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        try:
            node = ast.parse(self.expr, mode="eval").body
        except SyntaxError as e:
            raise ValueError(f"Invalid expression syntax: {self.expr!r}") from e

        # If the *top-level* node is a FrameCallback, execute it directly.
        if isinstance(node, ast.Call):
            func_name = self._get_func_name(node.func)
            if func_name in self._registry:
                CallbackClass = self._registry[func_name]

                if issubclass(CallbackClass, FrameCallback):
                    args_expr = [self._ast_to_polars(a) for a in node.args]
                    return CallbackClass(*args_expr)(lf)

                # If it's an ExpressionCallback at top-level, treat it as expression
                # and require output.
                if issubclass(CallbackClass, ExpressionCallback):
                    expr = self._ast_to_polars(node)
                    if self.output is None:
                        raise ValueError(
                            f"Missing output column for expression {self.expr!r}"
                        )
                    return lf.with_columns(expr.alias(self.output))

                raise TypeError(
                    f"Registered callback {func_name!r} is neither FrameCallback nor ExpressionCallback "
                    f"(got {CallbackClass})."
                )

        # Normal expression path
        expr = self._ast_to_polars(node)
        if self.output is None:
            raise ValueError(f"Missing output column for expression {self.expr!r}")
        return lf.with_columns(expr.alias(self.output))

    def _ast_to_polars(self, node: ast.AST) -> Expr:
        if isinstance(node, ast.Constant):
            value = node.value
            if isinstance(value, str) and value.lower() in {"none", "null"}:
                value = None
            return pl.lit(value)

        if isinstance(node, ast.Name):
            return pl.col(node.id)

        if isinstance(node, ast.UnaryOp):
            operand = self._ast_to_polars(node.operand)
            if isinstance(node.op, ast.USub):
                return -operand
            if isinstance(node.op, ast.UAdd):
                return operand
            raise NotImplementedError(f"Unsupported unary operator: {type(node.op)}")

        if isinstance(node, ast.BinOp):
            left = self._ast_to_polars(node.left)
            right = self._ast_to_polars(node.right)

            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.Pow):
                return left ** right
            if isinstance(node.op, ast.Mod):
                return left % right

            raise NotImplementedError(f"Unsupported binary operator: {type(node.op)}")

        if isinstance(node, ast.Call):
            func_name = self._get_func_name(node.func)
            args = [self._ast_to_polars(a) for a in node.args]

            if func_name in self._registry:
                CallbackClass = self._registry[func_name]

                if issubclass(CallbackClass, ExpressionCallback):
                    return CallbackClass(*args).as_expression()

                if issubclass(CallbackClass, FrameCallback):
                    raise TypeError(
                        f"FrameCallback {func_name!r} not allowed inside expression context: {ast.dump(node)}"
                    )

                raise TypeError(
                    f"Registered callback {func_name!r} has unsupported type {CallbackClass}."
                )

            # built-ins
            if func_name == "mean":
                return pl.mean_horizontal(args)
            if func_name == "sum":
                return pl.sum_horizontal(args)
            if func_name == "prod":
                return pl.fold(acc=pl.lit(1), function=operator.mul, exprs=args)
            if func_name == "root":
                if len(args) != 2:
                    raise ValueError("root(radicand, index) expects exactly 2 arguments")
                radicand, index = args
                return radicand.sign() * (radicand.abs() ** (1 / index))

            raise NotImplementedError(f"Function {func_name!r} not implemented")
        
        if isinstance(node, ast.List):
            if not node.elts:
                raise ValueError("Empty list literals are not supported (type is ambiguous)")
            return pl.concat_list([self._ast_to_polars(e) for e in node.elts])

        raise NotImplementedError(f"Unsupported AST node: {type(node)}")

    def _get_func_name(self, node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return node.id

        if isinstance(node, ast.Attribute):
            # allow a restricted whitelist of namespaces like dt.foo, str.bar, arr.baz
            if isinstance(node.value, ast.Name) and node.value.id in {"str", "dt", "arr"}:
                return node.attr
            raise NotImplementedError(f"Illegal attribute call: {ast.dump(node)}")

        raise NotImplementedError(f"Unsupported function node type: {type(node)}")
