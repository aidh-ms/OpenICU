import polars as pl
from polars import LazyFrame

import ast

from open_icu.transform.callbacks.proto import CallbackProtocol
from open_icu.transform.callbacks.registry import register_callback_class


@register_callback_class
class AST(CallbackProtocol):
    def __init__(self, expression: str, result: str) -> None:
        self.expression = expression
        self.result = result
        self.ast_tree = ast.parse(self.expression, mode="eval")
        

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        polars_expression = self._ast_to_polars(self.ast_tree)
        return lf.with_columns(polars_expression)
    
    def _ast_to_polars(self, node):
        if isinstance(node, ast.Constant):
            return pl.lit(node.value)

        if isinstance(node, ast.Name):
            return pl.col(node.id)

        if isinstance(node, ast.BinOp):
            left = self._ast_to_polars(node.left)
            right = self._ast_to_polars(node.right)

            op = node.op
            if isinstance(op, ast.Add):
                return left + right
            if isinstance(op, ast.Sub):
                return left - right
            if isinstance(op, ast.Mult):
                return left * right
            if isinstance(op, ast.Div):
                return left / right
            raise NotImplementedError(f"Unsupported operator: {type(op)}")

        if isinstance(node, ast.Call):
            func_name = self._get_func_name(node.func)
            args = [self._ast_to_polars(a) for a in node.args]

            if func_name == "mean":
                return args[0].mean()
            if func_name == "abs":
                return args[0].abs()

            raise NotImplementedError(f"Function {func_name} not implemented")

        raise NotImplementedError(f"Unsupported AST node: {type(node)}")
    
    def _get_func_name(self, node):
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        raise NotImplementedError(f"Unsupported function node type: {type(node)}")
