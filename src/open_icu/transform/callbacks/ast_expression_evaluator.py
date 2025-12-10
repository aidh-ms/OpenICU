import ast

import polars as pl
from polars import Expr, LazyFrame

from open_icu.transform.callbacks.proto import CallbackProtocol
from open_icu.transform.callbacks.registry import register_callback_class


@register_callback_class
class AbstractSyntaxTree(CallbackProtocol):
    def __init__(self, expression: str, result: str) -> None:
        self.expression = expression
        self.result = result
        self.ast_tree = ast.parse(self.expression, mode="eval")
        

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        polars_expression = self._ast_to_polars(self.ast_tree.body).alias(self.result)
        return lf.with_columns(polars_expression)
    
    def _ast_to_polars(self, node: ast.AST) -> Expr:
        if isinstance(node, ast.Constant):
            return pl.lit(node.value)

        if isinstance(node, ast.Name):
            return pl.col(node.id)

        if isinstance(node, ast.UnaryOp):
            unary_op = node.op
            operand = self._ast_to_polars(node.operand)

            if isinstance(unary_op, ast.USub):
                return -operand
            if isinstance(unary_op, ast.UAdd):
                return operand

        if isinstance(node, ast.BinOp):
            left = self._ast_to_polars(node.left)
            right = self._ast_to_polars(node.right)

            binary_op = node.op
            if isinstance(binary_op, ast.Add):
                return left + right
            if isinstance(binary_op, ast.Sub):
                return left - right
            if isinstance(binary_op, ast.Mult):
                return left * right
            if isinstance(binary_op, ast.Div):
                return left / right
            if isinstance(binary_op, ast.Pow):
                return left ** right
            if isinstance(binary_op, ast.Mod):
                return left % right
            raise NotImplementedError(f"Unsupported operator: {type(binary_op)}")

        if isinstance(node, ast.Call):
            func_name = self._get_func_name(node.func)
            args = [self._ast_to_polars(a) for a in node.args]

            if func_name == "mean":
                return args[0].mean()
            if func_name == "abs":
                return args[0].abs()
            if func_name == "sum":
                return pl.sum_horizontal(args)
            if func_name == "prod":
                import operator
                return pl.fold(acc=pl.lit(1), function=operator.mul, exprs=args)
            if func_name == "root":
                radicand, index = args
                return radicand.sign() * (radicand.abs() ** (1 / index))

            raise NotImplementedError(f"Function {func_name} not implemented")

        raise NotImplementedError(f"Unsupported AST node: {type(node)}")
    
    def _get_func_name(self, node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        raise NotImplementedError(f"Unsupported function node type: {type(node)}")
