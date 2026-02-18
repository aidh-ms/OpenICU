import ast
from typing import Tuple

from polars import Expr, LazyFrame

from open_icu.callbacks._callbacks.algebra import Add, Divide, Multiply, Subtract
from open_icu.callbacks.proto import AstValue, CallbackProtocol
from open_icu.callbacks.registry import registry


class ExprInterpreter(ast.NodeVisitor):
    def eval(self, expr: str) -> AstValue:
        tree = ast.parse(expr, mode="eval")
        return self.visit(tree.body)

    def visit_Constant(self, node) -> AstValue:
        return node.value

    def visit_List(self, node) -> AstValue:
        return [self.visit(e) for e in node.elts]

    def visit_Name(self, node) -> AstValue:
        # DSL: bare names are column references
        return node.id

    def visit_Keyword(self, node) -> Tuple[str, AstValue]:
        if node.arg is None:
            raise TypeError("**kwargs syntax is not supported")
        return node.arg, self.visit(node.value)

    def visit_Call(self, node) -> AstValue:
        name = self._get_name(node.func)

        if name not in registry:
            raise ValueError(f"Unknown callback: {name}")

        cls = registry.get(name)
        assert cls is not None

        args = [self.visit(a) for a in node.args]
        kwargs = dict(self.visit_Keyword(k) for k in node.keywords if k.arg is not None)

        try:
            return cls(*args, **kwargs)
        except TypeError as e:
            raise TypeError(f"Bad arguments for {name}(*{args}, **{kwargs}): {e}") from e

    def visit_BinOp(self, node) -> AstValue:
        left = self.visit(node.left)
        right = self.visit(node.right)

        if isinstance(node.op, ast.Add):
            return Add(left, right)
        if isinstance(node.op, ast.Mult):
            return Multiply(left, right)
        if isinstance(node.op, ast.Sub):
            return Subtract(left, right)
        if isinstance(node.op, ast.Div):
            return Divide(left, right)

        raise NotImplementedError(node.op)

    def visit_Compare(self, node) -> AstValue:
        left = self.visit(node.left)
        right = self.visit(node.comparators[0])

        if isinstance(node.ops[0], ast.Gt):
            return left > right  # Callback has to be implemented and called'

        raise NotImplementedError(node.ops[0])

    def _get_name(self, node) -> str:
        if isinstance(node, ast.Name):
            return node.id
        raise ValueError("Only simple calls allowed")


def parse_expr(lf : LazyFrame, expr: str) -> Expr:
    interpreter = ExprInterpreter()
    callback = interpreter.eval(expr)

    assert isinstance(callback, CallbackProtocol)
    pl_expr = callback(lf)

    return pl_expr
