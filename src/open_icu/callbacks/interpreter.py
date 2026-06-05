import ast
from typing import Tuple

from polars import LazyFrame

from open_icu.callbacks._callbacks.algebra import Add, Divide, Multiply, Subtract
from open_icu.callbacks._callbacks.logical import And, Or, Not
from open_icu.callbacks._callbacks.comparison import GreaterThan, LessThan, GreaterEqual, LessEqual, Equal, NotEqual
from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult
from open_icu.callbacks.registry import registry


class ExprInterpreter(ast.NodeVisitor):
    def eval(self, expr: str) -> AstValue:
        tree = ast.parse(expr, mode="eval")
        return self.visit(tree.body)

    def visit_Constant(self, node) -> AstValue:
        return node.value

    def visit_List(self, node) -> AstValue:
        return [self.visit(e) for e in node.elts]
    
    def visit_Tuple(self, node) -> AstValue:
        return tuple(self.visit(e) for e in node.elts)

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
        if isinstance(node.op, ast.BitAnd):
            return And(left, right)
        if isinstance(node.op, ast.BitOr):
            return Or(left, right)

        raise NotImplementedError(type(node.op))
    
    def visit_BoolOp(self, node) -> AstValue:
        if not node.values:
            raise ValueError("Empty boolean operation is not supported")

        values = [self.visit(v) for v in node.values]

        if isinstance(node.op, ast.And):
            expr = values[0]
            for value in values[1:]:
                expr = And(expr, value)
            return expr

        if isinstance(node.op, ast.Or):
            expr = values[0]
            for value in values[1:]:
                expr = Or(expr, value)
            return expr

    def visit_UnaryOp(self, node) -> AstValue:
        operand = self.visit(node.operand)

        if isinstance(node.op, ast.USub):
            return Multiply(operand, -1)
        if isinstance(node.op, ast.UAdd):
            return operand
        if isinstance(node.op, ast.Not):
            return Not(operand)
        if isinstance(node.op, ast.Invert):
            return Not(operand)

        raise NotImplementedError(type(node.op).__name__)

    def visit_Compare(self, node) -> AstValue:
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise NotImplementedError("Chained comparisons are not supported")

        left = self.visit(node.left)
        right = self.visit(node.comparators[0])
        op = node.ops[0]

        if isinstance(op, ast.Gt):
            return GreaterThan(left, right)
        if isinstance(op, ast.Lt):
            return LessThan(left, right)
        if isinstance(op, ast.GtE):
            return GreaterEqual(left, right)
        if isinstance(op, ast.LtE):
            return LessEqual(left, right)
        if isinstance(op, ast.Eq):
            return Equal(left, right)
        if isinstance(op, ast.NotEq):
            return NotEqual(left, right)

        raise NotImplementedError(type(op))

    def _get_name(self, node) -> str:
        if isinstance(node, ast.Name):
            return node.id
        raise ValueError("Only simple calls allowed")
    
    def generic_visit(self, node):
        raise ValueError(f"Unsupported syntax: {ast.dump(node)}")


def parse_expr(lf : LazyFrame, expr: str) -> CallbackResult:
    interpreter = ExprInterpreter()
    callback = interpreter.eval(expr)

    assert isinstance(callback, CallbackProtocol)
    pl_expr = callback(lf)

    return pl_expr
