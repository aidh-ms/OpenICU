
import ast

from polars import LazyFrame

from open_icu.callbacks.proto import CallbackProtocol, ExpressionCallback, FrameCallback
from open_icu.callbacks.registry import CallbackRegistry, register_callback_class


@register_callback_class
class AstInterpreter(CallbackProtocol):
    
    def __init__(self, expr: str) -> None:
        self.expr = expr

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        interpreter = ExprInterpreter()

        callback = interpreter.eval(self.expr)
        print("test", self.expr, callback)
        if not isinstance(callback, ExpressionCallback) and not isinstance(callback, FrameCallback):
            raise TypeError("Top-level expression must be a Callback")

        lf = callback(lf)

        return lf


    
class ExprInterpreter(ast.NodeVisitor):
    def eval(self, expr: str):
        tree = ast.parse(expr, mode="eval")
        return self.visit(tree.body)
    
    def visit_constant(self, node):
        return node.value
    
    def visit_list(self, node):
        return [self.visit(e) for e in node.elts]
    
    def visit_name(self, node):
        # return pl.col(node.id)
        raise NameError(f"Unknown name: {node.id}")
    
    def visit_keyword(self, node):
        if node.arg is None:
            raise TypeError("Argument name must not be None")
        return node.arg, self.visit(node.value)
    
    def visit_call(self, node):
        name = self._get_name(node.func)

        if name not in CallbackRegistry():
            raise ValueError(f"Unknown callback: {name}")

        cls = CallbackRegistry()[name]

        args = [self.visit(a) for a in node.args]
        kwargs = dict(self.visit_keyword(k) for k in node.keywords)

        # positional args → kwargs erzwingen
        if args:
            raise TypeError("Only keyword arguments allowed")

        return cls(**kwargs)
    
    def visit_bin_op(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)

        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Div):
            return left / right

        raise NotImplementedError(node.op)
    
    def visit_compare(self, node):
        left = self.visit(node.left)
        right = self.visit(node.comparators[0])

        if isinstance(node.ops[0], ast.Gt):
            return left > right

        raise NotImplementedError(node.ops[0])
    
    def _get_name(self, node):
        if isinstance(node, ast.Name):
            return node.id
        raise ValueError("Only simple calls allowed")