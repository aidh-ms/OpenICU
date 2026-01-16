
import ast

from polars import LazyFrame

from open_icu.callbacks.proto import CallbackProtocol
from open_icu.callbacks.registry import CallbackRegistry, register_callback_class


@register_callback_class
class AstInterpreter(CallbackProtocol):
    
    def __init__(self, expr: str) -> None:
        self.expr = expr

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        interpreter = ExprInterpreter()
        print("expr", self.expr)
        callback = interpreter.eval(self.expr)
        print("callback", callback)
        if not isinstance(callback, CallbackProtocol):
            raise TypeError("Top-level expression must be a Callback")

        lf = callback(lf)

        return lf


    
class ExprInterpreter(ast.NodeVisitor):
    def eval(self, expr: str):
        tree = ast.parse(expr, mode="eval")
        print("tree", tree, tree.body)
        return self.visit(tree.body)
    
    def visit_Constant(self, node):
        return node.value
    
    def visit_List(self, node):
        return [self.visit(e) for e in node.elts]
    
    def visit_Name(self, node):
        # return pl.col(node.id)
        return node.id
        raise NameError(f"Unknown name: {node.id}")
    
    def visit_Keyword(self, node):
        return node.arg, self.visit(node.value)
    
    def visit_Call(self, node):
        name = self._get_name(node.func)

        if name not in CallbackRegistry():
            raise ValueError(f"Unknown callback: {name}")

        cls = CallbackRegistry()[name]

        args = [self.visit(a) for a in node.args]
        kwargs = dict(self.visit_keyword(k) for k in node.keywords if k.arg is not None)

        # positional args → kwargs erzwingen
        # if args:
        #     raise TypeError("Only keyword arguments allowed")

        return cls(*args, **kwargs)
    
    def visit_BinOp(self, node):
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
    
    def visit_Compare(self, node):
        left = self.visit(node.left)
        right = self.visit(node.comparators[0])

        if isinstance(node.ops[0], ast.Gt):
            return left > right

        raise NotImplementedError(node.ops[0])
    
    def _get_name(self, node):
        if isinstance(node, ast.Name):
            return node.id
        raise ValueError("Only simple calls allowed")