import ast

import polars as pl
from polars import Expr, LazyFrame

from open_icu.transform.callbacks.proto import CallbackProtocol, ExpressionCallback, FrameCallback, HybridCallback 
from open_icu.transform.callbacks.registry import CallbackRegistry, register_callback_class


@register_callback_class
class AbstractSyntaxTree(CallbackProtocol):
    def __init__(self, expression: str, result: str) -> None:
        self.expression = expression
        self.result = result
    
    
    def __call__(self, lf: LazyFrame) -> LazyFrame:
        """Evaluate Expression"""

        """Check if expression represents a FrameCallback and call"""
        node = ast.parse(self.expression, mode="eval")
        self.registry = CallbackRegistry()
        if isinstance(node, ast.Call) and \
            (func_name := self._get_func_name(node.func)) in self.registry:
                CallbackClass = self.registry[func_name]
                """Check if its not a Callbackobject with as_expression method 
                else it should be an FrameCallback"""
                if not issubclass(CallbackClass, ExpressionCallback) and \
                    not issubclass(CallbackClass, HybridCallback):
                    if not issubclass(CallbackClass, FrameCallback):
                        raise TypeError(f"Unknown callback type: {type(self.expression)}")
                    return CallbackClass(*[self._ast_to_polars(a) for a in node.args]).__call__(lf)
        """If it is a CallbackObject with as_expression method, evaluate via abstract syntax tree"""
        return lf.with_columns(self._ast_to_polars(node.body).alias(self.result))
    
    """Creating an Polars expression for pl.with_columns from the ast expression"""
    def _ast_to_polars(self, node) -> Expr:

        """evaluate simple expression"""

        if isinstance(node, ast.Constant):
            return pl.lit(node.value)
        
        if(isinstance(node, ast.Name)):
            return pl.col(node.id)
        
        if isinstance(node, ast.UnaryOp):
            op = node.op
            operand = self._ast_to_polars(node.operand)

            if isinstance(op, ast.USub):
                return -operand
            if isinstance(op, ast.UAdd):
                return operand
            
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
            if isinstance(op, ast.Pow):
                return left ** right
            if isinstance(op, ast.Mod):
                return left % right
            raise NotImplementedError(f"Unsupported operator: {type(op)}")
        
        """evaluate callback calls"""
        if isinstance(node, ast.Call):
            func_name = self._get_func_name(node.func)
            args = [self._ast_to_polars(a) for a in node.args]

            registry = CallbackRegistry()
            # TODO: 
            if func_name in registry:
                CallbackClass = registry[func_name]
                if issubclass(CallbackClass, FrameCallback):
                    raise TypeError(f"FrameCallback not allowed inside of abstract syntax tree: {node}")
                if issubclass(CallbackClass, ExpressionCallback):
                    print("!is subclass")
                callback: ExpressionCallback | HybridCallback = CallbackClass(*args)
                return callback.as_expression()
        
            if func_name == "mean":
                return pl.mean_horizontal(args)
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
    

    def _get_func_name(self, node) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id in ("str", "dt", "arr"):
                return node.attr

            raise NotImplementedError(
                f"Illegal attribute call: {ast.dump(node)}. "
                "Top-level DSL functions must not be attribute calls. "
                "Use mean_horizontal(x, y) instead of x.mean_horizontal(y)"
            )
        raise NotImplementedError(f"Unsupported function node type: {type(node)}")
