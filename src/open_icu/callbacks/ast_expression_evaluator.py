import ast

import polars as pl
from polars import Expr, LazyFrame

from open_icu.callbacks.proto import CallbackProtocol, ExpressionCallback, FrameCallback
from open_icu.callbacks.registry import CallbackRegistry, register_callback_class


@register_callback_class
class AbstractSyntaxTree(CallbackProtocol):
    """Parse and execute a Python-like expression using Polars.

    The expression is parsed via Python AST. A top-level `FrameCallback` call
    is executed on the `LazyFrame`; otherwise the expression is translated into
    a Polars expression and written to `output`.

    Supported:
    - Literals and column names
    - Unary ops: +x, -x
    - Binary ops: +, -, *, /, **, %
    - Calls:
        - Registered `ExpressionCallback`s
        - Built-ins: mean, sum, prod, root
    """

    def __init__(self, expr: str, output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            expr: Expression string to parse.
            output: Output column name.
        """
        self.expr = expr
        self.output = output
        self.registry = CallbackRegistry()
    
    
    def __call__(self, lf: LazyFrame) -> LazyFrame:
        """Apply the AST expression to a LazyFrame.

        Args:
            lf: Input LazyFrame.

        Returns:
            Transformed LazyFrame.

        Raises:
            ValueError: If no output column is specified.
            TypeError: If an invalid callback type is encountered.
        """
        node = ast.parse(self.expr, mode="eval").body
        if isinstance(node, ast.Call) and \
            (func_name := self._get_func_name(node.func)) in self.registry:
                CallbackClass = self.registry[func_name]
                if not issubclass(CallbackClass, ExpressionCallback):
                    if not issubclass(CallbackClass, FrameCallback):
                        raise TypeError(f"Unknown callback type: {type(self.expr)}")
                    return CallbackClass(*[self._ast_to_polars(a) for a in node.args])(lf)
        expr = self._ast_to_polars(node)
        if self.output is None:
                raise ValueError("Missing output column")
        return lf.with_columns(expr.alias(self.output))
    
    def _ast_to_polars(self, node: ast.AST) -> Expr:
        """Convert an AST node to a Polars expression.

        Args:
            node: AST node to translate.

        Returns:
            Polars expression equivalent to the AST node.

        Raises:
            NotImplementedError: Unsupported node, operator, or function.
            TypeError: If a `FrameCallback` is used in an expression context.
        """

        if isinstance(node, ast.Constant):
            value = node.value
            if isinstance(value, str) and value.lower() in {"none", "null"}:
                value = None
            return pl.lit(value)
        
        if(isinstance(node, ast.Name)):
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
            if isinstance(node.args[-1], ast.Name):
                output = node.args[-1].id
            args = [self._ast_to_polars(a) for a in node.args]

            if func_name in self.registry:
                CallbackClass = self.registry[func_name]
                if issubclass(CallbackClass, ExpressionCallback):
                    callback = CallbackClass(*args)
                    if callback.output is not None:
                        self.output = output
                    return callback.as_expression()
                if issubclass(CallbackClass, FrameCallback):
                    raise TypeError(f"FrameCallback not allowed inside of abstract syntax tree: {node}")
        
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
        
        if isinstance(node, ast.List):
            return [self._ast_to_polars(e) for e in node.elts] # type: ignore

        raise NotImplementedError(f"Unsupported AST node: {type(node)}")
    

    def _get_func_name(self, node: ast.AST) -> str:
        """Resolve the function name from an AST call node.

        Args:
            node: Function AST node.

        Returns:
            Resolved function name.

        Raises:
            NotImplementedError: Unsupported or illegal function node.
        """
        if isinstance(node, ast.Name):
            return node.id
        
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id in ("str", "dt", "arr"):
                return node.attr

            raise NotImplementedError(f"Illegal attribute call: {ast.dump(node)}.")
        
        raise NotImplementedError(f"Unsupported function node type: {type(node)}")
