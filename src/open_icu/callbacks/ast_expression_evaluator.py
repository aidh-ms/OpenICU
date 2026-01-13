import ast

import polars as pl
from polars import Expr, LazyFrame

from open_icu.callbacks.proto import CallbackProtocol, ExpressionCallback, FrameCallback 
from open_icu.callbacks.registry import CallbackRegistry, register_callback_class


@register_callback_class
class AbstractSyntaxTree(CallbackProtocol):
    """Evaluate an expression via Python AST and translate it to Polars.

    The callback parses a Python-like expression string (e.g. `"a + b * 2"`)
    into an AST and converts supported nodes into a Polars `Expr`. The resulting
    expression is added to the input `LazyFrame` as a new column.

    In addition, if the *top-level* expression is a call to a registered
    `FrameCallback`, that callback is executed on the frame directly.

    Supported constructs:
    - Constants and column names
    - Unary operations: `+x`, `-x`
    - Binary operations: `+`, `-`, `*`, `/`, `**`, `%`
    - Function calls:
        - Registered `ExpressionCallback`s (via `as_expression()`)
        - Built-ins: `mean(...)`, `sum(...)`, `prod(...)`, `root(radicand, index)`
    """

    def __init__(self, expr: str, output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            expression: AST expression to parse and evaluate.
            result: Name of the output column to be created.
        """
        self.expr = expr
        self.result = output
    
    
    def __call__(self, lf: LazyFrame) -> LazyFrame:
        """Apply the transformation to a Polars LazyFrame.

        If the top-level AST node is a call to a registered `FrameCallback`,
        that callback is invoked and its result returned. Otherwise, the
        expression is translated into a Polars `Expr` and appended as a new
        column named `result`.

        Args:
            lf: Input LazyFrame.

        Returns:
            A new LazyFrame after either executing a `FrameCallback` or adding
            the evaluated expression as a column.
        """
        node = ast.parse(self.expr, mode="eval").body
        registry = CallbackRegistry()

        if isinstance(node, ast.Call) and \
            (func_name := self._get_func_name(node.func)) in registry:
                CallbackClass = registry[func_name]
                if not issubclass(CallbackClass, ExpressionCallback):
                    if not issubclass(CallbackClass, FrameCallback):
                        raise TypeError(f"Unknown callback type: {type(self.expr)}")
                    return CallbackClass(*[self._ast_to_polars(a) for a in node.args])(lf)

        return lf.with_columns(self._ast_to_polars(node))
        # return lf.with_columns(self._ast_to_polars(node).alias(self.result))
    
    def _ast_to_polars(self, node: ast.AST) -> Expr:
        """Translate an AST node into a Polars expression.

        Args:
            node: AST node produced by parsing the expression.

        Returns:
            A Polars `Expr` equivalent to the given AST node.

        Raises:
            NotImplementedError: If the node type or operator/function is not supported.
            TypeError: If a `FrameCallback` is used inside an expression context.
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
            args = [self._ast_to_polars(a) for a in node.args]

            registry = CallbackRegistry()

            if func_name in registry:
                CallbackClass = registry[func_name]
                if issubclass(CallbackClass, ExpressionCallback):
                    return CallbackClass(*args).as_expression()
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
        """Extract the function identifier from an AST function node.

        Args:
            node: AST node representing the function part of a call.

        Returns:
            The resolved function name.

        Raises:
            NotImplementedError: If the function node uses unsupported constructs
                (e.g. illegal attribute calls or unknown node shapes).
        """
        if isinstance(node, ast.Name):
            return node.id
        
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id in ("str", "dt", "arr"):
                return node.attr

            raise NotImplementedError(f"Illegal attribute call: {ast.dump(node)}.")
        
        raise NotImplementedError(f"Unsupported function node type: {type(node)}")
