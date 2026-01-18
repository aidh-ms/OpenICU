import re

CAMEL_CASE_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")


def camel_to_snake(s: str) -> str:
    """Convert CamelCase string to snake_case.

    Args:
        s: The CamelCase string.

    Returns:
        The converted snake_case string.
    """
    return CAMEL_CASE_PATTERN.sub("_", s).lower()
