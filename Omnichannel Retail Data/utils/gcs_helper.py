import re


def to_snake_case(value: str) -> str:
    """
    Convert a value into lowercase snake_case.
    """
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", str(value).strip())
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_").lower()
