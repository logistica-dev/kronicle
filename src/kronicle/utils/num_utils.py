def normalize_int(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as e:
            raise ValueError(f"Cannot normalize '{value}' to int") from e
    raise ValueError(f"Cannot normalize type '{type(value).__name__}' to int")


def normalize_float(value) -> float:
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError as e:
            raise ValueError(f"Cannot normalize '{value}' to float") from e
    raise ValueError(f"Cannot normalize type '{type(value).__name__}' to float")
