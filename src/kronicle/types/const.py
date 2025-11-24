from types import SimpleNamespace


class FrozenNamespace(SimpleNamespace):
    def __setattr__(self, name, value):
        raise TypeError(f"Cannot reassign constant '{name}'")


# --------------------------------------------------------------------------------------------------
# Main test
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    here = "const.test"

    # Define constants
    CONST = FrozenNamespace(
        JWT_EXPIRATION_MINUTES=5,
        API_VERSION="v1",
        MAX_RETRIES=3,
    )

    print(CONST.JWT_EXPIRATION_MINUTES)  # 5

    # Attempt to change raises error
    try:
        CONST.JWT_EXPIRATION_MINUTES = 10
    except TypeError as e:
        print(here, "TypeError (expected):", e)
