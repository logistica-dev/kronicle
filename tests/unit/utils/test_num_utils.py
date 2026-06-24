import pytest

from kronicle.utils.num_utils import normalize_float, normalize_int


class TestNormalizeInt:
    def test_int_passthrough(self):
        assert normalize_int(42) == 42

    def test_zero(self):
        assert normalize_int(0) == 0

    def test_negative_int(self):
        assert normalize_int(-5) == -5

    def test_str_conversion(self):
        assert normalize_int("42") == 42

    def test_str_negative(self):
        assert normalize_int("-5") == -5

    def test_str_zero(self):
        assert normalize_int("0") == 0

    def test_str_invalid_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize 'abc' to int"):
            normalize_int("abc")

    def test_str_empty_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize '' to int"):
            normalize_int("")

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize type 'float' to int"):
            normalize_int(3.14)

    def test_none_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize type 'NoneType' to int"):
            normalize_int(None)

    def test_list_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize type 'list' to int"):
            normalize_int([1, 2, 3])


class TestNormalizeFloat:
    def test_int_conversion(self):
        assert normalize_float(42) == 42.0

    def test_float_passthrough(self):
        assert normalize_float(3.14) == 3.14

    def test_zero(self):
        assert normalize_float(0) == 0.0

    def test_negative_float(self):
        assert normalize_float(-2.5) == -2.5

    def test_str_conversion(self):
        assert normalize_float("3.14") == 3.14

    def test_str_int(self):
        assert normalize_float("42") == 42.0

    def test_str_negative(self):
        assert normalize_float("-1.5") == -1.5

    def test_str_invalid_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize 'abc' to float"):
            normalize_float("abc")

    def test_str_empty_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize '' to float"):
            normalize_float("")

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize type 'list' to float"):
            normalize_float([1, 2, 3])

    def test_none_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize type 'NoneType' to float"):
            normalize_float(None)

    def test_dict_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize type 'dict' to float"):
            normalize_float({})
