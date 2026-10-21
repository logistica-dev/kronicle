"""Tests for the ColumnMetadata model."""

from __future__ import annotations

import pytest

from kronicle.db.data.models.column_metadata import (
    ColumnMetadata,
    normalize_column_metadata,
)


class TestColumnMetadata:
    """Tests for ColumnMetadata."""

    def test_none_values_are_kept_as_none(self) -> None:
        """None input stays None."""
        metadata = ColumnMetadata(unit=None)
        assert metadata.unit is None

    def test_non_string_raises_type_error(self) -> None:
        """Non-string values raise a TypeError."""
        with pytest.raises(TypeError, match="must be a string or None"):
            ColumnMetadata(unit=123)  # type: ignore[arg-type]

    def test_bool_reflects_any_content(self) -> None:
        """Bool is True when there is any descriptive content."""
        assert not ColumnMetadata()
        assert bool(ColumnMetadata(unit="Cel"))
        assert bool(ColumnMetadata(precision=2))  # type: ignore[call-arg]  # extra="allow" field


class TestNormalizeColumnMetadata:
    """Tests for normalize_column_metadata."""

    def test_none_returns_empty_dict(self) -> None:
        """None input returns an empty dict."""
        assert normalize_column_metadata(None, allowed_columns=["temp"]) == {}

    def test_empty_returns_empty_dict(self) -> None:
        """Empty input returns an empty dict."""
        assert normalize_column_metadata({}, allowed_columns=["temp"]) == {}

    def test_coerces_dicts_and_drops_empty_entries(self) -> None:
        """Bare dicts are coerced; entries without content are dropped."""
        result = normalize_column_metadata(
            {"temp": {"unit": "Cel"}, "hum": {}},
            allowed_columns=["temp", "hum"],
        )
        assert set(result) == {"temp"}
        assert isinstance(result["temp"], ColumnMetadata)
        assert result["temp"].unit == "Cel"

    def test_unknown_column_raises(self) -> None:
        """A key that is not an allowed column is rejected."""
        from kronicle.errors.error_types import BadRequestError

        with pytest.raises(BadRequestError):
            normalize_column_metadata({"nope": {"unit": "Cel"}}, allowed_columns=["temp"])
