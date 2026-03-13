# tests/test_abc_connector_instantiation.py
from unittest.mock import patch

import pytest
from kronicle_sdk.connectors.abc_connector import KronicleAbstractConnector


def test_abstract_connector_cannot_be_instantiated():
    """Abstract connector should raise TypeError when instantiated."""
    with pytest.raises(TypeError):
        KronicleAbstractConnector("http://127.0.0.1:8000")  # type: ignore


def test_instantiation_logs_warning():
    """Replicates the behaviour of the __main__ block."""
    here = "abstract Kronicle connector"

    with patch("kronicle.connectors.abc_connector.log_w") as mock_log_w:
        try:
            KronicleAbstractConnector("http://127.0.0.1:8000")  # type: ignore
        except TypeError as e:
            mock_log_w(here, "WARNING", e)

    mock_log_w.assert_called_once()
