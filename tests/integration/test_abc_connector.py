# tests/test_abc_connector_instantiation.py

import pytest
from kronicle_sdk.connectors.abc_connector import KronicleAbstractConnector


def test_abstract_connector_cannot_be_instantiated():
    """Abstract connector should raise TypeError when instantiated."""
    with pytest.raises(TypeError):
        KronicleAbstractConnector("http://127.0.0.1:8000")  # type: ignore
