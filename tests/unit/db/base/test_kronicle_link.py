# tests/unit/db/base/test_kronicle_link.py
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Column, String

from kronicle.db.base.kronicle_link import KronicleLink


@pytest.fixture
def mock_db():
    return MagicMock()


class TestInitSubclass:
    def test_missing_uq_constraint_raises(self):
        with pytest.raises(TypeError, match="must define UQ_CONSTRAINT"):

            class BadLink(KronicleLink):
                __tablename__ = "bad_link"
                __abstract__ = False

                @classmethod
                def namespace(cls):
                    return "test"

    def test_abstract_class_skips_validation(self):
        class AbstractLink(KronicleLink):
            __abstract__ = True

        assert AbstractLink.__abstract__ is True

    def test_valid_subclass_works(self):
        class ValidLink(KronicleLink):
            __tablename__ = "valid_links"
            UQ_CONSTRAINT = "uq_valid_link"
            parent_id = Column(String, primary_key=True)
            child_id = Column(String, primary_key=True)

            @classmethod
            def namespace(cls):
                return "test"

        assert ValidLink.UQ_CONSTRAINT == "uq_valid_link"

    def test_non_abstract_valid_subclass_passes_table_check(self):
        """A non-abstract subclass with a UQ_CONSTRAINT and PK columns passes validation."""
        from sqlalchemy import PrimaryKeyConstraint

        class CheckedLink(KronicleLink):
            __tablename__ = "checked_links"
            __abstract__ = False
            UQ_CONSTRAINT = "uq_checked_link"
            parent_id = Column(String, primary_key=True)
            child_id = Column(String, primary_key=True)

            @classmethod
            def namespace(cls):
                return "test"

        assert CheckedLink.__table__ is not None
        assert CheckedLink.__table__.primary_key.columns
        assert isinstance(CheckedLink.__table__.primary_key, PrimaryKeyConstraint)

    def test_non_abstract_subclass_without_primary_key_raises(self):
        """An explicit __table__ without PK columns is rejected."""
        from sqlalchemy import Table

        no_pk_table = Table(
            "link_no_pk",
            KronicleLink.metadata,  # type: ignore[attr-defined]
            Column("payload", String),
        )

        with pytest.raises(TypeError, match="has no primary key columns"):

            class NoPkLink(KronicleLink):
                __table__ = no_pk_table
                __abstract__ = False
                UQ_CONSTRAINT = "uq_no_pk"

                @classmethod
                def namespace(cls):
                    return "test"


class TestUqConstraint:
    def test_returns_constraint_when_defined(self):
        class ValidLinkForUq(KronicleLink):
            __tablename__ = "valid_links_for_uq"
            UQ_CONSTRAINT = "uq_valid_link"
            parent_id = Column(String, primary_key=True)
            child_id = Column(String, primary_key=True)

            @classmethod
            def namespace(cls):
                return "test"

        assert ValidLinkForUq.uq_constraint() == "uq_valid_link"

    def test_raises_when_missing(self):
        class NoConstraintLink(KronicleLink):
            __tablename__ = "no_constraint"
            UQ_CONSTRAINT = ""

            @classmethod
            def namespace(cls):
                return "test"

        with pytest.raises(NotImplementedError, match="should define UQ_CONSTRAINT"):
            NoConstraintLink.uq_constraint()
