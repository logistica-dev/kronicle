# tests/unit/db/base/test_kronicle_link.py
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import Column, String

from kronicle.db.base.kronicle_link import KronicleLink


class TestRelationshipContract:
    """Pin the ORM navigation attribute names to the KronicleLink constants.

    The engine lambdas (rbac_service, core_service) resolve links via
    getattr(..., KronicleLink.PARENT/CHILD/PARENTS/CHILDREN). Renaming a
    relationship (or a constant) must fail here, not at runtime.
    """

    def test_group_hierarchy_relationship_names_track_constants(self):
        from kronicle.db.rbac.links.group_hierarchy import RbacGroupHierarchy
        from kronicle.db.rbac.models.rbac_group import RbacGroup

        link_rels = RbacGroupHierarchy.__mapper__.relationships
        assert KronicleLink.PARENT in link_rels
        assert KronicleLink.CHILD in link_rels

        group_rels = RbacGroup.__mapper__.relationships
        assert KronicleLink.PARENT_LINKS in group_rels
        assert KronicleLink.CHILDREN in group_rels

    def test_zone_hierarchy_relationship_names_track_constants(self):
        from kronicle.db.core.links.zone_hierarchy import ZoneHierarchy
        from kronicle.db.core.models.core_zone import CoreZone

        link_rels = ZoneHierarchy.__mapper__.relationships
        assert KronicleLink.PARENT in link_rels
        assert KronicleLink.CHILD in link_rels

        zone_rels = CoreZone.__mapper__.relationships
        assert KronicleLink.PARENT_LINKS in zone_rels
        assert KronicleLink.CHILDREN in zone_rels


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


class TestAdd:
    def test_calls_insert_on_conflict_do_nothing(self, mock_db):
        parent = MagicMock()
        parent.id = uuid4()
        child = MagicMock()
        child.id = uuid4()

        class AddTestLink(KronicleLink):
            __tablename__ = "add_test_links"
            UQ_CONSTRAINT = "uq_test"

            @classmethod
            def namespace(cls):
                return "test"

        AddTestLink.add(mock_db, parent, child)

        mock_db.execute.assert_called_once()
        args, _ = mock_db.execute.call_args
        stmt = args[0]
        assert hasattr(stmt, "on_conflict_do_nothing")


class TestRemove:
    def test_calls_delete_with_conditions(self, mock_db):
        parent = MagicMock()
        parent.id = uuid4()
        child = MagicMock()
        child.id = uuid4()

        class RemoveTestLink(KronicleLink):
            __tablename__ = "remove_test_links"
            UQ_CONSTRAINT = "uq_test"
            parent_id = Column(String, primary_key=True)
            child_id = Column(String, primary_key=True)

            @classmethod
            def namespace(cls):
                return "test"

        RemoveTestLink.remove(mock_db, parent, child)

        mock_db.execute.assert_called_once()
