# tests/unit/db/rbac/test_rbac_db_session.py
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from kronicle.db.rbac.rbac_db_session import RbacDbSession


class TestInit:
    def test_creates_engine_and_session_factory(self):
        mock_engine = MagicMock()
        mock_session_factory = MagicMock()

        with patch("kronicle.db.rbac.rbac_db_session.create_engine", return_value=mock_engine) as mock_create_engine:
            with patch(
                "kronicle.db.rbac.rbac_db_session.sessionmaker", return_value=mock_session_factory
            ) as mock_sessionmaker:
                db_url = "sqlite:///test.db"
                rbac = RbacDbSession(db_url, echo=True)

                mock_create_engine.assert_called_once_with(db_url, echo=True, future=True)
                mock_sessionmaker.assert_called_once_with(
                    bind=mock_engine,
                    expire_on_commit=False,
                    class_=Session,
                )
                assert rbac._engine is mock_engine
                assert rbac._session_factory is mock_session_factory


class TestGetDb:
    def test_yields_session_and_closes(self):
        mock_session = MagicMock(spec=Session)
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        with rbac.get_db() as session:
            assert session is mock_session

        mock_factory.assert_called_once_with()
        mock_session.close.assert_called_once_with()

    def test_closes_session_on_exception(self):
        mock_session = MagicMock(spec=Session)
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        with pytest.raises(RuntimeError):
            with rbac.get_db():
                raise RuntimeError("boom")

        mock_session.close.assert_called_once_with()


class TestTransaction:
    def test_yields_session_and_commits(self):
        mock_session = MagicMock(spec=Session)
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        with rbac.transaction() as session:
            assert session is mock_session

        mock_session.commit.assert_called_once_with()
        mock_session.close.assert_called_once_with()

    def test_rollback_on_exception(self):
        mock_session = MagicMock(spec=Session)
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        with pytest.raises(ValueError, match="fail"):
            with rbac.transaction():
                raise ValueError("fail")

        mock_session.rollback.assert_called_once_with()
        mock_session.close.assert_called_once_with()
        mock_session.commit.assert_not_called()

    def test_closes_session_even_on_exception(self):
        mock_session = MagicMock(spec=Session)
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        with pytest.raises(ValueError):
            with rbac.transaction():
                raise ValueError("err")

        mock_session.close.assert_called_once_with()


class TestExecute:
    def test_calls_func_with_session_and_returns_result(self):
        mock_session = MagicMock(spec=Session)
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        result = rbac.execute(lambda s: "ok")

        assert result == "ok"
        mock_session.close.assert_called_once_with()

    def test_catches_error_and_returns_none(self):
        mock_session = MagicMock(spec=Session)
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        with patch("kronicle.db.rbac.rbac_db_session.log_e") as mock_log:
            result = rbac.execute(lambda s: (_ for _ in ()).throw(ValueError("bad")))

        assert result is None
        mock_log.assert_called_once()
        mock_session.close.assert_called_once_with()

    def test_re_raises_when_catch_errors_false(self):
        mock_session = MagicMock(spec=Session)
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        with pytest.raises(RuntimeError, match="critical"):
            rbac.execute(lambda s: (_ for _ in ()).throw(RuntimeError("critical")), catch_errors=False)

        mock_session.close.assert_called_once_with()


class TestPing:
    def test_returns_true_on_success(self):
        mock_session = MagicMock(spec=Session)
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        result = rbac.ping()

        assert result is True
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    def test_returns_false_on_exception(self):
        mock_session = MagicMock(spec=Session)
        mock_session.execute.side_effect = RuntimeError("db down")
        mock_factory = MagicMock(return_value=mock_session)

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._session_factory = mock_factory

        with patch("kronicle.db.rbac.rbac_db_session.log_e") as mock_log:
            result = rbac.ping()

        assert result is False
        mock_log.assert_called_once()
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()


class TestValidateTables:
    def test_calls_validate_table_on_each_model(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        mock_model_1 = MagicMock()
        mock_model_2 = MagicMock()

        with patch("kronicle.db.rbac.rbac_db_session.ALL_RBAC_TABLES", [mock_model_1, mock_model_2]):
            rbac = RbacDbSession.__new__(RbacDbSession)
            rbac._engine = mock_engine

            rbac.validate_tables()

            mock_model_1.validate_table.assert_called_once_with(mock_conn)
            mock_model_2.validate_table.assert_called_once_with(mock_conn)

    def test_raises_on_validation_error(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        mock_model_ok = MagicMock()
        mock_model_bad = MagicMock()
        mock_model_bad.validate_table.side_effect = RuntimeError("table missing")

        with patch("kronicle.db.rbac.rbac_db_session.ALL_RBAC_TABLES", [mock_model_ok, mock_model_bad]):
            rbac = RbacDbSession.__new__(RbacDbSession)
            rbac._engine = mock_engine

            with pytest.raises(RuntimeError, match="RBAC table validation failed"):
                rbac.validate_tables()

            mock_model_ok.validate_table.assert_called_once_with(mock_conn)
            mock_model_bad.validate_table.assert_called_once_with(mock_conn)


class TestClose:
    def test_disposes_engine(self):
        mock_engine = MagicMock()

        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._engine = mock_engine

        rbac.close()

        mock_engine.dispose.assert_called_once_with()

    def test_does_not_dispose_if_engine_is_none(self):
        rbac = RbacDbSession.__new__(RbacDbSession)
        rbac._engine = None  # type: ignore

        rbac.close()
