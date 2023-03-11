from unittest import TestCase, mock

import gobeventproducer.database
from gobeventproducer.database.local.connection import connect, disconnect, is_connected, migrate_storage


class MockedService:
    service_id = None
    id = None
    host = None
    name = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class MockedSession:
    def __init__(self):
        self._first = None
        self._add = None
        self._delete = None
        self._all = []
        self.filter_kwargs = {}
        self.update_args = ()
        pass

    def rollback(self):
        pass

    def close(self):
        pass

    def execute(self):
        pass

    def query(self, anyClass):
        return self

    def get(self, arg):
        return arg

    def filter_by(self, *args, **kwargs):
        self.filter_kwargs = kwargs
        return self

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return self._all

    def first(self):
        return self._first

    def add(self, anyObject):
        self._add = anyObject
        return self

    def order_by(self, *args, **kwargs):
        return self

    def delete(self, anyObject=None):
        self._delete = anyObject
        return self

    def commit(self):
        pass

    def update(self, *args, **kwargs):
        self.update_args = args
        return 1


class MockedEngine:
    def dispose(self):
        pass

    def execute(self, stmt):
        self.stmt = stmt

    def begin(self):
        return self

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


class MockException(Exception):
    pass


def raise_exception(e):
    raise e("Raised")


class TestStorage(TestCase):
    def setUp(self):
        gobeventproducer.database.local.connection.engine = MockedEngine()
        gobeventproducer.database.local.connection.session = MockedSession()

    @mock.patch("gobeventproducer.database.local.connection.URL.create")
    @mock.patch("gobeventproducer.database.local.connection.migrate_storage")
    @mock.patch("gobeventproducer.database.local.connection.create_engine")
    def test_connect(self, mock_create, mock_migrate, mock_url):
        result = connect()

        mock_create.assert_called_with(mock_url.return_value, connect_args={"sslmode": "require"})
        mock_migrate.assert_called()
        self.assertEqual(result, True)
        self.assertEqual(is_connected(), True)

    @mock.patch("gobeventproducer.database.local.connection.DBAPIError", MockException)
    @mock.patch("gobeventproducer.database.local.connection.create_engine", mock.MagicMock())
    @mock.patch(
        "gobeventproducer.database.local.connection.migrate_storage", lambda argv: raise_exception(MockException)
    )
    def test_connect_error(self):
        # Operation errors should be catched
        result = connect()

        self.assertEqual(result, False)
        self.assertEqual(is_connected(), False)

    @mock.patch(
        "gobeventproducer.database.local.connection.migrate_storage",
        lambda force_migrate: raise_exception(MockException),
    )
    @mock.patch("gobeventproducer.database.local.connection.create_engine", mock.MagicMock())
    def test_connect_other_error(self):
        # Only operational errors should be catched
        with self.assertRaises(MockException):
            connect()

    @mock.patch("gobeventproducer.database.local.connection.engine.dispose")
    @mock.patch("gobeventproducer.database.local.connection.session.close")
    @mock.patch("gobeventproducer.database.local.connection.session.rollback")
    def test_disconnect(self, mock_rollback, mock_close, mock_dispose):
        disconnect()

        mock_rollback.assert_called()
        mock_close.assert_called()
        mock_dispose.assert_called()

        self.assertEqual(gobeventproducer.database.local.connection.session, None)
        self.assertEqual(gobeventproducer.database.local.connection.engine, None)
        self.assertEqual(is_connected(), False)

    @mock.patch("gobeventproducer.database.local.connection.DBAPIError", MockException)
    @mock.patch("gobeventproducer.database.local.connection.engine.dispose", lambda: raise_exception(MockException))
    @mock.patch("gobeventproducer.database.local.connection.session.close", mock.MagicMock())
    @mock.patch("gobeventproducer.database.local.connection.session.rollback", mock.MagicMock())
    def test_disconnect_operational_error(self):
        # Operation errors should be catched

        disconnect()

        self.assertEqual(gobeventproducer.database.local.connection.session, None)
        self.assertEqual(gobeventproducer.database.local.connection.engine, None)

    @mock.patch("gobeventproducer.database.local.connection.engine.dispose", lambda: raise_exception(MockException))
    @mock.patch("gobeventproducer.database.local.connection.session.close", mock.MagicMock())
    @mock.patch("gobeventproducer.database.local.connection.session.rollback", mock.MagicMock())
    def test_disconnect_other_error(self):
        # Only operational errors should be catched

        with self.assertRaises(MockException):
            disconnect()

    def test_is_connected_not_ok(self):
        result = is_connected()
        self.assertEqual(result, False)

    @mock.patch("gobeventproducer.database.local.connection.session.execute", mock.MagicMock())
    def test_is_connected_ok(self):
        result = is_connected()
        self.assertEqual(result, True)

    @mock.patch("gobeventproducer.database.local.connection.alembic.config")
    @mock.patch("gobeventproducer.database.local.connection.alembic.script")
    @mock.patch("gobeventproducer.database.local.connection.migration")
    def test_migrate_storage(self, mock_migration, mock_script, mock_config):
        context = mock.MagicMock()
        context.get_current_revision.return_value = "revision 1"
        mock_migration.MigrationContext.configure.return_value = context

        script = mock.MagicMock()
        script.get_current_head.return_value = "revision 2"
        mock_script.ScriptDirectory.from_config.return_value = script

        migrate_storage(force_migrate=True)
        self.assertEqual(script.get_current_head.call_count, 1)
        self.assertEqual(context.get_current_revision.call_count, 1)
        mock_config.main.assert_called()

    @mock.patch("gobeventproducer.database.local.connection.alembic.config")
    @mock.patch("gobeventproducer.database.local.connection.alembic.script")
    @mock.patch("gobeventproducer.database.local.connection.migration")
    def test_migrate_storage_up_to_date(self, mock_migration, mock_script, mock_config):
        context = mock.MagicMock()
        context.get_current_revision.return_value = "revision 2"
        mock_migration.MigrationContext.configure.return_value = context

        script = mock.MagicMock()
        script.get_current_head.return_value = "revision 2"
        mock_script.ScriptDirectory.from_config.return_value = script

        migrate_storage(force_migrate=False)
        self.assertEqual(script.get_current_head.call_count, 1)
        self.assertEqual(context.get_current_revision.call_count, 1)
        mock_config.main.assert_not_called()

    @mock.patch("gobeventproducer.database.local.connection.alembic.config")
    @mock.patch("gobeventproducer.database.local.connection.alembic.script")
    @mock.patch("gobeventproducer.database.local.connection.migration")
    def test_migrate_storage_exception(self, mock_migration, mock_script, mock_config):
        context = mock.MagicMock()
        context.get_current_revision.return_value = "revision 1"
        mock_migration.MigrationContext.configure.return_value = context

        script = mock.MagicMock()
        script.get_current_head.return_value = "revision 2"
        mock_script.ScriptDirectory.from_config.return_value = script

        mock_config.main = lambda argv: raise_exception(MockException)

        migrate_storage(force_migrate=False)
        self.assertEqual(script.get_current_head.call_count, 1)
        self.assertEqual(context.get_current_revision.call_count, 1)
