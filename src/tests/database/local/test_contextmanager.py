from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobeventproducer.database.local.contextmanager import LocalDatabaseConnection
from gobeventproducer.database.local.model import LastSentEvent


class TestLocalDatabaseConnection(TestCase):
    @patch("gobeventproducer.database.local.contextmanager.Base")
    @patch("gobeventproducer.database.local.contextmanager.create_engine")
    @patch("gobeventproducer.database.local.contextmanager.Session")
    @patch("gobeventproducer.database.local.contextmanager.URL")
    @patch("gobeventproducer.database.local.contextmanager.DATABASE_CONFIG", {'db': 'config'})
    def test_init_local_db_session(self, mock_url, mock_session, mock_create_engine, mock_base):
        inst = LocalDatabaseConnection('', '')
        inst._connect()

        # Check initialisation of session
        self.assertEqual(mock_session.return_value, inst.session)
        mock_session.assert_called_with(mock_create_engine.return_value)
        mock_create_engine.assert_called_with(mock_url.return_value, connect_args={'sslmode': 'require'})
        mock_url.assert_called_with(db='config')

        self.assertEqual(mock_base.metadata.bind, mock_create_engine.return_value)

    def test_context_manager(self):
        inst = LocalDatabaseConnection('', '')
        inst._connect = MagicMock()
        inst.session = MagicMock()

        with inst as tst:
            self.assertEqual(tst, inst)

            inst._connect.assert_called_once()
        inst.session.commit.assert_called_once()

    def test_get_last_event(self):
        inst = LocalDatabaseConnection('cat', 'coll')
        inst.session = MagicMock()

        # 1. Already set
        mock_last_event = MagicMock()
        inst.last_event = mock_last_event
        self.assertEqual(inst.get_last_event(), mock_last_event)

        # 2. Query database, return result
        inst.last_event = None
        database_result = MagicMock()
        inst.session.query.return_value.filter_by.return_value.first.return_value = database_result
        result = inst.get_last_event()

        inst.session.query.assert_has_calls([
            call(LastSentEvent),
            call().filter_by(catalogue='cat', collection='coll'),
            call().filter_by().first(),
        ])
        self.assertEqual(result, database_result)
        self.assertEqual(inst.last_event, database_result)

        # 3. Query database, no result, create new one
        inst.last_event = None
        inst.session.query.return_value.filter_by.return_value.first.return_value = None

        with patch("gobeventproducer.database.local.contextmanager.LastSentEvent") as mock_last_sent_event:
            result = inst.get_last_event()
            inst.session.add.assert_called_with(mock_last_sent_event.return_value)
            mock_last_sent_event.assert_called_with(catalogue='cat', collection='coll', last_event=-1)

            self.assertEqual(inst.last_event, mock_last_sent_event.return_value)
            self.assertEqual(result, mock_last_sent_event.return_value)

    def test_get_last_eventid(self):
        inst = LocalDatabaseConnection('', '')
        inst.get_last_event = MagicMock()
        inst.get_last_event.return_value.last_event = 1480

        self.assertEqual(1480, inst.get_last_eventid())
        self.assertEqual(inst.get_last_event.return_value.last_event, inst.get_last_eventid())

    def test_set_last_eventid(self):
        inst = LocalDatabaseConnection('', '')
        inst.last_event = MagicMock()
        inst.last_event.last_event = 48

        inst.set_last_eventid(20)
        self.assertEqual(inst.last_event.last_event, 20)

