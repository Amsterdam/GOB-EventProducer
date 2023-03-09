from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobeventproducer.producer import EventProducer
from tests.mocks.asyncconnection import AsyncConnectionMock
from tests.mocks.eventbuilder import MockEventBuilder


class MockEvent:
    def __init__(self, id):
        self.eventid = id

    def __eq__(self, other):
        return self.eventid == other.eventid


class TestEventProducer(TestCase):
    def test_add_event(self):
        event = MagicMock()
        event.action = 'ACTION'
        event.eventid = 'ID'
        event.data = {
            'some': 'data',
            'int': 8042,
        }
        event.last_event = 15480
        event.catalogue = 'CAT'
        event.entity = 'COLL'
        event.tid = 'TID'

        p = EventProducer('cat', 'coll', MagicMock())
        p.producer = MagicMock()

        connection = MagicMock()
        p._add_event(event, connection, MockEventBuilder())
        connection.publish.assert_called_with(
            "gob.events",
            "cat.coll",
            {
                'header': {
                    'event_type': 'ACTION',
                    'event_id': 'ID',
                    'tid': 'TID',
                    'catalog': 'CAT',
                    'collection': 'COLL',
                },
                'data': {
                    'tid': 'TID',
                    'a': 'A',
                    'b': 'B',
                }
            }
        )

    @patch("gobeventproducer.producer.LocalDatabaseConnection")
    @patch("gobeventproducer.producer.GobDatabaseConnection")
    @patch("gobeventproducer.producer.AsyncConnection", AsyncConnectionMock)
    @patch("gobeventproducer.producer.EventDataBuilder", MockEventBuilder)
    def test_produce(self, mock_gobdb, mock_localdb):
        event_cnt = 5

        localdb_instance = mock_localdb.return_value.__enter__.return_value
        gobdb_instance = mock_gobdb.return_value.__enter__.return_value
        gobdb_instance.get_events = MagicMock(return_value=[MockEvent(i) for i in range(event_cnt)])

        p = EventProducer('', '', MagicMock())
        p._add_event = MagicMock()
        p.gob_db_session = MagicMock()
        localdb_instance.get_last_eventid = MagicMock(return_value=100)

        p.produce(100, 200)

        gobdb_instance.get_events.assert_called_with(100, 200)
        self.assertEqual(event_cnt, p._add_event.call_count)
        gobdb_instance.session.expunge.assert_has_calls([
            call(MockEvent(0)),
            call(MockEvent(1)),
            call(MockEvent(2)),
            call(MockEvent(3)),
            call(MockEvent(4)),
        ])
        localdb_instance.set_last_eventid.assert_has_calls([
            call(0),
            call(1),
            call(2),
            call(3),
            call(4),
        ])
        p.logger.warning.assert_not_called()

        # min_eventid does not match start_event
        p.produce(110, 200)
        p.logger.warning.assert_called_with("Min eventid (110) to produce does not match last_eventid (100) in database. Recovering.")

        # Test with no last event in database
        localdb_instance.get_last_eventid.return_value = -1

        p.produce(110, 200)
        p.logger.warning.assert_called_with("Have no previous produced events in database. Starting at beginning")
