from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobkafkaproducer.producer import _to_bytes, KafkaEventProducer, LastSentEvent


class TestModuleFunctions(TestCase):

    @patch("builtins.bytes")
    def test_to_bytes(self, mock_bytes):
        self.assertEqual(mock_bytes.return_value, _to_bytes('some string'))
        mock_bytes.assert_called_with('some string', encoding='utf-8')


class TestKafkaEventProducerInitConnections(TestCase):
    """Tests constructor and init_connections. init_connections is fully mocked in other test class below.
    """

    @patch("gobkafkaproducer.producer.KafkaEventProducer._init_connections")
    def test_init(self, mock_init_connection):
        prod = KafkaEventProducer('CAT', 'COLL', 'LOGGER')

        self.assertEqual('CAT', prod.catalogue)
        self.assertEqual('COLL', prod.collection)
        self.assertEqual('LOGGER', prod.logger)
        mock_init_connection.assert_called_once()

    @patch("gobkafkaproducer.producer.KafkaEventProducer._init_local_db_session")
    @patch("gobkafkaproducer.producer.KafkaEventProducer._init_gob_db_session")
    @patch("gobkafkaproducer.producer.KafkaEventProducer._init_kafka")
    def test_init_connections(self, mock_init_kafka, mock_init_gob_db, mock_init_local_db):
        KafkaEventProducer('', '', '')
        mock_init_local_db.assert_called_once()
        mock_init_gob_db.assert_called_once()
        mock_init_kafka.assert_called_once()


@patch("gobkafkaproducer.producer.KafkaEventProducer._init_connections", MagicMock())
class TestKafkaEventProducerInit(TestCase):

    @patch("gobkafkaproducer.producer.MetaData")
    @patch("gobkafkaproducer.producer.create_engine")
    @patch("gobkafkaproducer.producer.Session")
    @patch("gobkafkaproducer.producer.URL")
    @patch("gobkafkaproducer.producer.automap_base")
    @patch("gobkafkaproducer.producer.GOB_DATABASE_CONFIG", {'db': 'config'})
    def test_init_gob_db_session(self, mock_automap, mock_url, mock_session, mock_create_engine, mock_metadata):

        p = KafkaEventProducer('', '', MagicMock())
        p._init_gob_db_session()

        # Check initialisation of session
        self.assertEqual(mock_session.return_value, p.gob_db_session)
        mock_session.assert_called_with(mock_create_engine.return_value)
        mock_create_engine.assert_called_with(mock_url.return_value)
        mock_url.assert_called_with(db='config')

        # Check that Event obj is mapped and set correctly
        self.assertEqual(mock_automap.return_value.classes.events, p.Event)
        mock_automap.assert_has_calls([
            call(metadata=mock_metadata.return_value),
            call().prepare(),
        ])
        mock_metadata.assert_has_calls([
            call(),
            call().reflect(mock_create_engine.return_value, only=['events'])
        ])

    @patch("gobkafkaproducer.producer.Base")
    @patch("gobkafkaproducer.producer.create_engine")
    @patch("gobkafkaproducer.producer.Session")
    @patch("gobkafkaproducer.producer.URL")
    @patch("gobkafkaproducer.producer.DATABASE_CONFIG", {'db': 'config'})
    def test_init_local_db_session(self, mock_url, mock_session, mock_create_engine, mock_base):

        p = KafkaEventProducer('', '', MagicMock())
        p._init_local_db_session()

        # Check initialisation of session
        self.assertEqual(mock_session.return_value, p.db_session)
        mock_session.assert_called_with(mock_create_engine.return_value)
        mock_create_engine.assert_called_with(mock_url.return_value)
        mock_url.assert_called_with(db='config')

        self.assertEqual(mock_base.metadata.bind, mock_create_engine.return_value)

    @patch("gobkafkaproducer.producer.KAFKA_CONNECTION_CONFIG", {'kafka': 'config'})
    @patch("gobkafkaproducer.producer.KafkaProducer")
    def test_init_kafka(self, mock_kafka_producer):
        p = KafkaEventProducer('', '', MagicMock())
        p._init_kafka()

        self.assertEqual(mock_kafka_producer.return_value, p.producer)
        mock_kafka_producer.assert_called_with(kafka='config',
                                               max_in_flight_requests_per_connection=1,
                                               retries=3)

    def test_get_last_event(self):
        p = KafkaEventProducer('cat', 'coll', MagicMock())
        p.db_session = MagicMock()

        result = p._get_last_event()
        p.db_session.query.assert_has_calls([
            call(LastSentEvent),
            call().filter_by(catalogue='cat', collection='coll'),
            call().filter_by().first(),
        ])
        self.assertEqual(p.db_session.query().filter_by().first(), result)

    def test_get_last_eventid(self):
        p = KafkaEventProducer('', '', MagicMock())
        p._get_last_event = MagicMock()

        self.assertEqual(p._get_last_event.return_value.last_event, p._get_last_eventid())

        p._get_last_event.return_value = None
        self.assertEqual(-1, p._get_last_eventid())

    def test_set_last_eventid(self):
        p = KafkaEventProducer('cat', 'coll', MagicMock())
        p.db_session = MagicMock()

        # 1. Test existing
        last = LastSentEvent(catalogue='cat', collection='col', last_event=1)
        p._get_last_event = MagicMock(return_value=last)

        p._set_last_eventid(5)

        p.db_session.add.assert_not_called()
        p.db_session.commit.assert_called_once()
        self.assertEqual(5, last.last_event)

        # 2. Test new
        with patch("gobkafkaproducer.producer.LastSentEvent") as mock_last_sent_event:
            p.db_session.commit.reset_mock()

            p._get_last_event.return_value = None
            p._set_last_eventid(10)
            p.db_session.add.assert_called_with(mock_last_sent_event.return_value)
            mock_last_sent_event.assert_called_with(catalogue='cat', collection='coll', last_event=10)
            p.db_session.commit.assert_called_once()

    @patch("gobkafkaproducer.producer.and_")
    def test_get_events(self, mock_and):

        class MockComp:
            asc = MagicMock()
            def __init__(self, name):
                self.name = name
            def __eq__(self, other):
                return f"{self.name} == {other}"
            def __gt__(self, other):
                return f"{self.name} > {other}"

        p = KafkaEventProducer('cat', 'coll', MagicMock())
        p.Event = MagicMock()
        p.Event.catalogue = MockComp("catalogue")
        p.Event.entity = MockComp("entity")
        p.Event.eventid = MockComp("eventid")

        p.gob_db_session = MagicMock()

        res = p._get_events(184)

        p.gob_db_session.query.assert_has_calls([
            call(p.Event),
            call().yield_per(10000),
            call().yield_per().filter(mock_and.return_value),
            call().yield_per().filter().order_by(p.Event.eventid.asc.return_value),
        ])

        self.assertEqual(p.gob_db_session.query().yield_per().filter().order_by(), res)

        mock_and.assert_called_with(
            'catalogue == cat',
            'entity == coll',
            'eventid > 184'
        )

    @patch("gobkafkaproducer.producer.KAFKA_TOPIC", "KAFKA_TOPIC")
    def test_add_event(self):
        event = MagicMock()
        event.name = 'NAME'
        event.id = 'ID'
        event.data = {
            '_entity_source_id': 'ENT SOURCE ID',
            '_source_id': 'SOURCE ID',
            'some': 'data',
            'int': 8042,
        }
        event.last_event = 15480
        event.catalogue = 'CAT'
        event.entity = 'COLL'
        event.source = 'SOURCE'

        p = KafkaEventProducer('cat', 'coll', MagicMock())
        p.producer = MagicMock()

        p._add_event(event)
        p.producer.send.assert_called_with(
            'KAFKA_TOPIC',
            key=b'ENT SOURCE ID',
            value=b'{"_entity_source_id": "ENT SOURCE ID", "_source_id": "SOURCE ID", "some": "data", "int": 8042}',
            headers=[
                ('event_type', b'NAME'),
                ('event_id', b'ID'),
                ('source_id', b'ENT SOURCE ID'),
                ('last_event', b'15480'),
                ('catalog', b'CAT'),
                ('collection', b'COLL'),
                ('source', b'SOURCE'),
            ]
        )

        # Test without _entity_source_id. Source ID should fall back to _source_id
        del event.data['_entity_source_id']
        p._add_event(event)

        p.producer.send.assert_called_with(
            'KAFKA_TOPIC',
            key=b'SOURCE ID',
            value=b'{"_source_id": "SOURCE ID", "some": "data", "int": 8042}',
            headers=[
                ('event_type', b'NAME'),
                ('event_id', b'ID'),
                ('source_id', b'SOURCE ID'),
                ('last_event', b'15480'),
                ('catalog', b'CAT'),
                ('collection', b'COLL'),
                ('source', b'SOURCE'),
            ]
        )

    @patch("builtins.print", MagicMock())
    def test_flush(self):
        p = KafkaEventProducer('', '', '')
        p._set_last_eventid = MagicMock()
        p.producer = MagicMock()

        p._flush(2480)
        p.producer.flush.assert_called_with(timeout=120)
        p._set_last_eventid.assert_called_with(2480)

    @patch("gobkafkaproducer.producer.database_to_gobevent")
    def test_produce(self, mock_db_to_gobevent):
        class MockEvent:
            def __init__(self, id):
                self.id = id

        event_cnt = 5

        p = KafkaEventProducer('', '', MagicMock())
        p._get_last_eventid = MagicMock(return_value=240)
        p._get_events = MagicMock(return_value=[i for i in range(event_cnt)])
        p._add_event = MagicMock()
        p._flush = MagicMock()
        p.FLUSH_PER = 2
        mock_db_to_gobevent.side_effect = lambda i: MockEvent(i)

        p.produce()

        p._get_events.assert_called_with(240)
        self.assertEqual(event_cnt, p._add_event.call_count)
        p._flush.assert_has_calls([
            call(1),
            call(3),
            call(4),
        ])

        # Test last flush 0
        p.FLUSH_PER = event_cnt
        p._flush.reset_mock()
        p.produce()
        p._flush.assert_has_calls([
            call(4),
            call(4),  # Last flush happens with the same argument because nothing was added between flushes. That's ok.
        ])
