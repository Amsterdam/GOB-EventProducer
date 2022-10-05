from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobkafkaproducer.producer import _to_bytes, KafkaEventProducer, LastSentEvent, EventDataBuilder


class TestModuleFunctions(TestCase):

    @patch("builtins.bytes")
    def test_to_bytes(self, mock_bytes):
        self.assertEqual(mock_bytes.return_value, _to_bytes('some string'))
        mock_bytes.assert_called_with('some string', encoding='utf-8')


class TestEventDataBuilder(TestCase):

    mock_gobmodel_data = {
            'cat': {
                'collections': {
                    'coll': {
                        'attributes': {}
                    }
                }
            }
        }

    @patch("gobkafkaproducer.producer.split_relation_table_name")
    @patch("gobkafkaproducer.producer.get_relations_for_collection")
    @patch("gobkafkaproducer.producer.gob_model", spec_set=True)
    def test_init(self, mock_model, mock_get_relations, mock_split_relation_table_name):
        session = MagicMock()
        base = MagicMock()
        mock_model.__getitem__.return_value = self.mock_gobmodel_data['cat']
        mock_model.get_table_name = lambda x, y: f"{x}_{y}"
        mock_get_relations.return_value = {
            'rel_a_attr': 'a',
            'rel_b_attr': 'b',
        }
        mock_split_relation_table_name.side_effect = lambda x: {
            'dst_cat_abbr': 'dstcat', 'dst_col_abbr': x}
        mock_model.get_reference_by_abbreviations = lambda x, y: y
        mock_model.get_table_name_from_ref = lambda y: f'dst_table_{y}'
        edb = EventDataBuilder(session, base, 'cat', 'coll')

        self.assertEqual(session, edb.db_session)
        self.assertEqual(base, edb.base)
        self.assertEqual(self.mock_gobmodel_data['cat']['collections']['coll'], edb.collection)
        self.assertEqual('cat_coll', edb.tablename)

        expected_relations = {
            'rel_a_attr': {
                'dst_table_name': 'dst_table_rel_a',
                'relation_table_name': 'rel_a'
            },
            'rel_b_attr': {
                'dst_table_name': 'dst_table_rel_b',
                'relation_table_name': 'rel_b'
            }
        }
        self.assertEqual(expected_relations, edb.relations)

    def test_build_event(self):
        class RelationObject:
            # Create mock relation for DbObject
            def __init__(self, id, begin_geldigheid, eind_geldigheid, dst_name, dst_has_states):
                self.begin_geldigheid = begin_geldigheid
                self.eind_geldigheid = eind_geldigheid

                dst_fields = {
                    '_id': id,
                    '_tid': id
                }
                if dst_has_states:
                    dst_fields['volgnummer'] = 1

                self.__setattr__(dst_name, type("DstTable", (), dst_fields))

        class DbObject:
            id = 42
            identificatie = 'identificatie'
            ref_to_c = None
            manyref_to_c = None
            ref_to_d = None
            manyref_to_d = None
            rel_tst_rta_tst_rtc_ref_to_c_collection = [
                RelationObject('id1', 'begingeldigheid', None, 'test_catalogue_rel_test_entity_c', True),
                RelationObject('id2', 'begingeldigheid', 'eindgeldigheid', 'test_catalogue_rel_test_entity_c', True),
            ]
            rel_tst_rta_tst_rtc_manyref_to_c_collection = [
                RelationObject('id3', 'begingeldigheid', None, 'test_catalogue_rel_test_entity_c', True),
                RelationObject('id4', 'begingeldigheid', 'eindgeldigheid', 'test_catalogue_rel_test_entity_c', True),
            ]
            rel_tst_rta_tst_rtd_ref_to_d_collection = [
                RelationObject('id5', 'begingeldigheid', None, 'test_catalogue_rel_test_entity_d', False),
                RelationObject('id6', 'begingeldigheid', 'eindgeldigheid', 'test_catalogue_rel_test_entity_d', False),
            ]
            rel_tst_rta_tst_rtd_manyref_to_d_collection = [
                RelationObject('id7', 'begingeldigheid', None, 'test_catalogue_rel_test_entity_d', False),
                RelationObject('id8', 'begingeldigheid', 'eindgeldigheid', 'test_catalogue_rel_test_entity_d', False),
            ]

        edb = EventDataBuilder(MagicMock(), MagicMock(), 'test_catalogue', 'rel_test_entity_a')

        edb.db_session.query.return_value.filter.return_value.one.return_value = DbObject()

        expected = {
            'id': '42',
            'identificatie': 'identificatie',
            'manyref_to_c': [
                {
                    'begin_geldigheid': 'begingeldigheid',
                    'eind_geldigheid': None,
                    'id': 'id3',
                    'tid': 'id3',
                    'volgnummer': 1
                },
                {
                    'begin_geldigheid': 'begingeldigheid',
                    'eind_geldigheid': 'eindgeldigheid',
                    'id': 'id4',
                    'tid': 'id4',
                    'volgnummer': 1
                }
            ],
            'manyref_to_d': [
                {
                    'begin_geldigheid': 'begingeldigheid',
                    'eind_geldigheid': None,
                    'id': 'id7',
                    'tid': 'id7'
                },
                {
                    'begin_geldigheid': 'begingeldigheid',
                    'eind_geldigheid': 'eindgeldigheid',
                    'id': 'id8',
                    'tid': 'id8'
                }
            ],
            'ref_to_c': [
                {
                    'begin_geldigheid': 'begingeldigheid',
                    'eind_geldigheid': None,
                    'id': 'id1',
                    'tid': 'id1',
                    'volgnummer': 1
                },
                {
                    'begin_geldigheid': 'begingeldigheid',
                    'eind_geldigheid': 'eindgeldigheid',
                    'id': 'id2',
                    'tid': 'id2',
                    'volgnummer': 1
                }
            ],
            'ref_to_d': [
                {
                    'begin_geldigheid': 'begingeldigheid',
                    'eind_geldigheid': None,
                    'id': 'id5',
                    'tid': 'id5'
                },
                {
                    'begin_geldigheid': 'begingeldigheid',
                    'eind_geldigheid': 'eindgeldigheid',
                    'id': 'id6',
                    'tid': 'id6'
                }
            ]
        }

        self.assertEqual(expected, edb.build_event('the tid'))


class TestKafkaEventProducerInitConnections(TestCase):
    """Tests constructor and init_connections. init_connections is fully mocked in other test class below.
    """

    @patch("gobkafkaproducer.producer.EventDataBuilder")
    @patch("gobkafkaproducer.producer.KafkaEventProducer._init_connections")
    def test_init(self, mock_init_connection, mock_databuilder):
        prod = KafkaEventProducer('CAT', 'COLL', 'LOGGER')

        self.assertEqual('CAT', prod.catalogue)
        self.assertEqual('COLL', prod.collection)
        self.assertEqual('LOGGER', prod.logger)
        mock_init_connection.assert_called_once()
        mock_databuilder.assert_called_with(None, None, 'CAT', 'COLL')

    @patch("gobkafkaproducer.producer.EventDataBuilder", MagicMock())
    @patch("gobkafkaproducer.producer.KafkaEventProducer._init_local_db_session")
    @patch("gobkafkaproducer.producer.KafkaEventProducer._init_gob_db_session")
    @patch("gobkafkaproducer.producer.KafkaEventProducer._init_kafka")
    def test_init_connections(self, mock_init_kafka, mock_init_gob_db, mock_init_local_db):
        KafkaEventProducer('', '', '')
        mock_init_local_db.assert_called_once()
        mock_init_gob_db.assert_called_once()
        mock_init_kafka.assert_called_once()


class MockEventBuilder:
    def __init__(self, *args):
        pass

    def build_event(self, tid):
        return {'tid': tid, 'a': 'A', 'b': 'B'}


@patch("gobkafkaproducer.producer.EventDataBuilder", MockEventBuilder)
@patch("gobkafkaproducer.producer.KafkaEventProducer._init_connections", MagicMock())
class TestKafkaEventProducerInit(TestCase):

    @patch("gobkafkaproducer.producer.get_relations_for_collection")
    @patch("gobkafkaproducer.producer.gob_model", spec_set=True)
    def test_get_tables_to_reflect(self, mock_model, mock_get_relations):
        mock_get_relations.return_value = {'rel a': 'a', 'rel b': 'b'}
        p = KafkaEventProducer('CAT', 'COL', MagicMock())
        mock_model.get_table_name = lambda x, y: f"{x}_{y}".lower()

        expected = [
            'events',
            'cat_col',
            'rel_a',
            'rel_b',
        ]
        self.assertEqual(expected, p._get_tables_to_reflect())
        mock_get_relations.assert_called_with(mock_model, 'CAT', 'COL')

    @patch("gobkafkaproducer.producer.MetaData")
    @patch("gobkafkaproducer.producer.create_engine")
    @patch("gobkafkaproducer.producer.Session")
    @patch("gobkafkaproducer.producer.URL")
    @patch("gobkafkaproducer.producer.automap_base")
    @patch("gobkafkaproducer.producer.GOB_DATABASE_CONFIG", {'db': 'config'})
    def test_init_gob_db_session(self, mock_automap, mock_url, mock_session, mock_create_engine, mock_metadata):
        p = KafkaEventProducer('', '', MagicMock())
        p._get_tables_to_reflect = MagicMock(return_value=['events', 'some_table'])
        p._init_gob_db_session()

        # Check initialisation of session
        self.assertEqual(mock_session.return_value, p.gob_db_session)
        mock_session.assert_called_with(mock_create_engine.return_value)
        mock_create_engine.assert_called_with(mock_url.return_value, connect_args={'sslmode': 'require'})
        mock_url.assert_called_with(db='config')

        # Check that Event obj is mapped and set correctly
        self.assertEqual(mock_automap.return_value.classes.events, p.Event)
        mock_automap.assert_has_calls([
            call(metadata=mock_metadata.return_value),
            call().prepare(),
        ])
        mock_metadata.assert_has_calls([
            call(),
            call().reflect(mock_create_engine.return_value, only=['events', 'some_table'])
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
        mock_create_engine.assert_called_with(mock_url.return_value, connect_args={'sslmode': 'require'})
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

        p = KafkaEventProducer('cat', 'coll', MagicMock())
        p.producer = MagicMock()

        p._add_event(event)
        p.producer.send.assert_called_with(
            'KAFKA_TOPIC',
            key=b'TID',
            value=b'{"tid": "TID", "a": "A", "b": "B"}',
            headers=[
                ('event_type', b'ACTION'),
                ('event_id', b'ID'),
                ('tid', b'TID'),
                ('catalog', b'CAT'),
                ('collection', b'COLL'),
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

    def test_produce(self):
        class MockEvent:
            def __init__(self, id):
                self.eventid = id

            def __eq__(self, other):
                return self.eventid == other.eventid

        event_cnt = 5

        p = KafkaEventProducer('', '', MagicMock())
        p._get_last_eventid = MagicMock(return_value=240)
        p._get_events = MagicMock(return_value=[MockEvent(i) for i in range(event_cnt)])
        p._add_event = MagicMock()
        p._flush = MagicMock()
        p.gob_db_session = MagicMock()
        p.FLUSH_PER = 2

        p.produce()

        p._get_events.assert_called_with(240)
        self.assertEqual(event_cnt, p._add_event.call_count)
        p._flush.assert_has_calls([
            call(1),
            call(3),
            call(4),
        ])
        p.gob_db_session.expunge.assert_has_calls([
            call(MockEvent(0)),
            call(MockEvent(1)),
            call(MockEvent(2)),
            call(MockEvent(3)),
            call(MockEvent(4)),
        ])

        # Test last flush 0
        p.FLUSH_PER = event_cnt
        p._flush.reset_mock()
        p.produce()
        p._flush.assert_has_calls([
            call(4),
            call(4),  # Last flush happens with the same argument because nothing was added between flushes. That's ok.
        ])
