from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobeventproducer.producer import _to_bytes, EventProducer, LastSentEvent, EventDataBuilder, LocalDatabaseConnection

class AsyncConnectionMock:
    def __init__(self, connection_params: dict):
        self.connection_params = connection_params
        self.published = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def publish(self, exchange: str, routing_key: str, msg: dict):
        self.published.append((exchange, routing_key, msg))

    def assert_message_published(self, exchange: str, routing_key: str, msg: dict):
        assert (exchange, routing_key, msg) in self.published

    def assert_publish_count(self, n: int):
        assert len(self.published) == n

    def assert_connected_with(self, connection_params: str):
        assert self.connection_params == connection_params


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

    @patch("gobeventproducer.producer.split_relation_table_name")
    @patch("gobeventproducer.producer.get_relations_for_collection")
    @patch("gobeventproducer.producer.gob_model", spec_set=True)
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


class TestEventProducerInitConnections(TestCase):
    """Tests constructor and init_connections. init_connections is fully mocked in other test class below.
    """

    @patch("gobeventproducer.producer.EventDataBuilder")
    @patch("gobeventproducer.producer.EventProducer._init_gob_db_session")
    def test_init(self, mock_init_connection, mock_databuilder):
        prod = EventProducer('CAT', 'COLL', 'LOGGER')

        self.assertEqual('CAT', prod.catalogue)
        self.assertEqual('COLL', prod.collection)
        self.assertEqual('LOGGER', prod.logger)
        mock_init_connection.assert_called_once()
        mock_databuilder.assert_called_with(None, None, 'CAT', 'COLL')

    @patch("gobeventproducer.producer.EventDataBuilder")
    @patch("gobeventproducer.producer.EventProducer._get_tables_to_reflect")
    @patch("gobeventproducer.producer.MetaData")
    @patch("gobeventproducer.producer.create_engine")
    @patch("gobeventproducer.producer.Session")
    @patch("gobeventproducer.producer.URL")
    @patch("gobeventproducer.producer.automap_base")
    @patch("gobeventproducer.producer.GOB_DATABASE_CONFIG", {'db': 'config'})
    def test_init_gob_db_session(self, mock_automap, mock_url, mock_session, mock_create_engine, mock_metadata, mock_get_tables, mock_databuilder):
        mock_get_tables.return_value = ['events', 'some_table']
        p = EventProducer('', '', MagicMock())

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


class MockEventBuilder:
    def __init__(self, *args):
        pass

    def build_event(self, tid):
        return {'tid': tid, 'a': 'A', 'b': 'B'}

class TestLocalDatabaseConnection(TestCase):
    @patch("gobeventproducer.producer.Base")
    @patch("gobeventproducer.producer.create_engine")
    @patch("gobeventproducer.producer.Session")
    @patch("gobeventproducer.producer.URL")
    @patch("gobeventproducer.producer.DATABASE_CONFIG", {'db': 'config'})
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

        with patch("gobeventproducer.producer.LastSentEvent") as mock_last_sent_event:
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


@patch("gobeventproducer.producer.EventDataBuilder", MockEventBuilder)
@patch("gobeventproducer.producer.EventProducer._init_gob_db_session", MagicMock())
class TestEventProducerInit(TestCase):

    @patch("gobeventproducer.producer.get_relations_for_collection")
    @patch("gobeventproducer.producer.gob_model", spec_set=True)
    def test_get_tables_to_reflect(self, mock_model, mock_get_relations):
        mock_get_relations.return_value = {'rel a': 'a', 'rel b': 'b'}
        p = EventProducer('CAT', 'COL', MagicMock())
        mock_model.get_table_name = lambda x, y: f"{x}_{y}".lower()

        expected = [
            'events',
            'cat_col',
            'rel_a',
            'rel_b',
        ]
        self.assertEqual(expected, p._get_tables_to_reflect())
        mock_get_relations.assert_called_with(mock_model, 'CAT', 'COL')


    @patch("gobeventproducer.producer.and_")
    def test_get_events(self, mock_and):
        class MockComp:
            asc = MagicMock()

            def __init__(self, name):
                self.name = name

            def __eq__(self, other):
                return f"{self.name} == {other}"

            def __gt__(self, other):
                return f"{self.name} > {other}"

            def __le__(self, other):
                return f"{self.name} <= {other}"

        p = EventProducer('cat', 'coll', MagicMock())
        p.Event = MagicMock()
        p.Event.catalogue = MockComp("catalogue")
        p.Event.entity = MockComp("entity")
        p.Event.eventid = MockComp("eventid")

        p.gob_db_session = MagicMock()

        res = p._get_events(184, 200)

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
            'eventid > 184',
            'eventid <= 200',
        )

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

        p._add_event(event, connection)
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
    @patch("gobeventproducer.producer.AsyncConnection", AsyncConnectionMock)
    def test_produce(self, mock_localdb):
        class MockEvent:
            def __init__(self, id):
                self.eventid = id

            def __eq__(self, other):
                return self.eventid == other.eventid

        localdb_instance = mock_localdb.return_value.__enter__.return_value
        event_cnt = 5

        p = EventProducer('', '', MagicMock())
        p._get_events = MagicMock(return_value=[MockEvent(i) for i in range(event_cnt)])
        p._add_event = MagicMock()
        p.gob_db_session = MagicMock()
        localdb_instance.get_last_eventid = MagicMock(return_value=100)

        p.produce(100, 200)

        p._get_events.assert_called_with(100, 200)
        self.assertEqual(event_cnt, p._add_event.call_count)
        p.gob_db_session.expunge.assert_has_calls([
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
