from freezegun import freeze_time
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from gobeventproducer.mapper import PassThroughEventDataMapper, RelationEventDataMapper
from gobeventproducer.producer import EventProducer, BatchEventsMessagePublisher


class MockEvent:
    def __init__(self, id, action, tid):
        self.eventid = id
        self.action = action
        self.tid = tid

    def __eq__(self, other):
        return self.eventid == other.eventid

class MockEventDatabuilder:

    def __init__(self, *args):
        pass

    def build_event(self, obj: object):
        fields = ["int", "some", "_gobid"]
        return {field: getattr(obj, field) for field in fields}

mock_model = {
    'cat': {
        'collections': {
            'coll': {
                'attributes': {
                    'some': {
                        'type': 'GOB.String',
                    },
                    'int': {
                        'type': 'GOB.Integer',
                    }
                }
            }
        }
    }
}

class TestBatchEventsMessagePublisher(TestCase):

    @patch("gobeventproducer.producer.MAX_EVENTS_PER_MESSAGE", 3)
    @patch("builtins.print")
    def test_add_event(self, mock_print):
        rabbitcon = MagicMock()
        localdb = MagicMock()
        events = [{
            "header": {
                "event_id": n,
            }
        } for n in range(7)]

        with BatchEventsMessagePublisher(rabbitcon, "some.routing.key", "LogName", localdb) as publisher:
            publisher.log_per = 2

            for event in events:
                publisher.add_event(event)

        mock_print.assert_has_calls([
            call("LogName: 2"),
            call("LogName: 4"),
            call("LogName: 6"),
            call("LogName: 7"),
        ])

        rabbitcon.publish.assert_has_calls([
            call("gob.events", "some.routing.key", events[:3]),
            call("gob.events", "some.routing.key", events[3:6]),
            call("gob.events", "some.routing.key", events[6:]),
        ])

        localdb.set_last_eventid.assert_has_calls([
            call(2),
            call(5),
            call(6),
        ])

@freeze_time("2023-06-27 00:00:00")
class TestEventProducer(TestCase):
    def test_init(self):
        p = EventProducer("nap", "peilmerken", MagicMock())
        self.assertEqual("nap", p.mapper.mapping_definition.catalog)
        self.assertEqual("peilmerken", p.mapper.mapping_definition.collection)

        self.assertEqual({
            "catalog": "nap",
            "collection": "peilmerken",
        }, p.header_data)
        self.assertEqual("nap.peilmerken", p.routing_key)

        p = EventProducer("some", "other", MagicMock())
        self.assertIsInstance(p.mapper, PassThroughEventDataMapper)

    def test_init_rel_catalog(self):
        p = EventProducer("rel", "nap_pmk_gbd_bbk_ligt_in_gebieden_bouwblok", MagicMock())
        self.assertIsInstance(p.mapper, RelationEventDataMapper)
        self.assertEqual({
            "catalog": "nap",
            "collection": "peilmerken_ligtInBouwblok",
        }, p.header_data)
        self.assertEqual("nap.peilmerken.rel.peilmerken_ligtInBouwblok", p.routing_key)

    def test_init_compressed_name(self):
        p = EventProducer("rel", "brk2_akt_brk2_kot__hft_btrk_op_brk_kot_", MagicMock())
        self.assertIsInstance(p.mapper, RelationEventDataMapper)
        self.assertEqual({
            "catalog": "brk2",
            "collection": "aantekeningenkadastraleobjecten_heeftBetrekkingOpBrkKadastraalObject",
        }, p.header_data)
        self.assertEqual("brk2.aantekeningenkadastraleobjecten.rel.aantekeningenkadastraleobjecten_heeftBetrekkingOpBrkKadastraalObject", p.routing_key)

    @patch("gobeventproducer.producer.gob_model", mock_model)
    @patch("gobeventproducer.eventbuilder.gob_model", mock_model)
    def test_build_event(self):
        """Mainly to test that mapper is called correctly, as the rest of this method is also tested in test_produce."""
        data = type('DbObject', (), {
            "some": "data",
            "int": 8042,
            "_gobid": 24,
        })

        p = EventProducer("cat", "coll", MagicMock())
        p.mapper = MagicMock()
        p.mapper.map.side_effect = lambda x: {**x, "transformed": True}

        result = p._build_event(
            "ACTION",
            "ID",
            "TID",
            data,
            MockEventDatabuilder()
        )
        self.assertEqual(result, {
            "header": {
                "event_type": "ACTION",
                "event_id": "ID",
                "tid": "TID",
                "catalog": "cat",
                "collection": "coll",
                "generated_timestamp": "2023-06-27T00:00:00",
            },
            "data": {
                "transformed": True,
                "_gobid": 24,
                "int": 8042,
                "some": "data",
            },
        })
        p.mapper.map.assert_called_with(
            {
                "_gobid": 24,
                "int": 8042,
                "some": "data",
            }
        )

    @patch("gobeventproducer.producer.EventDataBuilder", MockEventDatabuilder)
    @patch("gobeventproducer.producer.gob_model", mock_model)
    @patch("gobeventproducer.eventbuilder.gob_model", mock_model)
    @patch("gobeventproducer.producer.LocalDatabaseConnection")
    @patch("gobeventproducer.producer.GobDatabaseConnection")
    @patch("gobeventproducer.producer.AsyncConnection")
    def test_produce(self, mock_rabbit, mock_gobdb, mock_localdb):
        rabbit_instance = mock_rabbit.return_value.__enter__.return_value
        localdb_instance = mock_localdb.return_value.__enter__.return_value
        gobdb_instance = mock_gobdb.return_value.__enter__.return_value

        mock_events = iter([
            [MockEvent(101, "ADD", 200)],
            [MockEvent(105, "MODIFY", 201)],
            []
        ])
        gobdb_instance.get_events = MagicMock(side_effect=mock_events)
        gobdb_instance.get_object = MagicMock(side_effect=lambda tid: type('DbObject', (), {
            "some": "data",
            "int": 8042,
            "_gobid": tid,  # Of course not the same thing, but for the purpose of testing.
        }))

        p = EventProducer("cat", "coll", MagicMock())
        p.gob_db_session = MagicMock()
        localdb_instance.get_last_eventid = MagicMock(return_value=100)

        p.produce(100, 200)

        gobdb_instance.get_events.assert_has_calls([
            call(100, 200, 200),
            call(101, 200, 200),
            call(105, 200, 200),
        ])

        rabbit_instance.publish.assert_has_calls([
            call("gob.events", "cat.coll", [{
                "header": {
                    "catalog": "cat",
                    "collection": "coll",
                    "event_type": "ADD",
                    "event_id": 101,
                    "tid": 200,
                    "generated_timestamp": "2023-06-27T00:00:00",
                },
                "data": {
                    "_gobid": 200,
                    "int": 8042,
                    "some": "data",
                }
            }, {
                "header": {
                    "catalog": "cat",
                    "collection": "coll",
                    "event_type": "MODIFY",
                    "event_id": 105,
                    "tid": 201,
                    "generated_timestamp": "2023-06-27T00:00:00",
                },
                "data": {
                    "_gobid": 201,
                    "int": 8042,
                    "some": "data",
                }
            }]),
        ])

        gobdb_instance.get_object.assert_has_calls([call(event.tid) for event in mock_events])
        gobdb_instance.session.expunge.assert_has_calls([call(event) for event in mock_events])
        localdb_instance.set_last_eventid.assert_has_calls([call(105)])
        p.logger.warning.assert_not_called()

        # min_eventid does not match start_event
        gobdb_instance.get_events = MagicMock(side_effect=iter([[]]))
        p.produce(110, 200)
        p.logger.warning.assert_called_with(
            "Min eventid (110) to produce does not match last_eventid (100) in database. Recovering."
        )

        # Test with no last event in database
        localdb_instance.get_last_eventid.return_value = -1
        gobdb_instance.get_events = MagicMock(return_value=[])

        p.produce(110, 200)
        p.logger.warning.assert_called_with("Have no previous produced events in database. Starting from beginning")
        gobdb_instance.get_events.assert_called_with(-1, 200, 200)

        gobdb_instance.get_events = MagicMock(return_value=[])
        localdb_instance.get_last_eventid.return_value = 100

        # Test no min/max
        p.produce()
        p.logger.info.assert_any_call("No min_eventid specified. Starting from last_eventid (100)")
        gobdb_instance.get_events.assert_called_with(100, None, 200)

    @patch("gobeventproducer.producer.EventDataBuilder", MockEventDatabuilder)
    @patch("gobeventproducer.producer.MAX_EVENTS_PER_MESSAGE", 2)
    @patch("gobeventproducer.producer.gob_model", mock_model)
    @patch("gobeventproducer.eventbuilder.gob_model", mock_model)
    @patch("gobeventproducer.producer.LocalDatabaseConnection")
    @patch("gobeventproducer.producer.GobDatabaseConnection")
    @patch("gobeventproducer.producer.AsyncConnection")
    def test_produce_initial(self, mock_rabbit, mock_gobdb, mock_localdb):
        gobdb_instance = mock_gobdb.return_value.__enter__.return_value
        localdb_instance = mock_localdb.return_value.__enter__.return_value
        rabbit_instance = mock_rabbit.return_value.__enter__.return_value

        create_object = lambda tid: type('DbObject', (), {
            "some": "data",
            "int": 8042,
            "_last_event": int(tid),
            "_gobid": int(tid),  # Of course not the same thing, but for the purpose of testing.
            "_tid": tid,
        })

        gobdb_instance.get_objects.return_value = iter([
            create_object("19"),
            create_object("22"),
            create_object("24"),
        ])

        p = EventProducer("cat", "coll", MagicMock())
        p.produce_initial()

        rabbit_instance.publish.assert_has_calls([
            call("gob.events", "cat.coll", [{
                "header": {
                    "catalog": "cat",
                    "collection": "coll",
                    "event_type": "ADD",
                    "event_id": 19,
                    "tid": "19",
                    "full_load_sequence": True,
                    "first_of_sequence": True,
                    "last_of_sequence": False,
                    "generated_timestamp": "2023-06-27T00:00:00",
                },
                "data": {
                    "_gobid": 19,
                    "int": 8042,
                    "some": "data",
                }
            }, {
                "header": {
                    "catalog": "cat",
                    "collection": "coll",
                    "event_type": "ADD",
                    "event_id": 22,
                    "tid": "22",
                    "full_load_sequence": True,
                    "first_of_sequence": False,
                    "last_of_sequence": False,
                    "generated_timestamp": "2023-06-27T00:00:00",
                },
                "data": {
                    "_gobid": 22,
                    "int": 8042,
                    "some": "data",
                }
            }]),
            call("gob.events", "cat.coll", [{
                "header": {
                    "catalog": "cat",
                    "collection": "coll",
                    "event_type": "ADD",
                    "event_id": 24,
                    "tid": "24",
                    "full_load_sequence": True,
                    "first_of_sequence": False,
                    "last_of_sequence": True,
                    "generated_timestamp": "2023-06-27T00:00:00",
                },
                "data": {
                    "_gobid": 24,
                    "int": 8042,
                    "some": "data",
                }
            }]),
        ])

        localdb_instance.set_last_eventid.assert_has_calls([
            call(22),
            call(24),
        ])
