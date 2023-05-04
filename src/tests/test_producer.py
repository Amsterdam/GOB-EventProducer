from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from gobeventproducer.eventbuilder import EventDataBuilder
from gobeventproducer.mapper import PassThroughEventDataMapper, RelationEventDataMapper
from gobeventproducer.producer import EventProducer


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
        self.assertEqual("nap.rel.peilmerken_ligtInBouwblok", p.routing_key)

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

        mock_events = [
            MockEvent(24, "ADD", 200),
            MockEvent(25, "MODIFY", 201)
        ]
        gobdb_instance.get_events = MagicMock(return_value=mock_events)
        gobdb_instance.get_object = MagicMock(side_effect=lambda tid: type('DbObject', (), {
            "some": "data",
            "int": 8042,
            "_gobid": tid,  # Of course not the same thing, but for the purpose of testing.
        }))

        p = EventProducer("cat", "coll", MagicMock())
        p.gob_db_session = MagicMock()
        localdb_instance.get_last_eventid = MagicMock(return_value=100)

        p.produce(100, 200)

        gobdb_instance.get_events.assert_called_with(100, 200)

        rabbit_instance.publish.assert_has_calls([
            call("gob.events", "cat.coll", {
                "header": {
                    "catalog": "cat",
                    "collection": "coll",
                    "event_type": "ADD",
                    "event_id": 24,
                    "tid": 200,
                },
                "data": {
                    "_gobid": 200,
                    "int": 8042,
                    "some": "data",
                }
            }),
            call("gob.events", "cat.coll", {
                "header": {
                    "catalog": "cat",
                    "collection": "coll",
                    "event_type": "MODIFY",
                    "event_id": 25,
                    "tid": 201,
                },
                "data": {
                    "_gobid": 201,
                    "int": 8042,
                    "some": "data",
                }
            }),
        ])

        gobdb_instance.get_object.assert_has_calls([call(event.tid) for event in mock_events])
        gobdb_instance.session.expunge.assert_has_calls([call(event) for event in mock_events])
        localdb_instance.set_last_eventid.assert_has_calls([call(24), call(25)])
        p.logger.warning.assert_not_called()

        # min_eventid does not match start_event
        p.produce(110, 200)
        p.logger.warning.assert_called_with(
            "Min eventid (110) to produce does not match last_eventid (100) in database. Recovering."
        )

        # Test with no last event in database
        localdb_instance.get_last_eventid.return_value = -1

        p.produce(110, 200)
        p.logger.warning.assert_called_with("Have no previous produced events in database. Starting at beginning")

    @patch("gobeventproducer.producer.EventDataBuilder", MockEventDatabuilder)
    @patch("gobeventproducer.producer.FULL_LOAD_BATCH_SIZE", 2)
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
                    "event_id": None,
                    "tid": "19",
                    "full_load_sequence": True,
                    "first_of_sequence": True,
                    "last_of_sequence": False,
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
                    "event_id": None,
                    "tid": "22",
                    "full_load_sequence": True,
                    "first_of_sequence": False,
                    "last_of_sequence": False,
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
                    "event_id": None,
                    "tid": "24",
                    "full_load_sequence": True,
                    "first_of_sequence": False,
                    "last_of_sequence": True,
                },
                "data": {
                    "_gobid": 24,
                    "int": 8042,
                    "some": "data",
                }
            }]),
        ])

        localdb_instance.set_last_eventid.assert_called_once_with(24)
