from datetime import datetime
from unittest import TestCase
from unittest.mock import patch

from gobeventproducer.eventbuilder import EventDataBuilder


class TestEventDataBuilder(TestCase):
    mock_gobmodel_data = {
        'cat': {
            'collections': {
                'coll': {
                    'fields': {}
                }
            }
        }
    }

    @patch("gobeventproducer.eventbuilder.gob_model", spec_set=True)
    @patch("gobeventproducer.eventbuilder.RelationInfoBuilder")
    def test_init(self, mock_relation_info_builder, mock_model):
        mock_model.__getitem__.return_value = self.mock_gobmodel_data["cat"]
        edb = EventDataBuilder('cat', 'coll')

        self.assertEqual(self.mock_gobmodel_data['cat']['collections']['coll'], edb.collection)
        self.assertEqual(mock_relation_info_builder.build.return_value, edb.relations)
        mock_relation_info_builder.build.assert_called_with('cat', 'coll')

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
            _gobid = 43
            identificatie = 'identificatie'
            volgnummer = 3
            registratiedatum = '2020-01-01T00:00:00'
            begin_geldigheid = '2020-01-01T09:00:00'
            eind_geldigheid = None
            ref_to_c = None
            manyref_to_c = None
            ref_to_d = None
            manyref_to_d = None
            rel_tst_rta_tst_rtc_ref_to_c_collection = [
                RelationObject('id1', 'begingeldigheid', None, 'test_catalogue_rel_test_entity_c', True),
            ]
            rel_tst_rta_tst_rtc_manyref_to_c_collection = [
                RelationObject('id3', 'begingeldigheid', None, 'test_catalogue_rel_test_entity_c', True),
                RelationObject('id4', 'begingeldigheid', 'eindgeldigheid', 'test_catalogue_rel_test_entity_c', True),
            ]
            rel_tst_rta_tst_rtd_ref_to_d_collection = [
                RelationObject('id5', 'begingeldigheid', None, 'test_catalogue_rel_test_entity_d', False),
            ]
            rel_tst_rta_tst_rtd_manyref_to_d_collection = [
                RelationObject('id7', 'begingeldigheid', None, 'test_catalogue_rel_test_entity_d', False),
                RelationObject('id8', 'begingeldigheid', 'eindgeldigheid', 'test_catalogue_rel_test_entity_d', False),
            ]

        edb = EventDataBuilder('test_catalogue', 'rel_test_entity_a')

        expected = {
            'id': 42,
            '_gobid': 43,
            'identificatie': 'identificatie',
            'volgnummer': 3,
            'registratiedatum': datetime(2020, 1, 1, 0, 0),
            'begin_geldigheid': datetime(2020, 1, 1, 9, 0),
            'eind_geldigheid': None,
            'manyref_to_c': [],
            'manyref_to_d': [],
            'ref_to_c': {
                'begin_geldigheid': 'begingeldigheid',
                'eind_geldigheid': None,
                'id': 'id1',
                'tid': 'id1',
                'volgnummer': 1
            },
            'ref_to_d': {
                'begin_geldigheid': 'begingeldigheid',
                'eind_geldigheid': None,
                'id': 'id5',
                'tid': 'id5'
            },
        }

        dbobject = DbObject()

        self.assertEqual(expected, edb.build_event(dbobject))

        # Test empty relations
        dbobject.rel_tst_rta_tst_rtc_ref_to_c_collection = []
        dbobject.rel_tst_rta_tst_rtc_manyref_to_c_collection = []
        dbobject.rel_tst_rta_tst_rtd_ref_to_d_collection = []
        dbobject.rel_tst_rta_tst_rtd_manyref_to_d_collection = []

        expected = {
            'id': 42,
            '_gobid': 43,
            'identificatie': 'identificatie',
            'volgnummer': 3,
            'registratiedatum': datetime(2020, 1, 1, 0, 0),
            'begin_geldigheid': datetime(2020, 1, 1, 9, 0),
            'eind_geldigheid': None,
            'manyref_to_c': [],
            'manyref_to_d': [],
            'ref_to_c': {},
            'ref_to_d': {},
        }

        self.assertEqual(expected, edb.build_event(dbobject))

        # Test that a missing relation is added as empty relation
        del edb.relations['manyref_to_c']

        self.assertEqual(expected, edb.build_event(dbobject))

    @patch("gobeventproducer.eventbuilder.gob_model", spec_set=True)
    @patch("gobeventproducer.eventbuilder.RelationInfoBuilder")
    def test_build_event_shortname(self, mock_builder, mock_model):
        mock_model.__getitem__.return_value = self.mock_gobmodel_data["cat"]
        edb = EventDataBuilder('cat', 'coll')
        edb.collection["fields"] = {
            "some_long_attribute_name": {
                "type": "GOB.String",
                "description": "some description",
                "shortname": "shortened_attr"
            }
        }

        expected = {
            "shortened_attr": "the value",
            "_gobid": 42
        }
        class Object:
            some_long_attribute_name = "the value"
            _gobid = 42

        self.assertEqual(expected, edb.build_event(Object()))
