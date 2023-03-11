from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobeventproducer.eventbuilder import EventDataBuilder


class TestEventDataBuilder(TestCase):
    mock_gobmodel_data = {"cat": {"collections": {"coll": {"attributes": {}}}}}

    @patch("gobeventproducer.eventbuilder.split_relation_table_name")
    @patch("gobeventproducer.eventbuilder.get_relations_for_collection")
    @patch("gobeventproducer.eventbuilder.gob_model", spec_set=True)
    def test_init(self, mock_model, mock_get_relations, mock_split_relation_table_name):
        session = MagicMock()
        base = MagicMock()
        mock_model.__getitem__.return_value = self.mock_gobmodel_data["cat"]
        mock_model.get_table_name = lambda x, y: f"{x}_{y}"
        mock_get_relations.return_value = {
            "rel_a_attr": "a",
            "rel_b_attr": "b",
        }
        mock_split_relation_table_name.side_effect = lambda x: {"dst_cat_abbr": "dstcat", "dst_col_abbr": x}
        mock_model.get_reference_by_abbreviations = lambda x, y: y
        mock_model.get_table_name_from_ref = lambda y: f"dst_table_{y}"
        edb = EventDataBuilder(session, base, "cat", "coll")

        self.assertEqual(session, edb.db_session)
        self.assertEqual(base, edb.base)
        self.assertEqual(self.mock_gobmodel_data["cat"]["collections"]["coll"], edb.collection)
        self.assertEqual("cat_coll", edb.tablename)

        expected_relations = {
            "rel_a_attr": {"dst_table_name": "dst_table_rel_a", "relation_table_name": "rel_a"},
            "rel_b_attr": {"dst_table_name": "dst_table_rel_b", "relation_table_name": "rel_b"},
        }
        self.assertEqual(expected_relations, edb.relations)

    def test_build_event(self):
        class RelationObject:
            # Create mock relation for DbObject
            def __init__(self, id, begin_geldigheid, eind_geldigheid, dst_name, dst_has_states):
                self.begin_geldigheid = begin_geldigheid
                self.eind_geldigheid = eind_geldigheid

                dst_fields = {"_id": id, "_tid": id}
                if dst_has_states:
                    dst_fields["volgnummer"] = 1

                self.__setattr__(dst_name, type("DstTable", (), dst_fields))

        class DbObject:
            id = 42
            identificatie = "identificatie"
            ref_to_c = None
            manyref_to_c = None
            ref_to_d = None
            manyref_to_d = None
            rel_tst_rta_tst_rtc_ref_to_c_collection = [
                RelationObject("id1", "begingeldigheid", None, "test_catalogue_rel_test_entity_c", True),
                RelationObject("id2", "begingeldigheid", "eindgeldigheid", "test_catalogue_rel_test_entity_c", True),
            ]
            rel_tst_rta_tst_rtc_manyref_to_c_collection = [
                RelationObject("id3", "begingeldigheid", None, "test_catalogue_rel_test_entity_c", True),
                RelationObject("id4", "begingeldigheid", "eindgeldigheid", "test_catalogue_rel_test_entity_c", True),
            ]
            rel_tst_rta_tst_rtd_ref_to_d_collection = [
                RelationObject("id5", "begingeldigheid", None, "test_catalogue_rel_test_entity_d", False),
                RelationObject("id6", "begingeldigheid", "eindgeldigheid", "test_catalogue_rel_test_entity_d", False),
            ]
            rel_tst_rta_tst_rtd_manyref_to_d_collection = [
                RelationObject("id7", "begingeldigheid", None, "test_catalogue_rel_test_entity_d", False),
                RelationObject("id8", "begingeldigheid", "eindgeldigheid", "test_catalogue_rel_test_entity_d", False),
            ]

        edb = EventDataBuilder(MagicMock(), MagicMock(), "test_catalogue", "rel_test_entity_a")

        edb.db_session.query.return_value.filter.return_value.one.return_value = DbObject()

        expected = {
            "id": "42",
            "identificatie": "identificatie",
            "manyref_to_c": [
                {
                    "begin_geldigheid": "begingeldigheid",
                    "eind_geldigheid": None,
                    "id": "id3",
                    "tid": "id3",
                    "volgnummer": 1,
                },
                {
                    "begin_geldigheid": "begingeldigheid",
                    "eind_geldigheid": "eindgeldigheid",
                    "id": "id4",
                    "tid": "id4",
                    "volgnummer": 1,
                },
            ],
            "manyref_to_d": [
                {"begin_geldigheid": "begingeldigheid", "eind_geldigheid": None, "id": "id7", "tid": "id7"},
                {"begin_geldigheid": "begingeldigheid", "eind_geldigheid": "eindgeldigheid", "id": "id8", "tid": "id8"},
            ],
            "ref_to_c": [
                {
                    "begin_geldigheid": "begingeldigheid",
                    "eind_geldigheid": None,
                    "id": "id1",
                    "tid": "id1",
                    "volgnummer": 1,
                },
                {
                    "begin_geldigheid": "begingeldigheid",
                    "eind_geldigheid": "eindgeldigheid",
                    "id": "id2",
                    "tid": "id2",
                    "volgnummer": 1,
                },
            ],
            "ref_to_d": [
                {"begin_geldigheid": "begingeldigheid", "eind_geldigheid": None, "id": "id5", "tid": "id5"},
                {"begin_geldigheid": "begingeldigheid", "eind_geldigheid": "eindgeldigheid", "id": "id6", "tid": "id6"},
            ],
        }

        self.assertEqual(expected, edb.build_event("the tid"))
