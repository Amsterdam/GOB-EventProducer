from unittest import TestCase
from unittest.mock import patch

from gobeventproducer.eventbuilder import EventDataBuilder


class TestEventDataBuilder(TestCase):
    mock_gobmodel_data = {"cat": {"collections": {"coll": {"attributes": {}}}}}

    @patch("gobeventproducer.eventbuilder.gob_model", spec_set=True)
    def test_init(self, mock_model):
        mock_model.__getitem__.return_value = self.mock_gobmodel_data["cat"]
        edb = EventDataBuilder("cat", "coll")
        self.assertEqual(self.mock_gobmodel_data["cat"]["collections"]["coll"], edb.collection)

    def test_build_event(self):
        class DbObject:
            id = 42
            identificatie = "identificatie"
            ref_to_c = None
            manyref_to_c = None
            ref_to_d = None
            manyref_to_d = None
            _gobid = 45

        edb = EventDataBuilder("test_catalogue", "rel_test_entity_a")

        expected = {
            "id": 42,
            "_gobid": 45,
            "identificatie": "identificatie",
        }

        self.assertEqual(expected, edb.build_event(DbObject()))
