from pathlib import Path
from unittest import TestCase

from gobeventproducer.mapper import EventDataMapper, PassThroughEventDataMapper, RelationEventDataMapper
from gobeventproducer.mapping import MappingDefinitionLoader

eventdata = {
    "ligt_in_gebieden_bouwblok": {
        "tid": "thet id",
        "id": "the id",
        "begin_geldigheid": "2022-01-01 00:00:00",
        "eind_geldigheid": None,
    },
    "transactie_bedrag": 122,
    "transactie_datum": "2022-02-01",
    "merk_code": 28,
    "merk_omschrijving": "Dit heb ik ook maar verzonnen",
    "valuta": "EUR",
    "objectje": {
        "met": {
            "nested": "veld",
        }
    }
}


class TestPassThroughEventDataMapper(TestCase):
    def test_map(self):
        mapper = PassThroughEventDataMapper()
        self.assertEqual(eventdata, mapper.map(eventdata))

    def test_get_mapped_name_reverse(self):
        mapper = PassThroughEventDataMapper()
        self.assertEqual("some_str", mapper.get_mapped_name_reverse("some_str"))


class TestEventDataMapper(TestCase):

    def setUp(self) -> None:
        mock_definition_path = Path(__file__).parent / "mocks" / "mock_mapping_definition.yml"
        mapping_definition = MappingDefinitionLoader()._load_mapping_definition(mock_definition_path)
        self.mapper = EventDataMapper(mapping_definition)

    def test_map(self):
        expected = {
            "ligt_in_bouwblok": {
                "tid": "thet id",
                "id": "the id",
                "begin_geldigheid": "2022-01-01 00:00:00",
                "eind_geldigheid": None,
            },
            "bedrag": 122,
            "transactie_datum": "2022-02-01",
            "merk": {
                "code": 28,
                "omschrijving": "Dit heb ik ook maar verzonnen",
            },
            "vanuit_nested_veld": "veld",
        }

        self.assertEqual(expected, self.mapper.map(eventdata))

    def test_get_mapped_name_reverse(self):
        self.assertEqual("ligt_in_bouwblok", self.mapper.get_mapped_name_reverse("ligt_in_gebieden_bouwblok"))

        with self.assertRaisesRegex(Exception, "non_existent cannot be found"):
            self.mapper.get_mapped_name_reverse("non_existent")


class TestRelationEventDataMapper(TestCase):

    def test_map_without_states(self):
        mapper = RelationEventDataMapper()

        eventdata = {
            "_gobid": 45,
            "src_id": "248",
            "dst_id": "202",
            "begin_geldigheid": "2023-01-01 00:00:00",
            "eind_geldigheid": None,
            "dst_volgnummer": None,
            "other_field": "some value"
        }

        expected = {
            "id": 45,
            "src_id": "248",
            "dst_id": "202",
            "begin_geldigheid": "2023-01-01 00:00:00",
            "eind_geldigheid": None,
            "src_volgnummer": None,
            "dst_volgnummer": None,
        }

        self.assertEqual(expected, mapper.map(eventdata))
