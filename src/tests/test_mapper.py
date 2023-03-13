from pathlib import Path
from unittest import TestCase

from gobeventproducer.mapper import EventDataMapper, PassThroughEventDataMapper
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
}


class TestPassThroughEventDataMapper(TestCase):
    def test_map(self):
        mapper = PassThroughEventDataMapper()
        self.assertEqual(eventdata, mapper.map(eventdata))


class TestEventDataMapper(TestCase):
    def test_map(self):
        mock_definition_path = Path(__file__).parent / "mocks" / "mock_mapping_definition.yml"
        mapping_definition = MappingDefinitionLoader()._load_mapping_definition(mock_definition_path)
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
        }

        mapper = EventDataMapper(mapping_definition)
        self.assertEqual(expected, mapper.map(eventdata))
