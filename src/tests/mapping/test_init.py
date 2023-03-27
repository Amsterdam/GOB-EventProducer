from unittest import TestCase

from gobeventproducer.mapping import MappedObjectDefinition, MappingDefinitionLoader


class TestMappingDefinitions(TestCase):
    def test_get_definition(self):
        mapping_definitions = MappingDefinitionLoader()

        mapdef = mapping_definitions.get("nap", "peilmerken")
        self.assertEqual("nap", mapdef.catalog)
        self.assertEqual("peilmerken", mapdef.collection)
        self.assertEqual("v1.0.1", mapdef.version)
        self.assertEqual(
            {
                "geometrie": "geometrie",
                "hoogte_tov_nap": "hoogte_tov_nap",
                "identificatie": "identificatie",
                "jaar": "jaar",
                "ligt_in_bouwblok": "ligt_in_gebieden_bouwblok",
                "merk": MappedObjectDefinition(
                    type="object", mapping={"code": "merk_code", "omschrijving": "merk_omschrijving"}
                ),
                "omschrijving": "omschrijving",
                "publiceerbaar": "publiceerbaar",
                "rws_nummer": "rws_nummer",
                "status": MappedObjectDefinition(
                    type="object", mapping={"code": "status_code", "omschrijving": "status_omschrijving"}
                ),
                "vervaldatum": "vervaldatum",
                "windrichting": "windrichting",
                "x_coordinaat_muurvlak": "x_coordinaat_muurvlak",
                "y_coordinaat_muurvlak": "y_coordinaat_muurvlak",
            },
            mapdef.mapping,
        )

    def test_non_existent_definition(self):
        mapping_definitions = MappingDefinitionLoader()
        self.assertIsNone(mapping_definitions.get("non", "existent"))
