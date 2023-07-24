from unittest import TestCase
from unittest.mock import patch

from gobeventproducer.utils.relations import RelationInfo, RelationInfoBuilder, gob_model


class TestRelationInfoBuilder(TestCase):

    @patch("gobeventproducer.utils.relations.get_relations_for_collection")
    def test_build_undefined_relation(self, mock_get_relations):
        mock_get_relations.return_value = {
            "is_bron_voor_brk_tenaamstelling": "brk2_sdl_brk2_tng_is_bron_voor_brk_tenaamstelling",
            "is_bron_voor_brk_aantekening_kadastraal_object": "brk2_sdl_brk2_akt__is_bron_voor_brk_akt_",
            "is_bron_voor_brk_aantekening_recht": "brk2_sdl_brk2_art__is_bron_voor_brk_art_",
            "is_bron_voor_brk_erfpachtcanon": None,
        }

        res = RelationInfoBuilder().build("brk2", "stukdelen")

        self.assertEqual(res, {
            "is_bron_voor_brk_aantekening_kadastraal_object": RelationInfo("rel_brk2_sdl_brk2_akt__is_bron_voor_brk_akt_", "brk2_aantekeningenkadastraleobjecten", True),
            "is_bron_voor_brk_aantekening_recht": RelationInfo("rel_brk2_sdl_brk2_art__is_bron_voor_brk_art_", "brk2_aantekeningenrechten", True),
            "is_bron_voor_brk_tenaamstelling": RelationInfo("rel_brk2_sdl_brk2_tng_is_bron_voor_brk_tenaamstelling", "brk2_tenaamstellingen", True),
        })

        mock_get_relations.assert_called_with(gob_model, "brk2", "stukdelen")

    @patch("gobeventproducer.utils.relations.get_relations_for_collection")
    def test_build_many_singlerefs(self, mock_get_relations):
        # Mock get_relations to limit the number of relations to test and to avoid failing tests when the model changes
        mock_get_relations.return_value = {
            "betrokken_gorzen_en_aanwassen_brk_subject": "brk2_tng_brk2_sjt__betr_gorzen_aanwassen_brk_sjt_",
            "betrokken_samenwerkingsverband_brk_subject": "brk2_tng_brk2_sjt__betr_samenwerkverband_brk_sjt_",
            "is_gebaseerd_op_brk_stukdelen": "brk2_tng_brk2_sdl_is_gebaseerd_op_brk_stukdelen",
        }

        res = RelationInfoBuilder().build("brk2", "tenaamstellingen")

        self.assertEqual(res, {
            "betrokken_gorzen_en_aanwassen_brk_subject": RelationInfo("rel_brk2_tng_brk2_sjt__betr_gorzen_aanwassen_brk_sjt_", "brk2_kadastralesubjecten", False),
            "betrokken_samenwerkingsverband_brk_subject": RelationInfo("rel_brk2_tng_brk2_sjt__betr_samenwerkverband_brk_sjt_", "brk2_kadastralesubjecten", False),
            "is_gebaseerd_op_brk_stukdelen": RelationInfo("rel_brk2_tng_brk2_sdl_is_gebaseerd_op_brk_stukdelen", "brk2_stukdelen", True),
        })

        mock_get_relations.assert_called_with(gob_model, "brk2", "tenaamstellingen")
