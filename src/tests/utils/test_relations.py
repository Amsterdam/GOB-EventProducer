from unittest import TestCase
from unittest.mock import patch

from gobeventproducer.utils.relations import RelationInfo, RelationInfoBuilder, gob_model


class TestRelationInfoBuilder(TestCase):

    @patch("gobeventproducer.utils.relations.get_relations_for_collection")
    def test_build(self, mock_get_relations):
        mock_get_relations.return_value = {
            "is_bron_voor_brk_tenaamstelling": "brk2_sdl_brk2_tng_is_bron_voor_brk_tenaamstelling",
            "is_bron_voor_brk_aantekening_kadastraal_object": "brk2_sdl_brk2_akt__is_bron_voor_brk_akt_",
            "is_bron_voor_brk_aantekening_recht": "brk2_sdl_brk2_art__is_bron_voor_brk_art_",
            "is_bron_voor_brk_erfpachtcanon": None,
        }

        res = RelationInfoBuilder().build("brk2", "stukdelen")

        self.assertEqual(res, {
            "is_bron_voor_brk_aantekening_kadastraal_object": RelationInfo("rel_brk2_sdl_brk2_akt__is_bron_voor_brk_akt_", "brk2_aantekeningenkadastraleobjecten"),
            "is_bron_voor_brk_aantekening_recht": RelationInfo("rel_brk2_sdl_brk2_art__is_bron_voor_brk_art_", "brk2_aantekeningenrechten"),
            "is_bron_voor_brk_tenaamstelling": RelationInfo("rel_brk2_sdl_brk2_tng_is_bron_voor_brk_tenaamstelling", "brk2_tenaamstellingen"),
        })

        mock_get_relations.assert_called_with(gob_model, "brk2", "stukdelen")
