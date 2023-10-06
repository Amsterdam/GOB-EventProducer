from unittest import TestCase, mock
from gobeventproducer.splitjob import trigger_event_produce_for_all_collections, _start_workflow, WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY


class TestSplitjob(TestCase):

    @mock.patch("gobeventproducer.splitjob._start_workflow")
    def test_trigger_event_produce_for_all_collections(self, mock_start_workflow):
        expected_brk2_collections = [
            "aardzakelijkerechten",
            "aantekeningenkadastraleobjecten",
            "aantekeningenrechten",
            "kadastralegemeentecodes",
            "kadastralegemeentes",
            "erfpachtcanons",
            "kadastraleobjecten",
            "kadastralesecties",
            "kadastralesubjecten",
            "meta",
            "stukdelen",
            "tenaamstellingen",
            "zakelijkerechten"
        ]

        expected_rel_collections = [
            "brk2_kot_brk2_gme__angdd_dr__brk_gemeente",
            "brk2_kot_brk2_kge__angdd_dr__brk_kge_",
            "brk2_kot_brk2_kce__angdd_dr__brk_kce_",
            "brk2_kot_brk2_kse__angdd_dr__brk_kadastralesectie",
            "brk2_kot_brk2_kot_is_ontstaan_uit_brk_g_perceel",
            "brk2_kot_bag_vot__hft_rel_mt__bag_verblijfsobject",
            "brk2_kot_brk2_kot__is_ontstaan_uit_brk_kot_",
            "brk2_zrt_brk2_zrt_belast_brk_zakelijke_rechten",
            "brk2_zrt_brk2_zrt_belast_met_brk_zakelijke_rechten",
            "brk2_zrt_brk2_zrt__ontstaan_uit_brk_zrt_",
            "brk2_zrt_brk2_zrt__betrokken_bij_brk_zrt_",
            "brk2_zrt_brk2_tng__is_beperkt_tot_brk_tng_",
            "brk2_zrt_brk2_kot_rust_op_brk_kadastraal_object",
            "brk2_zrt_brk2_sjt_vve_identificatie_ontstaan_uit",
            "brk2_zrt_brk2_sjt_vve_identificatie_betrokken_bij",
            "brk2_tng_brk2_sjt_van_brk_kadastraalsubject",
            "brk2_tng_brk2_sjt_betrokken_partner_brk_subject",
            "brk2_tng_brk2_sjt__betr_samenwerkverband_brk_sjt_",
            "brk2_tng_brk2_sjt__betr_gorzen_aanwassen_brk_sjt_",
            "brk2_tng_brk2_zrt_van_brk_zakelijk_recht",
            "brk2_tng_brk2_sdl_is_gebaseerd_op_brk_stukdelen",
            "brk2_sdl_brk2_tng_is_bron_voor_brk_tenaamstelling",
            "brk2_sdl_brk2_akt__is_bron_voor_brk_akt_",
            "brk2_sdl_brk2_art__is_bron_voor_brk_art_",
            "brk2_sdl_brk2_ecs_is_bron_voor_brk_erfpachtcanon",
            "brk2_sdl_brk2_zrt_is_bron_voor_brk_zakelijk_recht",
            "brk2_art_brk2_tng_betrokken_brk_tenaamstelling",
            "brk2_art_brk2_sjt_heeft_brk_betrokken_persoon",
            "brk2_art_brk2_sdl_is_gebaseerd_op_brk_stukdeel",
            "brk2_akt_brk2_sjt_heeft_brk_betrokken_persoon",
            "brk2_akt_brk2_kot__hft_btrk_op_brk_kot_",
            "brk2_akt_brk2_sdl_is_gebaseerd_op_brk_stukdeel",
            "brk2_kge_brk2_gme_ligt_in_brk_gemeente",
            "brk2_kce_brk2_kge_onderdeel_van_brk_kge_",
            "brk2_kse_brk2_kce_onderdeel_van_brk_kge_code",
            "brk2_ecs_brk2_sdl_is_gebaseerd_op_brk_stukdeel",
            "brk2_ecs_brk2_zrt_betreft_brk_zakelijkrecht"
        ]
        msg = {
            "header": {
                "jobid": 42,
                "catalogue": "brk2",
            }
        }

        res = trigger_event_produce_for_all_collections(msg, "brk2")

        expected_calls = [("brk2", c) for c in expected_brk2_collections] + [("rel", c) for c in expected_rel_collections]
        mock_start_workflow.assert_has_calls([mock.call(msg, cat, col) for cat, col in expected_calls], any_order=True)

    @mock.patch("gobeventproducer.splitjob.MessageBrokerConnection")
    def test_start_workflow(self, mock_connection):
        msg = {
            "header": {
                "jobid": 42,
                "stepid": 3,
                "catalogue": "brk2",
            }
        }

        _start_workflow(msg, "some cat", "some coll")

        mock_connection.return_value.__enter__.return_value.publish.assert_called_once_with(
            WORKFLOW_EXCHANGE,
            WORKFLOW_REQUEST_KEY,
            {
                "header": {
                    "catalogue": "some cat",
                    "collection": "some coll",
                    "split_from": 42,
                },
                "workflow": {
                    "workflow_name": "event_produce",
                }
            }
        )
