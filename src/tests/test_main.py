from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from gobeventproducer.__main__ import event_produce_handler, new_events_notification_handler


class TestMain(TestCase):
    @patch("gobeventproducer.__main__.LISTEN_TO_CATALOGS", ["nap"])
    @patch("gobeventproducer.__main__.get_notification")
    @patch("gobeventproducer.__main__.start_workflow")
    def test_new_events_notification_handler(self, mock_start_workflow, mock_get_notification):
        mock_get_notification.return_value = MagicMock()
        mock_get_notification.return_value.header = {
            "catalogue": "nap",
            "collection": "peilmerken",
            "application": "APPL",
            "process_id": "PID",
        }
        msg = MagicMock()

        new_events_notification_handler(msg)
        mock_get_notification.assert_called_with(msg)
        mock_start_workflow.assert_called_with(
            {
                "workflow_name": "event_produce",
            },
            {
                "catalogue": "nap",
                "collection": "peilmerken",
                "application": None,
                "process_id": "PID",
                "contents": mock_get_notification.return_value.contents,
            },
        )

        # Ignore other catalogs
        mock_start_workflow.reset_mock()
        mock_get_notification.return_value.header = {
            "catalogue": "gebieden",
            "collection": "bouwblokken",
            "application": "APPL",
            "process_id": "PID",
        }
        new_events_notification_handler(msg)
        mock_start_workflow.assert_not_called()

        # Should start workflow for relations from NAP
        mock_start_workflow.reset_mock()
        mock_get_notification.return_value.header = {
            "catalogue": "rel",
            "collection": "nap_pmk_gbd_bbk_ligt_in_gebieden_bouwblok",
            "application": "APPL",
            "process_id": "PID",
        }
        new_events_notification_handler(msg)
        mock_start_workflow.assert_called_once()

        # Should NOT start workflow for relations from gebieden
        mock_start_workflow.reset_mock()
        mock_get_notification.return_value.header = {
            "catalogue": "rel",
            "collection": "gbd_brt_brk_gme_ligt_in_brk_gemeente",
            "application": "APPL",
            "process_id": "PID",
        }
        new_events_notification_handler(msg)
        mock_start_workflow.assert_not_called()
        mock_start_workflow.reset_mock()


    @patch("gobeventproducer.__main__.logger")
    @patch("gobeventproducer.__main__.EventProducer")
    def test_event_produce_handler(self, mock_producer, mock_logger):
        msg = {
            "header": {
                "catalogue": "CAT",
                "collection": "COLL",
            },
            "contents": {"last_event": [100, 204]},
        }
        mock_producer.return_value.produce.return_value = 14804

        result = event_produce_handler(msg)
        self.assertEqual(
            {
                "header": msg["header"],
                "summary": {
                    "produced": 14804,
                },
            },
            result,
        )

        mock_producer.assert_has_calls(
            [
                call("CAT", "COLL", mock_logger),
                call().produce(100, 204),
            ]
        )

        with self.assertRaises(AssertionError):
            event_produce_handler({})

    @patch("gobeventproducer.__main__.logger")
    @patch("gobeventproducer.__main__.EventProducer")
    def test_event_produce_handler_full_load(self, mock_producer, mock_logger):
        msg = {
            "header": {
                "catalogue": "CAT",
                "collection": "COLL",
                "mode": "full",
            },
        }
        mock_producer.return_value.produce_initial.return_value = 14804

        result = event_produce_handler(msg)
        self.assertEqual(
            {
                "header": msg["header"],
                "summary": {
                    "produced": 14804,
                },
            },
            result,
        )

        mock_producer.assert_has_calls(
            [
                call("CAT", "COLL", mock_logger),
                call().produce_initial()
            ]
        )

        with self.assertRaises(AssertionError):
            event_produce_handler({})

    @patch("gobeventproducer.__main__.connect")
    @patch("gobeventproducer.__main__.MessagedrivenService")
    def test_main_entry(self, mock_messagedriven_service, mock_connect):
        from gobeventproducer import __main__ as module

        with patch.object(module, "__name__", "__main__"):
            module.init()

            mock_connect.assert_called_once()
            mock_messagedriven_service().start.assert_called_once()
