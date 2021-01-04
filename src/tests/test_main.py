from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobkafkaproducer.__main__ import new_events_notification_handler, kafka_produce_handler


class TestMain(TestCase):

    @patch("gobkafkaproducer.__main__.get_notification")
    @patch("gobkafkaproducer.__main__.start_workflow")
    def test_new_events_notification_handler(self, mock_start_workflow, mock_get_notification):
        mock_get_notification.return_value = MagicMock()
        mock_get_notification.return_value.header = {
            'catalogue': 'CAT',
            'collection': 'COLL',
            'application': 'APPL',
            'process_id': 'PID',
        }

        msg = MagicMock()

        with patch("gobkafkaproducer.__main__.LISTEN_TO_CATALOGS", ['CAT']):
            new_events_notification_handler(msg)
        mock_get_notification.assert_called_with(msg)

        mock_start_workflow.assert_called_with({
            'workflow_name': 'kafka_produce',
        }, {
            'catalogue': 'CAT',
            'collection': 'COLL',
            'application': 'APPL',
            'process_id': 'PID',
        })

        # Ignore other catalogs
        mock_start_workflow.reset_mock()
        new_events_notification_handler(msg)
        mock_start_workflow.assert_not_called()

    @patch("gobkafkaproducer.__main__.logger")
    @patch("gobkafkaproducer.__main__.KafkaEventProducer")
    def test_kafka_produce_handler(self, mock_producer, mock_logger):
        msg = {
            'header': {
                'catalogue': 'CAT',
                'collection': 'COLL',
            }
        }
        mock_producer.return_value.total_cnt = 14804

        result = kafka_produce_handler(msg)
        self.assertEqual({
            **msg,
            'summary': {
                'produced': 14804,
            }
        }, result)

        mock_producer.assert_has_calls([
            call('CAT', 'COLL', mock_logger),
            call().produce(),
        ])

        with self.assertRaises(AssertionError):
            kafka_produce_handler({})

    @patch("gobkafkaproducer.__main__.connect")
    @patch("gobkafkaproducer.__main__.MessagedrivenService")
    def test_main_entry(self, mock_messagedriven_service, mock_connect):
        from gobkafkaproducer import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()

            mock_connect.assert_called_once()
            mock_messagedriven_service().start.assert_called_once()
