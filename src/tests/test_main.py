from unittest import TestCase
from unittest.mock import patch, call

from gobkafkaproducer.__main__ import init_listen_queue, _to_bytes, new_events_handler


class TestMain(TestCase):

    @patch("gobkafkaproducer.__main__.MessagedrivenService")
    def test_main_entry(self, mock_messagedriven_service):
        from gobkafkaproducer import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service().start.assert_called_once()

    @patch("gobkafkaproducer.__main__.create_queue_with_binding")
    def test_init_listen_queue(self, mock_create_queue):
        init_listen_queue()
        mock_create_queue.assert_called_with('gob.event', 'gob.event.kafka', 'event.#')

    @patch("gobkafkaproducer.__main__.KAFKA_TOPIC", 'the-topic-to-publish-to')
    @patch("gobkafkaproducer.__main__.KAFKA_CONNECTION_CONFIG", {'config_key': 'config value'})
    @patch("gobkafkaproducer.__main__.KafkaProducer")
    def test_new_events_handler(self, mock_producer):
        msg = {
            'contents': [{
                'header': {
                    'name': 'ADD',
                    'event_id': 1,
                    'source_id': 248,
                    'last_event_id': None,
                    'catalog': 'the catalog',
                    'collection': 'the collection',
                    'source': 'the source',
                },
                'contents': {'event': 'contents'}
            }, {
                'header': {
                    'name': 'MODIFY',
                    'event_id': 2,
                    'source_id': 247,
                    'last_event_id': 48,
                    'catalog': 'the catalog',
                    'collection': 'the collection',
                    'source': 'the source',
                },
                'contents': {'event': 'contents'}
            }]
        }

        new_events_handler(msg)

        mock_producer.assert_called_with(config_key='config value')

        mock_producer.return_value.assert_has_calls([
            call.send(
                'the-topic-to-publish-to',
                key=b'the catalog.the collection',
                value=b'{"event": "contents"}',
                headers=[
                    ('event_type', b'ADD'),
                    ('event_id', b'1'),
                    ('source_id', b'248'),
                    ('last_event', b''),
                    ('catalog', b'the catalog'),
                    ('collection', b'the collection'),
                    ('source', b'the source'),
                ]
            ),
            call.send(
                'the-topic-to-publish-to',
                key=b'the catalog.the collection',
                value=b'{"event": "contents"}',
                headers=[
                    ('event_type', b'MODIFY'),
                    ('event_id', b'2'),
                    ('source_id', b'247'),
                    ('last_event', b'48'),
                    ('catalog', b'the catalog'),
                    ('collection', b'the collection'),
                    ('source', b'the source'),
                ]
            ),
            call.flush(timeout=30),
        ])


    @patch("builtins.bytes")
    def test_to_bytes(self, mock_bytes):
        self.assertEqual(mock_bytes.return_value, _to_bytes('some string'))
        mock_bytes.assert_called_with('some string', encoding='utf-8')