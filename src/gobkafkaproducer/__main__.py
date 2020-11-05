import json

from kafka.producer import KafkaProducer

from gobcore.message_broker.config import EVENT_EXCHANGE
from gobcore.message_broker.events import NEW_EVENT_PREFIX
from gobcore.message_broker.initialise_queues import create_queue_with_binding
from gobcore.message_broker.messagedriven_service import MessagedrivenService
from gobkafkaproducer.config import KAFKA_TOPIC, KAFKA_CONNECTION_CONFIG

KAFKA_EVENTS_QUEUE = f"{EVENT_EXCHANGE}.kafka"


def init_listen_queue():
    """Receive all new events on the EVENTS_QUEUE

    ALL events will be written to a single queue. This means we can only have ONE consumer instance of this
    container running at the same time, otherwise the ordering of events isn't guaranteed.

    TODO: Implement logic later to create one queue per routing key (of the form event.catalog,collection, for example
    event.gebieden.buurten) and have exactly one instance of this container listening to any specific queue. This can
    be done partly with RabbitMQ, but we need fail-over and proper handling. Have a go at this later.

    :return:
    """
    create_queue_with_binding(EVENT_EXCHANGE, KAFKA_EVENTS_QUEUE, f"{NEW_EVENT_PREFIX}#")
    return KAFKA_EVENTS_QUEUE


def _to_bytes(s: str):
    return bytes(s, encoding='utf-8')


def new_events_handler(msg):
    header_fields = {
        'event_type': 'name',
        'event_id': 'event_id',
        'source_id': 'source_id',
        'last_event': 'last_event_id',
        'catalog': 'catalog',
        'collection': 'collection',
        'source': 'source',
    }
    producer = KafkaProducer(**KAFKA_CONNECTION_CONFIG)

    for event in msg['contents']:
        headers = [(k, _to_bytes(str(event['header'][v])) if event['header'][v] else b'')
                   for k, v in header_fields.items()]

        producer.send(
            KAFKA_TOPIC,
            key=_to_bytes(f"{event['header']['catalog']}.{event['header']['collection']}"),
            value=_to_bytes(json.dumps(event['contents'])),
            headers=headers,
        )

    producer.flush()
    print(f"Sent {len(msg['contents'])} events to Kafka")


SERVICEDEFINITION = {
    'event_to_hub': {
        'queue': lambda: init_listen_queue(),
        'handler': new_events_handler,
    }
}


def init():
    if __name__ == "__main__":
        MessagedrivenService(SERVICEDEFINITION, "KafkaProducer").start()


init()
