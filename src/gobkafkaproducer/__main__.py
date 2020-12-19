from gobcore.logging.logger import logger
from gobcore.message_broker.config import KAFKA_PRODUCE, KAFKA_PRODUCE_QUEUE, KAFKA_PRODUCE_RESULT_KEY, \
    WORKFLOW_EXCHANGE
from gobcore.message_broker.messagedriven_service import MessagedrivenService
from gobcore.message_broker.notifications import get_notification, listen_to_notifications
from gobcore.workflow.start_workflow import start_workflow
from gobkafkaproducer.database.connection import connect
from gobkafkaproducer.producer import KafkaEventProducer


def new_events_notification_handler(msg):
    notification = get_notification(msg)

    workflow = {'workflow_name': KAFKA_PRODUCE}
    arguments = {
        'catalogue': notification.header.get('catalogue'),
        'collection': notification.header.get('collection'),
        'application': notification.header.get('application'),
        'process_id': notification.header.get('process_id'),
    }
    start_workflow(workflow, arguments)


def kafka_produce_handler(msg):
    logger.configure(msg, "KAFKA_PRODUCE")
    logger.info("Produce Kafka events")

    catalogue = msg.get('header', {}).get('catalogue')
    collection = msg.get('header', {}).get('collection')

    assert catalogue and collection, "Missing catalogue and collection in header"

    event_producer = KafkaEventProducer(catalogue, collection, logger)
    event_producer.produce()

    return {
        'header': msg['header'],
        'summary': {
            'produced': event_producer.total_cnt,
        }
    }


SERVICEDEFINITION = {
    'event_to_hub_notification': {
        'queue': lambda: listen_to_notifications("kafka", "events"),
        'handler': new_events_notification_handler,
    },
    'event_to_hub_request': {
        'queue': KAFKA_PRODUCE_QUEUE,
        'handler': kafka_produce_handler,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': KAFKA_PRODUCE_RESULT_KEY,
        }
    }
}


def init():
    if __name__ == "__main__":
        connect()
        MessagedrivenService(SERVICEDEFINITION, "KafkaProducer").start()


init()
