"""gobeventproducer main entry."""
from gobcore.logging.logger import logger
from gobcore.message_broker.config import (
    EVENT_PRODUCE,
    EVENT_PRODUCE_QUEUE,
    EVENT_PRODUCE_RESULT_KEY,
    WORKFLOW_EXCHANGE,
)
from gobcore.message_broker.messagedriven_service import MessagedrivenService
from gobcore.message_broker.notifications import get_notification, listen_to_notifications
from gobcore.workflow.start_workflow import start_workflow

from gobeventproducer.config import LISTEN_TO_CATALOGS
from gobeventproducer.database.local.connection import connect
from gobeventproducer.producer import EventProducer


def new_events_notification_handler(msg):
    """Handle new events notifications."""
    notification = get_notification(msg)

    if notification.header.get("catalogue") not in [*LISTEN_TO_CATALOGS, "rel"]:
        return

    workflow = {"workflow_name": EVENT_PRODUCE}
    arguments = {
        "catalogue": notification.header.get("catalogue"),
        "collection": notification.header.get("collection"),
        "application": notification.header.get("application"),
        "process_id": notification.header.get("process_id"),
        "contents": notification.contents,
    }
    start_workflow(workflow, arguments)


def event_produce_handler(msg):
    """Handle event produce request message."""
    logger.info("Produce Events")

    catalogue = msg.get("header", {}).get("catalogue")
    collection = msg.get("header", {}).get("collection")

    assert catalogue and collection, "Missing catalogue and collection in header"

    min_eventid, max_eventid = msg["contents"]["last_event"]
    event_producer = EventProducer(catalogue, collection, logger)
    event_producer.produce(min_eventid, max_eventid)

    return {
        "header": msg["header"],
        "summary": {
            "produced": event_producer.total_cnt,
        },
    }


SERVICEDEFINITION = {
    "event_to_hub_notification": {
        "queue": lambda: listen_to_notifications("eventproducer", "events"),
        "handler": new_events_notification_handler,
    },
    "event_to_hub_request": {
        "queue": EVENT_PRODUCE_QUEUE,
        "handler": event_produce_handler,
        "logger": "EVENT_PRODUCE",
        "report": {
            "exchange": WORKFLOW_EXCHANGE,
            "key": EVENT_PRODUCE_RESULT_KEY,
        },
    },
}


def init():
    """Initialise and start module."""
    if __name__ == "__main__":
        connect()
        MessagedrivenService(SERVICEDEFINITION, "EventProducer").start()


init()
