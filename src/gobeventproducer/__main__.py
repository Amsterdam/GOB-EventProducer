"""gobeventproducer main entry."""
from gobcore.logging.logger import logger
from gobcore.message_broker.config import (
    EVENT_PRODUCE,
    EVENT_PRODUCE_QUEUE,
    EVENT_PRODUCE_RESULT_KEY,
    WORKFLOW_EXCHANGE,
)
from gobcore.message_broker.messagedriven_service import MessagedrivenService
from gobcore.message_broker.notifications import EventNotification, get_notification, listen_to_notifications
from gobcore.model.relations import get_catalog_collection_relation_name
from gobcore.workflow.start_workflow import start_workflow

from gobeventproducer import gob_model
from gobeventproducer.config import LISTEN_TO_CATALOGS
from gobeventproducer.database.local.connection import connect
from gobeventproducer.producer import EventProducer, RelationNotProducibleException


def _listening_to_catalogue_collection(notification: EventNotification):
    if notification.header.get("catalogue") in LISTEN_TO_CATALOGS:
        return True

    if notification.header.get("catalogue") == "rel":
        collection_name = notification.header.get("collection")
        main_catalog_name, *_ = get_catalog_collection_relation_name(gob_model, collection_name)

        if main_catalog_name in LISTEN_TO_CATALOGS:
            return True

    return False


def new_events_notification_handler(msg):
    """Handle new events notifications."""
    notification = get_notification(msg)

    if not _listening_to_catalogue_collection(notification):
        return

    workflow = {"workflow_name": EVENT_PRODUCE}
    arguments = {
        "catalogue": notification.header.get("catalogue"),
        "collection": notification.header.get("collection"),
        "application": None,  # To avoid that multiple produce jobs run in parallel for different applications
        "process_id": notification.header.get("process_id"),
        "contents": notification.contents,
    }
    start_workflow(workflow, arguments)


def event_produce_handler(msg):
    """Handle event produce request message."""
    catalogue = msg.get("header", {}).get("catalogue")
    collection = msg.get("header", {}).get("collection")

    assert catalogue and collection, "Missing catalogue and collection in header"

    try:
        event_producer = EventProducer(catalogue, collection, logger)
    except RelationNotProducibleException:
        logger.info(
            f"Relation is not producible because it is not defined in the destination schema: "
            f"{catalogue}.{collection}. Skipping."
        )
        return {
            "header": msg["header"],
            "summary": {
                "produced": 0,
            },
        }

    mode = msg.get("header", {}).get("mode")

    if mode == "full":
        logger.info("Produce full load events")
        produced_cnt = event_producer.produce_initial()
    else:
        logger.info("Produce Events")

        min_eventid, max_eventid = msg.get("contents", {}).get("last_event", (None, None))
        produced_cnt = event_producer.produce(min_eventid, max_eventid)

    return {
        "header": msg["header"],
        "summary": {
            "produced": produced_cnt,
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
