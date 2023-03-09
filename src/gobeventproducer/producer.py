import logging

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS, EVENTS_EXCHANGE

from gobeventproducer.database.gob.contextmanager import GobDatabaseConnection
from gobeventproducer.database.local.contextmanager import LocalDatabaseConnection
from gobeventproducer.eventbuilder import EventDataBuilder

logging.getLogger("eventproducer").setLevel(logging.WARNING)


class EventProducer:
    """Produce events for external consumers."""

    def __init__(self, catalogue: str, collection_name: str, logger):
        self.catalogue = catalogue
        self.collection = collection_name
        self.logger = logger
        self.gob_db_session = None
        self.total_cnt = 0
        self.gob_db_base = None
        self.Event = None
        self.routing_key = f"{catalogue}.{collection_name}"

    def _add_event(self, event, connection, event_builder: EventDataBuilder):
        header = {
            "event_type": event.action,
            "event_id": event.eventid,
            "tid": event.tid,
            "catalog": event.catalogue,
            "collection": event.entity,
        }
        data = event_builder.build_event(event.tid)

        msg = {"header": header, "data": data}
        connection.publish(EVENTS_EXCHANGE, self.routing_key, msg)

    def produce(self, min_eventid: int, max_eventid: int):
        """Produce external events starting from min_eventid (exclusive) until max_eventid (inclusive)."""
        start_eventid = min_eventid

        with LocalDatabaseConnection(self.catalogue, self.collection) as localdb, GobDatabaseConnection(
            self.catalogue, self.collection, self.logger
        ) as gobdb:
            last_eventid = localdb.get_last_eventid()

            event_builder = EventDataBuilder(gobdb.session, gobdb.base, self.catalogue, self.collection)

            # Ideally we would remove the need for the database. We keep the database in place now to be able to spot
            # any errors thay may arise when min_eventid does not match the expected last_eventid.
            if last_eventid != min_eventid:
                if last_eventid == -1:
                    self.logger.warning("Have no previous produced events in database. Starting at beginning")
                else:
                    self.logger.warning(
                        f"Min eventid ({min_eventid}) to produce does not match last_eventid "
                        f"({last_eventid}) in database. Recovering."
                    )
                start_eventid = last_eventid

            start_msg = "from beginning" if start_eventid == -1 else f"> {start_eventid}"
            self.logger.info(f"Start producing events {start_msg} and <= {max_eventid}")

            events = gobdb.get_events(start_eventid, max_eventid)

            with AsyncConnection(CONNECTION_PARAMS) as rabbitconn:
                for event in events:
                    self._add_event(event, rabbitconn, event_builder)

                    gobdb.session.expunge(event)
                    localdb.set_last_eventid(event.eventid)
                    self.total_cnt += 1

            self.logger.info(f"Produced {self.total_cnt} events")
