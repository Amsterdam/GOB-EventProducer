import logging
from datetime import datetime
from typing import Iterator

from gobcore.events.import_events import ADD
from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS, EVENTS_EXCHANGE
from gobcore.model.name_compressor import NameCompressor
from gobcore.model.relations import get_catalog_collection_relation_name
from more_itertools import peekable

from gobeventproducer import gob_model
from gobeventproducer.database.gob.contextmanager import GobDatabaseConnection
from gobeventproducer.database.local.contextmanager import LocalDatabaseConnection
from gobeventproducer.eventbuilder import EventDataBuilder
from gobeventproducer.mapper import EventDataMapper, PassThroughEventDataMapper, RelationEventDataMapper
from gobeventproducer.mapping import MappingDefinitionLoader
from gobeventproducer.naming import camel_case

logging.getLogger("eventproducer").setLevel(logging.WARNING)


MAX_EVENTS_PER_MESSAGE = 200


class BatchEventsMessagePublisher:
    """Publish events in batches using a context manager."""

    def __init__(self, rabbitconnection, routing_key: str, log_name: str, localdb: LocalDatabaseConnection):
        self.events = []
        self.routing_key = routing_key
        self.rabbitconnection = rabbitconnection
        self.cnt = 0
        self.log_name = log_name
        self.log_per = 10_000
        self.localdb = localdb

    def __enter__(self):
        """Enter context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        self._flush()
        self._log_cnt()

    def _log_cnt(self):
        print(f"{self.log_name}: {self.cnt}")

    def add_event(self, event: dict):
        """Add event to batch."""
        self.events.append(event)
        self.cnt += 1

        if self.cnt % self.log_per == 0:
            self._log_cnt()

        if len(self.events) == MAX_EVENTS_PER_MESSAGE:
            self._flush()

    def _publish(self, events: list):
        self.rabbitconnection.publish(EVENTS_EXCHANGE, self.routing_key, events)

    def _flush(self):
        if self.events:
            self._publish(self.events)
            self.localdb.set_last_eventid(self.events[-1]["header"]["event_id"])

            self.events = []


class EventProducer:
    """Produce events for external consumers."""

    def __init__(self, catalog: str, collection_name: str, logger):
        self.catalog = catalog
        self.collection = collection_name
        self.logger = logger
        self.gob_db_session = None
        self.gob_db_base = None
        self.Event = None
        self.relation_name = None

        if catalog == "rel":
            main_catalog_name, main_collection_name, relation_name = get_catalog_collection_relation_name(
                gob_model, collection_name
            )

            main_mapping_definition = MappingDefinitionLoader().get(main_catalog_name, main_collection_name)
            main_mapper = (
                EventDataMapper(main_mapping_definition) if main_mapping_definition else PassThroughEventDataMapper()
            )
            relation_name = NameCompressor.uncompress_name(relation_name)
            relation_name = main_mapper.get_mapped_name_reverse(relation_name)
            event_collection_name = f"{main_collection_name}_{camel_case(relation_name)}"

            self.mapper = RelationEventDataMapper()

            # For example, for the relation nap_peilmerken ligt_in_bouwblok, we set
            # catalog: nap
            # collection: peilmerken_ligtInBouwblok
            self.header_data = {
                "catalog": main_catalog_name,
                "collection": event_collection_name,
            }
            # e.g. nap.peilmerken.rel.peilmerken_ligtInBouwblok
            self.routing_key = f"{main_catalog_name}.{main_collection_name}.rel.{event_collection_name}"

        else:
            mapping_definition = MappingDefinitionLoader().get(self.catalog, self.collection)
            self.mapper = EventDataMapper(mapping_definition) if mapping_definition else PassThroughEventDataMapper()
            self.header_data = {
                "catalog": self.catalog,
                "collection": self.collection,
            }
            self.routing_key = f"{catalog}.{collection_name}"

    def _build_event(self, event_action: str, event_id: int, object_tid: str, data: object, event_builder):
        header = {
            **self.header_data,
            "event_type": event_action,
            "event_id": event_id,
            "tid": object_tid,
            "generated_timestamp": datetime.now().isoformat(),
        }
        data = event_builder.build_event(data)
        transformed_data = self.mapper.map(data)

        return {"header": header, "data": transformed_data}

    def _produce(self, events: Iterator):
        with AsyncConnection(CONNECTION_PARAMS) as rabbitconn, LocalDatabaseConnection(
            self.catalog, self.collection
        ) as localdb:
            with BatchEventsMessagePublisher(
                rabbitconn, self.routing_key, f"{self.catalog} {self.collection}", localdb
            ) as batch_builder:
                for event in events:
                    batch_builder.add_event(event)

                self.logger.info(f"Produced {batch_builder.cnt} events.")
                return batch_builder.cnt

    def _generate_by_eventids(self, min_eventid: int, max_eventid: int = None):
        event_builder = EventDataBuilder(self.catalog, self.collection)
        with GobDatabaseConnection(self.catalog, self.collection, self.logger) as gobdb:
            current_max_id = None
            start_eventid = min_eventid
            while True:
                events_ = gobdb.get_events(start_eventid, max_eventid, MAX_EVENTS_PER_MESSAGE)

                for event_ in events_:
                    obj = gobdb.get_object(event_.tid)
                    external_event = self._build_event(event_.action, event_.eventid, event_.tid, obj, event_builder)
                    yield external_event
                    current_max_id = event_.eventid
                    gobdb.session.expunge(event_)

                if current_max_id is None or current_max_id == start_eventid:
                    break

                start_eventid = current_max_id

    def produce(self, min_eventid: int = None, max_eventid: int = None):
        """Produce external events starting from min_eventid (exclusive) until max_eventid (inclusive)."""
        start_eventid = min_eventid

        with LocalDatabaseConnection(self.catalog, self.collection) as localdb:
            last_eventid = localdb.get_last_eventid()

        if min_eventid is None:
            self.logger.info(f"No min_eventid specified. Starting from last_eventid ({last_eventid})")
            start_eventid = last_eventid
        # Ideally we would remove the need for the database. We keep the database in place now to be able to spot
        # any errors thay may arise when min_eventid does not match the expected last_eventid.
        elif last_eventid != min_eventid:
            if last_eventid == -1:
                self.logger.warning("Have no previous produced events in database. Starting from beginning")
            else:
                self.logger.warning(
                    f"Min eventid ({min_eventid}) to produce does not match last_eventid "
                    f"({last_eventid}) in database. Recovering."
                )
            start_eventid = last_eventid

        start_msg = "from beginning" if start_eventid == -1 else f"> {start_eventid}"
        max_msg = f" and <= {max_eventid}" if max_eventid is not None else ""
        self.logger.info(f"Start producing events {start_msg}{max_msg}")

        return self._produce(self._generate_by_eventids(start_eventid, max_eventid))

    def produce_initial(self):
        """Produce external ADD events for the current state of the database.

        Adds the 'full_load_sequence' property to the header, along with 'first_of_sequence' and 'last_of_sequence'.
        This is the mechanism that communicates to the consumer that a full load is in progress
        """
        with GobDatabaseConnection(self.catalog, self.collection, self.logger) as gobdb:
            event_builder = EventDataBuilder(self.catalog, self.collection)
            objects = peekable(gobdb.get_objects())

            self.logger.info(
                f"Start generating ADD events for current database state " f"using routing key {self.routing_key}"
            )

            def event_generator(objects_: list):
                first_of_sequence = True
                for obj in objects_:
                    external_event = self._build_event(ADD.name, obj._last_event, obj._tid, obj, event_builder)

                    external_event["header"] |= {
                        "full_load_sequence": True,
                        "first_of_sequence": first_of_sequence,
                        "last_of_sequence": False if objects.peek(None) else True,
                    }
                    first_of_sequence = False
                    yield external_event

            return self._produce(event_generator(objects))
