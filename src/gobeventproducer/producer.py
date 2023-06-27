import itertools
import logging
from datetime import datetime
from typing import Union

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


FULL_LOAD_BATCH_SIZE = 200


class Counter:
    """Simple counter for logging purposes. Logs a line every 10_000 or log_per increments."""

    def __init__(self, name: str, log_per: int = 10_000):
        self.name = name
        self.cnt = 0
        self.log_per = log_per

    def __enter__(self):
        """Enter context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        pass

    def increment(self):
        """Increment counter and log if needed."""
        self.cnt += 1
        if self.cnt % self.log_per == 0:
            print(f"{self.name}: {self.cnt}")


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

    def _publish(self, event: Union[dict, list], connection):
        connection.publish(EVENTS_EXCHANGE, self.routing_key, event)

    def _build_event(self, event_action: str, event_id: Union[str, None], object_tid: str, data: object, event_builder):
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

    def produce(self, min_eventid: int, max_eventid: int):
        """Produce external events starting from min_eventid (exclusive) until max_eventid (inclusive)."""
        start_eventid = min_eventid

        with LocalDatabaseConnection(self.catalog, self.collection) as localdb, GobDatabaseConnection(
            self.catalog, self.collection, self.logger
        ) as gobdb:
            last_eventid = localdb.get_last_eventid()

            event_builder = EventDataBuilder(self.catalog, self.collection)

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

            with AsyncConnection(CONNECTION_PARAMS) as rabbitconn, Counter(
                f"{self.catalog} {self.collection}"
            ) as counter:
                for event in events:
                    obj = gobdb.get_object(event.tid)
                    external_event = self._build_event(event.action, event.eventid, event.tid, obj, event_builder)
                    self._publish(external_event, rabbitconn)

                    gobdb.session.expunge(event)
                    localdb.set_last_eventid(event.eventid)
                    counter.increment()

                self.logger.info(f"Produced {counter.cnt} events.")
                return counter.cnt

    def produce_initial(self):
        """Produce external ADD events for the current state of the database.

        Adds the 'full_load' property to the header and 'finished': True to the last event of the sequence so that the
        consumer knows when the full_load is finished (and a table can be replaced, for example).
        """
        with LocalDatabaseConnection(self.catalog, self.collection) as localdb, GobDatabaseConnection(
            self.catalog, self.collection, self.logger
        ) as gobdb:
            event_builder = EventDataBuilder(self.catalog, self.collection)
            objects = peekable(gobdb.get_objects())
            first_of_sequence = True

            self.logger.info("Start generating ADD events for current database state")

            with AsyncConnection(CONNECTION_PARAMS) as rabbitconn, Counter(
                f"{self.catalog} {self.collection}"
            ) as counter:
                last_eventid = None

                while chunk := itertools.islice(objects, FULL_LOAD_BATCH_SIZE):
                    events_chunk = []
                    for obj in chunk:
                        external_event = self._build_event(ADD.name, None, obj._tid, obj, event_builder)

                        external_event["header"] |= {
                            "full_load_sequence": True,
                            "first_of_sequence": first_of_sequence,
                            "last_of_sequence": False if objects.peek(None) else True,
                        }

                        events_chunk.append(external_event)
                        first_of_sequence = False
                        last_eventid = obj._last_event if last_eventid is None else max(last_eventid, obj._last_event)
                        counter.increment()

                    if len(events_chunk) == 0:
                        break

                    self._publish(events_chunk, rabbitconn)

                localdb.set_last_eventid(last_eventid)

                self.logger.info(f"Produced {counter.cnt} events.")
                return counter.cnt
