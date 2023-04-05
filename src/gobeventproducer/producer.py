import logging
from typing import Union

from gobcore.events.import_events import ADD
from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS, EVENTS_EXCHANGE
from gobcore.model.relations import split_relation_table_name
from more_itertools import peekable

from gobeventproducer import gob_model
from gobeventproducer.database.gob.contextmanager import GobDatabaseConnection
from gobeventproducer.database.local.contextmanager import LocalDatabaseConnection
from gobeventproducer.eventbuilder import EventDataBuilder
from gobeventproducer.mapper import EventDataMapper, PassThroughEventDataMapper, RelationEventDataMapper
from gobeventproducer.mapping import MappingDefinitionLoader
from gobeventproducer.naming import camel_case

logging.getLogger("eventproducer").setLevel(logging.WARNING)


class EventProducer:
    """Produce events for external consumers."""

    def __init__(self, catalog: str, collection_name: str, logger):
        self.catalog = catalog
        self.collection = collection_name
        self.logger = logger
        self.gob_db_session = None
        self.total_cnt = 0
        self.gob_db_base = None
        self.Event = None
        self.relation_name = None

        if catalog == "rel":
            rel_info = split_relation_table_name(f"rel_{collection_name}")

            main_catalog, main_collection = gob_model.get_catalog_collection_from_abbr(
                rel_info["src_cat_abbr"], rel_info["src_col_abbr"]
            )
            _, dst_collection = gob_model.get_catalog_collection_from_abbr(
                rel_info["dst_cat_abbr"], rel_info["dst_col_abbr"]
            )
            relation_name = rel_info["reference_name"]

            main_catalog_name = main_catalog["name"]
            main_collection_name = main_collection["name"]

            main_mapping_definition = MappingDefinitionLoader().get(main_catalog_name, main_collection_name)
            main_mapper = (
                EventDataMapper(main_mapping_definition) if main_mapping_definition else PassThroughEventDataMapper()
            )
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
            # e.g. nap.rel.peilmerken_ligtInBouwblok
            self.routing_key = f"{main_catalog_name}.rel.{event_collection_name}"

        else:
            mapping_definition = MappingDefinitionLoader().get(self.catalog, self.collection)
            self.mapper = EventDataMapper(mapping_definition) if mapping_definition else PassThroughEventDataMapper()
            self.header_data = {
                "catalog": self.catalog,
                "collection": self.collection,
            }
            self.routing_key = f"{catalog}.{collection_name}"

    def _publish(self, event: dict, connection):
        connection.publish(EVENTS_EXCHANGE, self.routing_key, event)

    def _build_event(self, event_action: str, event_id: Union[str, None], object_tid: str, data: object, event_builder):
        header = {
            **self.header_data,
            "event_type": event_action,
            "event_id": event_id,
            "tid": object_tid,
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

            with AsyncConnection(CONNECTION_PARAMS) as rabbitconn:
                for event in events:
                    obj = gobdb.get_object(event.tid)
                    external_event = self._build_event(event.action, event.eventid, event.tid, obj, event_builder)
                    self._publish(external_event, rabbitconn)

                    gobdb.session.expunge(event)
                    localdb.set_last_eventid(event.eventid)
                    self.total_cnt += 1

            self.logger.info(f"Produced {self.total_cnt} events.")

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

            with AsyncConnection(CONNECTION_PARAMS) as rabbitconn:
                last_eventid = None
                for obj in objects:
                    external_event = self._build_event(ADD.name, None, obj._tid, obj, event_builder)

                    external_event["header"] |= {
                        "full_load_sequence": True,
                        "first_of_sequence": first_of_sequence,
                        "last_of_sequence": False if objects.peek(None) else True,
                    }

                    self._publish(external_event, rabbitconn)
                    self.total_cnt += 1
                    last_eventid = obj._last_event if last_eventid is None else max(last_eventid, obj._last_event)
                    first_of_sequence = False
                localdb.set_last_eventid(last_eventid)

            self.logger.info(f"Produced {self.total_cnt} events.")
