import logging

from sqlalchemy import MetaData, and_, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from gobcore.model.relations import get_relations_for_collection, split_relation_table_name
from gobcore.typesystem import get_gob_type_from_info
from gobcore.message_broker.config import CONNECTION_PARAMS, EVENTS_EXCHANGE
from gobcore.message_broker.async_message_broker import AsyncConnection

from gobeventproducer import gob_model
from gobeventproducer.config import DATABASE_CONFIG, GOB_DATABASE_CONFIG
from gobeventproducer.database.model import Base, LastSentEvent

logging.getLogger("eventproducer").setLevel(logging.WARNING)


def _to_bytes(s: str):
    return bytes(s, encoding='utf-8')


class EventDataBuilder:
    """Helper class that generates external event data."""

    def __init__(self, gob_db_session, gob_db_base, catalogue: str, collection_name: str):
        self.db_session = gob_db_session
        self.base = gob_db_base

        self.collection = gob_model[catalogue]['collections'][collection_name]
        self.tablename = gob_model.get_table_name(catalogue, collection_name)
        self.basetable = getattr(self.base.classes, self.tablename)

        self._init_relations(catalogue, collection_name)

    def _init_relations(self, catalogue: str, collection_name: str):
        self.relations = {}

        for attr_name, relname in get_relations_for_collection(
                gob_model, catalogue, collection_name).items():
            rel_table_name = gob_model.get_table_name('rel', relname)
            self.relations[attr_name] = {
                'relation_table_name': rel_table_name,
                'dst_table_name': self._get_rel_dst_tablename(rel_table_name),
            }

    def _get_rel_dst_tablename(self, rel_table_name: str):
        info = split_relation_table_name(rel_table_name)
        reference = gob_model.get_reference_by_abbreviations(info['dst_cat_abbr'], info['dst_col_abbr'])
        return gob_model.get_table_name_from_ref(reference)

    def build_event(self, tid: str) -> dict:
        """Build event data for object with given tid."""
        query = self.db_session.query(self.basetable).filter(self.basetable._tid == tid)
        obj = query.one()

        result = {}
        for attr_name, attr in self.collection['attributes'].items():
            if 'Reference' in attr['type']:
                relation = self.relations[attr_name]
                relation_table_rows = getattr(obj, f"{relation['relation_table_name']}_collection")
                relation_obj = []
                for row in relation_table_rows:
                    dst_table = getattr(row, relation['dst_table_name'])
                    rel = {
                        'tid': dst_table._tid,
                        'id': dst_table._id,
                        'begin_geldigheid': str(row.begin_geldigheid) if row.begin_geldigheid else None,
                        'eind_geldigheid': str(row.eind_geldigheid) if row.eind_geldigheid else None,
                    }
                    if hasattr(dst_table, 'volgnummer'):
                        rel['volgnummer'] = dst_table.volgnummer

                    relation_obj.append(rel)

                result[attr_name] = relation_obj
            else:
                gob_type = get_gob_type_from_info(attr)
                type_instance = gob_type.from_value(getattr(obj, attr_name))
                result[attr_name] = str(type_instance.to_value)
        return result


class LocalDatabaseConnection:
    """Abstraction for receiving and updating the last event that is sent."""

    def __init__(self, catalogue: str, collection: str):
        self.catalogue = catalogue
        self.collection = collection
        self.session = None
        self.last_event = None

    def _connect(self):
        engine = create_engine(URL(**DATABASE_CONFIG), connect_args={'sslmode': 'require'})
        Base.metadata.bind = engine
        self.session = Session(engine)

    def __enter__(self):
        """Enter context, connect to local database."""
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context, commit last event to database."""
        self.session.commit()

    def get_last_event(self) -> LastSentEvent:
        """Return the last event.

        The last event is either saved locally in this class, fetched from the database, or a newly generated
        object.
        """
        if self.last_event:
            return self.last_event

        self.last_event = self.session \
            .query(LastSentEvent) \
            .filter_by(catalogue=self.catalogue, collection=self.collection) \
            .first()

        if not self.last_event:
            self.last_event = LastSentEvent(
                catalogue=self.catalogue, collection=self.collection, last_event=-1)
            self.session.add(self.last_event)

        return self.last_event

    def get_last_eventid(self):
        """Return last event id."""
        return self.get_last_event().last_event

    def set_last_eventid(self, eventid: int):
        """Update last event id."""
        self.get_last_event().last_event = eventid


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
        self._init_gob_db_session()

        self.event_builder = EventDataBuilder(self.gob_db_session, self.gob_db_base, catalogue, collection_name)

    def _get_tables_to_reflect(self):
        """Return tables to reflect.

        Tables that are reflected:
        - events
        - object table (e.g. gebieden_buurten)
        - relation tables (e.g. rel_gb_brt_gbd_wijk_ligt_in_wijk, ...)

        :return:
        """
        relations = get_relations_for_collection(gob_model, self.catalogue, self.collection)
        relation_tables = [gob_model.get_table_name('rel', rel_table) for rel_table in relations.values()]

        return [
            'events',
            gob_model.get_table_name(self.catalogue, self.collection)
        ] + relation_tables

    def _init_gob_db_session(self):
        """Init db session for gob db (to access events).

        :return:
        """
        engine = create_engine(URL(**GOB_DATABASE_CONFIG), connect_args={'sslmode': 'require'})
        self.gob_db_session = Session(engine)
        meta = MetaData()
        meta.reflect(engine, only=self._get_tables_to_reflect())
        base = automap_base(metadata=meta)
        base.prepare()
        self.Event = base.classes.events
        self.gob_db_base = base
        self.logger.info("Initialised events storage")

    def _get_events(self, min_eventid: int, max_eventid: int):
        return self.gob_db_session \
            .query(self.Event) \
            .yield_per(10000) \
            .filter(and_(self.Event.catalogue == self.catalogue,
                         self.Event.entity == self.collection,
                         self.Event.eventid > min_eventid,
                         self.Event.eventid <= max_eventid)) \
            .order_by(self.Event.eventid.asc())

    def _add_event(self, event, connection):
        header = {
            'event_type': event.action,
            'event_id': event.eventid,
            'tid': event.tid,
            'catalog': event.catalogue,
            'collection': event.entity,
        }
        data = self.event_builder.build_event(event.tid)

        msg = {
            'header': header,
            'data': data
        }
        connection.publish(EVENTS_EXCHANGE, self.routing_key, msg)

    def produce(self, min_eventid: int, max_eventid: int):
        """Produce external events starting from min_eventid (exclusive) until max_eventid (inclusive)."""
        start_eventid = min_eventid

        with LocalDatabaseConnection(self.catalogue, self.collection) as localdb:
            last_eventid = localdb.get_last_eventid()

            # Ideally we would remove the need for the database. We keep the database in place now to be able to spot
            # any errors thay may arise when min_eventid does not match the expected last_eventid.
            if last_eventid != min_eventid:
                self.logger.warning(f"Min eventid ({min_eventid}) to produce does not match last_eventid "
                                    f"({last_eventid}) in database. Recovering.")
                start_eventid = last_eventid

            self.logger.info(f"Start producing events > {start_eventid} and <= {max_eventid}")

            events = self._get_events(start_eventid, max_eventid)

            with AsyncConnection(CONNECTION_PARAMS) as rabbitconn:
                for event in events:
                    self._add_event(event, rabbitconn)

                    self.gob_db_session.expunge(event)
                    localdb.set_last_eventid(event.eventid)
                    self.total_cnt += 1

            self.logger.info(f"Produced {self.total_cnt} events")
