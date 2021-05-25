import json
import logging
from gobcore.model import GOBModel
from gobcore.model.relations import get_relations_for_collection, split_relation_table_name
from gobcore.typesystem import get_gob_type_from_info
from kafka.producer import KafkaProducer
from sqlalchemy import MetaData, and_, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from gobkafkaproducer.config import DATABASE_CONFIG, GOB_DATABASE_CONFIG, KAFKA_CONNECTION_CONFIG, KAFKA_TOPIC
from gobkafkaproducer.database.model import Base, LastSentEvent

logging.getLogger("kafka").setLevel(logging.WARNING)


def _to_bytes(s: str):
    return bytes(s, encoding='utf-8')


class EventDataBuilder:
    gobmodel = None

    def __init__(self, gob_db_session, gob_db_base, catalogue: str, collection: str):
        if self.gobmodel is None:
            self.gobmodel = GOBModel()

        self.db_session = gob_db_session
        self.base = gob_db_base

        self.collection = self.gobmodel.get_collection(catalogue, collection)
        self.tablename = self.gobmodel.get_table_name(catalogue, collection)
        self.basetable = getattr(self.base.classes, self.gobmodel.get_table_name(catalogue, collection))

        self._init_relations(catalogue, collection)

    def _init_relations(self, catalogue: str, collection: str):
        self.relations = {}

        for attr_name, relname in get_relations_for_collection(self.gobmodel, catalogue, collection).items():
            rel_table_name = self.gobmodel.get_table_name('rel', relname)
            self.relations[attr_name] = {
                'relation_table_name': rel_table_name,
                'dst_table_name': self._get_rel_dst_tablename(rel_table_name),
            }

    def _get_rel_dst_tablename(self, rel_table_name: str):
        info = split_relation_table_name(rel_table_name)
        reference = self.gobmodel.get_reference_by_abbreviations(info['dst_cat_abbr'], info['dst_col_abbr'])
        return self.gobmodel.get_table_name_from_ref(reference)

    def build_event(self, tid: str) -> dict:
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
                type = get_gob_type_from_info(attr)
                type_instance = type.from_value(getattr(obj, attr_name))
                result[attr_name] = str(type_instance.to_value)
        return result


class KafkaEventProducer:
    FLUSH_PER = 10000
    gobmodel = GOBModel()

    def __init__(self, catalogue: str, collection: str, logger):
        self.catalogue = catalogue
        self.collection = collection
        self.logger = logger
        self.gob_db_session = None
        self.db_session = None
        self.gob_db_base = None
        self.Event = None
        self.producer = None
        self.total_cnt = 0

        self._init_connections()

        self.event_builder = EventDataBuilder(self.gob_db_session, self.gob_db_base, catalogue, collection)

    def _get_tables_to_reflect(self):
        """Returns tables to reflect:
        - events
        - object table (e.g. gebieden_buurten)
        - relation tables (e.g. rel_gb_brt_gbd_wijk_ligt_in_wijk, ...)

        :return:
        """
        relations = get_relations_for_collection(self.gobmodel, self.catalogue, self.collection)
        relation_tables = [self.gobmodel.get_table_name('rel', rel_table) for rel_table in relations.values()]

        return [
                   'events',
                   self.gobmodel.get_table_name(self.catalogue, self.collection)
               ] + relation_tables

    def _init_gob_db_session(self):
        """Inits db session for gob db (to access events)

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

    def _init_local_db_session(self):
        """Inits db session for local (gob_kafka) db

        :return:
        """
        engine = create_engine(URL(**DATABASE_CONFIG), connect_args={'sslmode': 'require'})
        Base.metadata.bind = engine
        self.db_session = Session(engine)

    def _init_kafka(self):
        self.producer = KafkaProducer(**KAFKA_CONNECTION_CONFIG,
                                      max_in_flight_requests_per_connection=1,
                                      # With retries, max_in_flight should always be 1 to ensure ordering of batches!
                                      retries=3)
        self.logger.info("Initialised Kafka connection")

    def _init_connections(self):
        self._init_gob_db_session()
        self._init_local_db_session()
        self._init_kafka()

    def _get_last_event(self):
        last_event = self.db_session \
            .query(LastSentEvent) \
            .filter_by(catalogue=self.catalogue, collection=self.collection) \
            .first()

        return last_event

    def _get_last_eventid(self):
        last_event = self._get_last_event()
        return last_event.last_event if last_event else -1

    def _set_last_eventid(self, eventid: int):
        last_event = self._get_last_event()

        if last_event:
            last_event.last_event = eventid
        else:
            last_event = LastSentEvent(catalogue=self.catalogue, collection=self.collection, last_event=eventid)
            self.db_session.add(last_event)

        self.db_session.commit()

    def _get_events(self, min_eventid: int):
        return self.gob_db_session \
            .query(self.Event) \
            .yield_per(10000) \
            .filter(and_(self.Event.catalogue == self.catalogue, self.Event.entity == self.collection,
                         self.Event.eventid > min_eventid)) \
            .order_by(self.Event.eventid.asc())

    def _add_event(self, event):
        header = {
            'event_type': event.action,
            'event_id': event.eventid,
            'tid': event.tid,
            'catalog': event.catalogue,
            'collection': event.entity,
        }
        headers = [(k, _to_bytes(str(v)) if v else b'') for k, v in header.items()]
        data = self.event_builder.build_event(event.tid)

        self.producer.send(
            KAFKA_TOPIC,
            key=_to_bytes(header['tid']),
            value=_to_bytes(json.dumps(data)),
            headers=headers
        )

    def _flush(self, last_eventid: int):
        self.producer.flush(timeout=120)
        self._set_last_eventid(last_eventid)
        print(f"Flushed Kafka events. Total events: {self.total_cnt}. Last event id: {last_eventid}")

    def produce(self):
        last_eventid = self._get_last_eventid()
        self.logger.info(f"Start producing. Last event was {last_eventid}")

        events = self._get_events(last_eventid)

        for event in events:
            self._add_event(event)

            self.total_cnt += 1
            last_eventid = event.eventid

            self.gob_db_session.expunge(event)

            if self.total_cnt % self.FLUSH_PER == 0:
                self._flush(last_eventid)

        self._flush(last_eventid)
        self.logger.info(f"Produced {self.total_cnt} Kafka events")
