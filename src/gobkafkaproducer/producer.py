import json
import logging
from gobcore.events import database_to_gobevent
from gobcore.events.import_events import ImportEvent
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


class KafkaEventProducer:
    FLUSH_PER = 10000

    def __init__(self, catalogue: str, collection: str, logger):
        self.catalogue = catalogue
        self.collection = collection
        self.logger = logger
        self.gob_db_session = None
        self.db_session = None
        self.Event = None
        self.producer = None
        self.total_cnt = 0

        self._init_connections()

    def _init_gob_db_session(self):
        """Inits db session for gob db (to access events)

        :return:
        """
        engine = create_engine(URL(**GOB_DATABASE_CONFIG))
        self.gob_db_session = Session(engine)
        meta = MetaData()
        meta.reflect(engine, only=['events'])
        base = automap_base(metadata=meta)
        base.prepare()
        self.Event = base.classes.events
        self.logger.info("Initialised events storage")

    def _init_local_db_session(self):
        """Inits db session for local (gob_kafka) db

        :return:
        """
        engine = create_engine(URL(**DATABASE_CONFIG))
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

    def _add_event(self, event: ImportEvent):
        header = {
            'event_type': event.name,
            'event_id': event.id,
            # entity_source_id is the source id from before the event. not always present (for example with an ADD)
            'source_id': event.data.get('_entity_source_id', event.data['_source_id']),
            'last_event': event.last_event,
            'catalog': event.catalogue,
            'collection': event.entity,
            'source': event.source,
        }
        headers = [(k, _to_bytes(str(v)) if v else b'') for k, v in header.items()]

        self.producer.send(
            KAFKA_TOPIC,
            key=_to_bytes(header['source_id']),
            value=_to_bytes(json.dumps(event.data)),
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
            gob_event = database_to_gobevent(event)
            self._add_event(gob_event)

            self.total_cnt += 1
            last_eventid = gob_event.id

            if self.total_cnt % self.FLUSH_PER == 0:
                self._flush(last_eventid)

        self._flush(last_eventid)
        self.logger.info(f"Produced {self.total_cnt} Kafka events")
