import warnings

from gobcore.model.relations import get_relations_for_collection
from sqlalchemy import MetaData, and_, create_engine
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from gobeventproducer import gob_model
from gobeventproducer.config import GOB_DATABASE_CONFIG


class GobDatabaseConnection:
    """Abstraction for getting data from the GOB DB."""

    def __init__(self, catalogue: str, collection: str, logger):
        self.catalogue = catalogue
        self.collection = collection
        self.logger = logger
        self.Event = None
        self.ObjectTable = None
        self.base = None
        self.session = None

    def _get_tables_to_reflect(self):
        """Return tables to reflect.

        Tables that are reflected:
        - events
        - object table (e.g. gebieden_buurten)
        - relation tables (e.g. rel_gb_brt_gbd_wijk_ligt_in_wijk, ...)

        :return:
        """
        relations = get_relations_for_collection(gob_model, self.catalogue, self.collection)
        relation_tables = [gob_model.get_table_name("rel", rel_table) for rel_table in relations.values()]

        return ["events", gob_model.get_table_name(self.catalogue, self.collection)] + relation_tables

    def _connect(self):
        engine = create_engine(URL(**GOB_DATABASE_CONFIG), connect_args={"sslmode": "require"})
        self.session = Session(engine)

        meta = MetaData()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            meta.reflect(engine, only=self._get_tables_to_reflect())
        base = automap_base(metadata=meta)
        base.prepare()
        self.Event = base.classes.events
        tablename = gob_model.get_table_name(self.catalogue, self.collection)
        self.ObjectTable = getattr(base.classes, tablename)
        self.base = base
        self.logger.info("Initialised events storage")

    def __enter__(self):
        """Enter context, connect to GOB database."""
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context, commit any uncommitted changes."""
        self.session.commit()

    def get_events(self, min_eventid: int, max_eventid: int):
        """Return events between min_eventid (inclusive) and max_eventid (exclusive)."""
        return (
            self.session.query(self.Event)
            .yield_per(10_000)
            .filter(
                and_(
                    self.Event.catalogue == self.catalogue,
                    self.Event.entity == self.collection,
                    self.Event.eventid > min_eventid,
                    self.Event.eventid <= max_eventid,
                )
            )
            .order_by(self.Event.eventid.asc())
        )

    def get_objects(self):
        """Get all objects for this table."""
        return self.session.query(self.ObjectTable).yield_per(10_000)

    def get_object(self, tid: str):
        """Get full object for given tid."""
        return self.session.query(self.ObjectTable).filter(self.ObjectTable._tid == tid).one()
