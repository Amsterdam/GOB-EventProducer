import warnings

from sqlalchemy import MetaData, and_, create_engine
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, selectinload, with_loader_criteria

from gobeventproducer import gob_model
from gobeventproducer.config import GOB_DATABASE_CONFIG
from gobeventproducer.utils.relations import RelationInfoBuilder


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
        self.relations = RelationInfoBuilder.build(catalogue, collection)

    def _get_tables_to_reflect(self):
        """Return tables to reflect.

        Tables that are reflected:
        - events
        - object table (e.g. gebieden_buurten)
        - relation tables (e.g. rel_gb_brt_gbd_wijk_ligt_in_wijk, ...)

        :return:
        """
        relation_tables = [relation.relation_table_name for relation in self.relations.values()]
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

    def get_events(self, min_eventid: int, max_eventid: int = None, limit: int = None):
        """Return events between min_eventid (inclusive) and max_eventid (exclusive)."""
        and_filter = [
            self.Event.catalogue == self.catalogue,
            self.Event.entity == self.collection,
            self.Event.eventid > min_eventid,
        ]
        if max_eventid is not None:
            and_filter.append(self.Event.eventid <= max_eventid)

        query = (
            self.session.query(self.Event)
            .yield_per(10_000)
            .filter(and_(*and_filter))
            .order_by(self.Event.eventid.asc())
        )
        if limit is not None:
            query = query.limit(limit)
        return query

    def _query_object(self):
        query = self.session.query(self.ObjectTable)
        options = []

        for relation in self.relations.values():
            if relation.is_many:
                continue

            rel_src_attr = getattr(self.ObjectTable, f"{relation.relation_table_name}_collection")
            rel_obj = getattr(self.base.classes, relation.relation_table_name)
            rel_dst_attr = getattr(rel_obj, relation.dst_table_name)
            rel_date_deleted = getattr(rel_obj, "_date_deleted")

            # Eager load relation tables and dst table, skip deleted
            options += [
                selectinload(rel_src_attr).selectinload(rel_dst_attr),
                with_loader_criteria(rel_obj, rel_date_deleted.is_(None)),
            ]

        return query.options(*options)

    def get_objects(self):
        """Get all objects for this table."""
        return (
            self._query_object()
            .filter(self.ObjectTable._date_deleted == None)  # noqa: E711
            .order_by(self.ObjectTable._last_event.asc())
            .yield_per(5_000)
        )

    def get_object(self, tid: str):
        """Get full object for given tid."""
        return self._query_object().filter(self.ObjectTable._tid == tid).one()
