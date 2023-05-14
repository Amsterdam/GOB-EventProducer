import warnings
from typing import Optional

from sqlalchemy import MetaData, and_, create_engine
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import AutomapBase, automap_base
from sqlalchemy.orm import Session, selectinload

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
        self.base: Optional[AutomapBase] = None
        self.session: Optional[Session] = None
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
        self.session.commit()  # type: ignore[union-attr]

    def get_events(self, min_eventid: int, max_eventid: Optional[int] = None, limit: Optional[int] = None):
        """Return events between min_eventid (inclusive) and max_eventid (exclusive)."""
        and_filter = [
            self.Event.catalogue == self.catalogue,  # type: ignore[attr-defined]
            self.Event.entity == self.collection,  # type: ignore[attr-defined]
            self.Event.eventid > min_eventid,  # type: ignore[attr-defined]
        ]
        if max_eventid is not None:
            and_filter.append(self.Event.eventid <= max_eventid)  # type: ignore[attr-defined]

        query = (
            self.session.query(self.Event)  # type: ignore[union-attr]
            .yield_per(10_000)
            .filter(and_(*and_filter))
            .order_by(self.Event.eventid.asc())  # type: ignore[attr-defined]
        )
        if limit is not None:
            query = query.limit(limit)
        return query

    def _query_object(self):
        query = self.session.query(self.ObjectTable)  # type: ignore[union-attr]
        options = [
            # Eager load relation tables and dst table
            selectinload(getattr(self.ObjectTable, f"{relation.relation_table_name}_collection")).selectinload(
                getattr(
                    getattr(self.base.classes, relation.relation_table_name),  # type: ignore[union-attr]
                    relation.dst_table_name,
                )
            )
            for relation in self.relations.values()
        ]
        return query.options(*options)

    def get_objects(self):
        """Get all objects for this table."""
        return (
            self._query_object()
            .filter(self.ObjectTable._date_deleted == None)  # type: ignore[attr-defined] # noqa: E711
            .order_by(self.ObjectTable._last_event.asc())  # type: ignore[attr-defined]
            .yield_per(5_000)
        )

    def get_object(self, tid: str):
        """Get full object for given tid."""
        return self._query_object().filter(self.ObjectTable._tid == tid).one()  # type: ignore[attr-defined]
