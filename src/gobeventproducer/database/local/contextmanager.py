from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

from gobeventproducer.config import DATABASE_CONFIG
from gobeventproducer.database.local.model import Base, LastSentEvent


class LocalDatabaseConnection:
    """Abstraction for receiving and updating the last event that is sent."""

    def __init__(self, catalogue: str, collection: str):
        self.catalogue = catalogue
        self.collection = collection
        self.session = None
        self.last_event = None

    def _connect(self):
        engine = create_engine(URL(**DATABASE_CONFIG), connect_args={"sslmode": "require"})
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

        self.last_event = (
            self.session.query(LastSentEvent).filter_by(catalogue=self.catalogue, collection=self.collection).first()
        )

        if not self.last_event:
            self.last_event = LastSentEvent(catalogue=self.catalogue, collection=self.collection, last_event=-1)
            self.session.add(self.last_event)

        return self.last_event

    def get_last_eventid(self):
        """Return last event id."""
        return self.get_last_event().last_event

    def set_last_eventid(self, eventid: int):
        """Update last event id."""
        self.get_last_event().last_event = eventid
