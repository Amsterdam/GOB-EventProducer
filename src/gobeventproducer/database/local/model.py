from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class LastSentEvent(Base):
    """Holds the last event ID that is sent for the given catalog/collection."""

    __tablename__ = "last_sent_events"

    catalogue = Column(String, doc="The catalogue", primary_key=True)
    collection = Column(String, doc="The collection", primary_key=True)
    last_event = Column(Integer, doc="The id of the last event sent")

    def __repr__(self):
        """Represent this object as string."""
        return f"<LastSentEvent {self.catalogue} {self.collection} ({self.last_event})>"
