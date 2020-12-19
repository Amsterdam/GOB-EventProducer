from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class LastSentEvent(Base):

    __tablename__ = "last_sent_events"

    catalogue = Column(String, doc='The catalogue', primary_key=True)
    collection = Column(String, doc='The collection', primary_key=True)
    last_event = Column(Integer, doc='The id of the last event sent')

    def __repr__(self):
        return f'<LastSentEvent {self.catalogue} {self.collection} ({self.last_event})>'
