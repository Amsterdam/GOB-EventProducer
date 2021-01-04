from unittest import TestCase

from gobkafkaproducer.database.model import LastSentEvent


class TestLastSentEvent(TestCase):

    def test_repr(self):
        event = LastSentEvent(catalogue='cat', collection='coll', last_event=2480)
        self.assertEqual("<LastSentEvent cat coll (2480)>", str(event))
