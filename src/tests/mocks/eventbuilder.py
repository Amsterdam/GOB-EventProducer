class MockEventBuilder:
    def __init__(self, *args):
        pass

    def build_event(self, tid):
        return {'tid': tid, 'a': 'A', 'b': 'B'}

