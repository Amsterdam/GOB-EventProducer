class AsyncConnectionMock:
    def __init__(self, connection_params: dict):
        self.connection_params = connection_params
        self.published = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def publish(self, exchange: str, routing_key: str, msg: dict):
        self.published.append((exchange, routing_key, msg))

    def assert_message_published(self, exchange: str, routing_key: str, msg: dict):
        assert (exchange, routing_key, msg) in self.published

    def assert_publish_count(self, n: int):
        assert len(self.published) == n

    def assert_connected_with(self, connection_params: str):
        assert self.connection_params == connection_params
