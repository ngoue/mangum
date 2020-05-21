import json
from dataclasses import dataclass

import redis

from mangum.backends.base import WebSocketBackend


@dataclass
class RedisBackend(WebSocketBackend):
    def __post_init__(self) -> None:
        self.connection = redis.Redis.from_url(self.dsn)

    def get(self, key: str) -> dict:
        connection_data = json.loads(self.connection.get(self.connection_id))
        value = connection_data[key]

        return value

    def set(self, key: str, *, value: dict, create: bool = False) -> None:
        _connection_data = {}
        if not create:
            connection_data = self.connection.get(self.connection_id)
            _connection_data = json.loads(connection_data)
        _connection_data[key] = value
        self.connection.set(self.connection_id, json.dumps(_connection_data))

    def delete(self) -> None:
        self.connection.delete(self.connection_id)
