import typing

from bert import \
        datasource as bert_datasource, \
        utils as bert_utils, \
        constants as bert_constants

PWN = typing.TypeVar('PWN')

class CacheBackend:
    def fill_done_queue(self: PWN, queue_key: str) -> None:
        raise NotImplementedError

    def fill_cache(self: PWN, queue_key: str) -> None:
        raise NotImplementedError

    def contains(self: PWN, queue_key: str) -> bool:
        raise NotImplementedError

    def clear_queue(self: PWN, queue_key: str) -> None:
        raise NotImplementedError

class RedisCacheBackend(CacheBackend):
    def __init__(self: PWN) -> None:
        self._client = bert_datasource.RedisConnection.ParseURL(bert_constants.REDIS_URL).client
        self._contains_key = 'redis-cache-backend'
        self._step = 200

    def _cache_key(self: PWN, key: str) -> str:
        return f'{self._contains_key}-{key}'

    def fill_cache(self: PWN, queue_key: str) -> None:
        cache_key = self._cache_key(queue_key)
        total = self._client.llen(queue_key)
        offset = 0
        while offset < total:
            next_step_result = self._client.lrange(queue_key, offset, offset + self._step)
            self._client.lpush(cache_key, *next_step_result)
            offset = offset + len(next_step_result)

        self._client.hset(self._contains_key, queue_key, 1)

    def fill_done_queue(self: PWN, queue_key: str) -> None:
        cache_key = self._cache_key(queue_key)
        total = self._client.llen(cache_key)
        offset = 0
        while offset < total:
            next_step_result = self._client.lrange(cache_key, offset, offset + self._step)
            self._client.lpush(queue_key, *next_step_result)
            offset = offset + len(next_step_result)

    def contains(self: PWN, queue_key: str) -> bool:
        value = self._client.hget(self._contains_key, queue_key)
        if value is None:
            return False

        return int(value) == 1

    def clear_queue(self: PWN, queue_key: str) -> None:
        self._client.delete(queue_key)
