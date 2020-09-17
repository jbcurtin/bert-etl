import typing

from bert import \
        datasource as bert_datasource, \
        utils as bert_utils, \
        constants as bert_constants

PWN = typing.TypeVar('PWN')

class CacheBackend:
    def __init__(self: PWN, work_tablename: str, done_tablename: str) -> None:
        self._work_tablename = work_tablename
        self._done_tablename = done_tablename

    def fill_done_queue(self: PWN) -> None:
        raise NotImplementedError

    def fill_cache(self: PWN, max_fill: int = 0) -> None:
        raise NotImplementedError

    def contains(self: PWN) -> bool:
        raise NotImplementedError

    def clear_queue(self: PWN) -> None:
        raise NotImplementedError

class RedisCacheBackend(CacheBackend):
    def __init__(self: PWN, *args, **kwargs) -> None:
        super(RedisCacheBackend, self).__init__(*args, **kwargs)
        self._client = bert_datasource.RedisConnection.ParseURL(bert_constants.REDIS_URL).client
        self._contains_key = 'redis-cache-backend'
        self._step = 20000

    def _cache_key(self: PWN, key: str) -> str:
        return f'{self._contains_key}-{key}'

    # Cache specific
    # Fill cache from Queue tablename
    def fill_cache(self: PWN, queue_name: str, max_fill: int = 0) -> None:
        cache_key = self._cache_key(queue_name)
        total = self._client.llen(queue_name)
        offset = 0
        while offset < total:
            next_step_result = self._client.lrange(queue_name, offset, offset + self._step)
            self._client.lpush(cache_key, *next_step_result)
            offset = offset + len(next_step_result)
            if max_fill > 0 and offset >= max_fill:
                break

    def fill_cache_from_done_queue(self: PWN, max_fill: int = 0) -> None:
        self.fill_cache(self._done_tablename)

    def fill_cache_from_work_queue(self: PWN, max_fill: int = 0) -> None:
        self.fill_cache(self._work_tablename)

    # Clear Caches from Queue Tablename
    def clear_work_queue_cache(self: PWN) -> None:
        cache_key = self._cache_key(self._done_tablename)
        self._client.delete(cache_key)

    def clear_done_queue_cache(self: PWN) -> None:
        cache_key = self._cache_key(self._work_tablename)
        self._client.delete(cache_key)

    # Table specific
    # Fill queues from cache
    def fill_queue(self: PWN, queue_name: str, max_fill: int = 0) -> None:
        cache_key = self._cache_key(queue_name)
        total = self._client.llen(cache_key)
        offset = 0
        while offset < total:
            next_step_result = self._client.lrange(cache_key, offset, offset + self._step)
            if len(next_step_result) + offset > max_fill:
                diff_fill = len(next_step_result) + offset - max_fill
                diff_fill = len(next_step_result) - diff_fill
                self._client.lpush(queue_name, *next_step_result[:diff_fill])
                break

            else:
                self._client.lpush(queue_name, *next_step_result)

            offset = offset + len(next_step_result)
            if max_fill > 0 and offset > max_fill:
                break

    def fill_done_queue_from_cache(self: PWN, max_fill: int = 0) -> None:
        return self.fill_queue(self._done_tablename, max_fill)

    def fill_work_queue_from_cache(self: PWN, max_fill: int = 0) -> None:
        return self.fill_queue(self._work_tablename, max_fill)

    # Clear queues
    def clear_done_queue(self: PWN) -> None:
        self._client.delete(self._done_tablename)

    def clear_work_queue(self: PWN) -> None:
        self._client.delete(self._work_tablename)


    # Count functions
    def done_queue_cache_size(self: PWN) -> int:
        cache_key = self._cache_key(self._done_tablename)
        return int(self._client.llen(cache_key))

    def work_queue_cache_size(self: PWN) -> int:
        cache_key = self._cache_key(self._work_tablename)
        return int(self._client.llen(cache_key))

    def done_queue_size(self: PWN) -> int:
        return self._client.llen(self._done_tablename)

    def work_queue_size(self: PWN) -> int:
        return self._client.llen(self._work_tablename)

