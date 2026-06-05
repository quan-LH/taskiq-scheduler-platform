from taskiq_redis import RedisAsyncResultBackend, ListQueueBroker

REDIS_URL = "redis://localhost:6379"

broker = ListQueueBroker(
    REDIS_URL + "/0",
    result_backend=RedisAsyncResultBackend(REDIS_URL + "/1"),
)