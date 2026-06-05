from taskiq import InMemoryBroker

# 内存模式 Broker - 开发测试用，无需 Redis/PostgreSQL
broker = InMemoryBroker()
