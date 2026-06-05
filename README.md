

# TaskIQ 调度平台

基于 TaskIQ + Redis 的轻量级分布式任务调度平台。

## 技术栈

- Python 3.10+
- TaskIQ - 异步分布式任务队列
- Redis - 消息队列与结果存储
- FastAPI - REST API 管理接口

## 快速启动

```bash
# 安装依赖
pip install taskiq taskiq-redis fastapi uvicorn pydantic redis

# 启动 Redis
redis-server

# 启动 Worker
taskiq worker broker:broker --reload --fs-discover

# 启动调度器
taskiq scheduler scheduler:scheduler --reload

# 启动 API
uvicorn api.main:app --reload --port 8000 --host 0.0.0.0