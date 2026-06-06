from broker_config import broker
import asyncio
import time


# ============ 异步任务示例 ============

@broker.task
async def add(x: int, y: int) -> int:
    """异步加法任务"""
    await asyncio.sleep(0.1)
    print(f"[Worker] 异步加法: {x} + {y} = {x + y}")
    return x + y


@broker.task
async def multiply(x: int, y: int) -> int:
    """异步乘法任务"""
    await asyncio.sleep(0.2)
    print(f"[Worker] 异步乘法: {x} * {y} = {x * y}")
    return x * y


@broker.task
async def fetch_url(url: str) -> dict:
    """模拟IO任务 - 异步获取URL"""
    await asyncio.sleep(0.5)
    print(f"[Worker] 获取URL: {url}")
    return {"url": url, "status": "ok", "size": 1024}


@broker.task
async def process_data(data: list) -> dict:
    """异步数据处理任务"""
    await asyncio.sleep(0.3)
    result = sum(data)
    print(f"[Worker] 处理数据: sum({data}) = {result}")
    return {"data": data, "result": result, "count": len(data)}


# ============ 同步任务示例 ============

@broker.task
def sync_task(message: str) -> str:
    """同步任务 - CPU密集型"""
    time.sleep(0.1)
    print(f"[Worker] 同步任务: {message}")
    return f"处理完成: {message}"
