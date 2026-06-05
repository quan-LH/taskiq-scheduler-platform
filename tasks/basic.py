from broker import broker
import asyncio

@broker.task
async def add(x: int, y: int) -> int:
    await asyncio.sleep(0.1)
    print(f"[Worker] 加法: {x} + {y} = {x + y}")
    return x + y