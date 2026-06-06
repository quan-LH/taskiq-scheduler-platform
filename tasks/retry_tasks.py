"""
重试测试任务模块 - 演示 TaskIQ 内置重试中间件功能
"""

from broker_config import broker
import asyncio
import random


# ============ 重试测试任务 ============

@broker.task(retry_on_error=True, max_retries=3)
async def flaky_task() -> dict:
    """
    模拟不稳定任务 - 有概率失败

    使用 retry_on_error=True 启用重试
    max_retries=3 表示最多重试3次
    """
    # 50%概率失败
    if random.random() < 0.5:
        raise ValueError("模拟随机失败")

    return {
        "status": "success",
        "message": "任务执行成功"
    }


@broker.task(retry_on_error=True, max_retries=3)
async def always_fail_task() -> dict:
    """
    永远失败的任务 - 用于测试重试机制

    Returns:
        抛出异常
    """
    raise RuntimeError("这个任务永远会失败，用于测试重试机制")


@broker.task
async def retry_counter() -> dict:
    """
    重试计数器 - 记录被执行的次数

    Returns:
        当前执行计数
    """
    # 使用全局变量模拟（实际应该用 Redis 存储）
    if not hasattr(retry_counter, "_count"):
        retry_counter._count = 0
    retry_counter._count += 1

    return {
        "retry_count": retry_counter._count,
        "message": f"任务被执行了 {retry_counter._count} 次"
    }


@broker.task
async def delayed_success(delay: float = 2.0) -> dict:
    """
    延迟成功任务 - 模拟耗时任务

    Args:
        delay: 延迟秒数

    Returns:
        执行结果
    """
    await asyncio.sleep(delay)
    return {
        "status": "success",
        "delay": delay,
        "message": f"任务在 {delay} 秒后成功完成"
    }
