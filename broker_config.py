"""
Broker 配置和所有任务定义
"""
from taskiq_redis import ListQueueBroker
from taskiq.middlewares import SimpleRetryMiddleware
import asyncio
import time
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ============ 邮件配置 ============
SMTP_SERVER = "smtp.qq.com"
SMTP_PORT = 587
SENDER_EMAIL = "3498492554@qq.com"
SENDER_AUTH_CODE = "psjkyocjkgzjdbfd"

# ============ Redis Broker ============
broker = ListQueueBroker("redis://localhost:6379")

# ============ 注册中间件 ============
broker = broker.with_middlewares(
    SimpleRetryMiddleware(default_retry_count=3)
)

# ============ 基础任务 ============

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
    """模拟IO任务"""
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


@broker.task
def sync_task(message: str) -> str:
    """同步任务"""
    time.sleep(0.1)
    print(f"[Worker] 同步任务: {message}")
    return f"处理完成: {message}"


# ============ 邮件任务 ============

@broker.task
async def send_email(to_email: str, subject: str, body: str, body_type: str = "plain") -> dict:
    """异步发送邮件任务"""
    message = MIMEMultipart()
    message["From"] = SENDER_EMAIL
    message["To"] = to_email
    message["Subject"] = subject
    message.attach(MIMEText(body, body_type, "utf-8"))

    try:
        print(f"[Email] 正在发送邮件到: {to_email}")

        def _send():
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
                server.starttls()
                server.login(SENDER_EMAIL, SENDER_AUTH_CODE)
                server.send_message(message)

        await asyncio.to_thread(_send)
        print(f"[Email] 邮件发送成功: {to_email}")
        return {"status": "success", "to": to_email, "subject": subject}
    except Exception as e:
        print(f"[Email] 发送失败: {e}")
        return {"status": "error", "message": str(e)}


@broker.task
async def send_batch_emails(recipients: list, subject: str, body: str) -> dict:
    """批量发送邮件任务"""
    success_count = 0
    for recipient in recipients:
        result = await send_email(recipient, subject, body)
        if result.get("status") == "success":
            success_count += 1
        await asyncio.sleep(0.5)
    return {"total": len(recipients), "success": success_count}


# ============ 重试测试任务 ============

@broker.task(retry_on_error=True, max_retries=3)
async def flaky_task() -> dict:
    """模拟不稳定任务 - 50%概率失败"""
    if random.random() < 0.5:
        raise ValueError("模拟随机失败")
    return {"status": "success", "message": "任务执行成功"}


@broker.task(retry_on_error=True, max_retries=3)
async def always_fail_task() -> dict:
    """永远失败的任务"""
    raise RuntimeError("这个任务永远会失败")


@broker.task
async def retry_counter() -> dict:
    """重试计数器"""
    if not hasattr(retry_counter, "_count"):
        retry_counter._count = 0
    retry_counter._count += 1
    return {"retry_count": retry_counter._count}


@broker.task
async def delayed_success(delay: float = 2.0) -> dict:
    """延迟成功任务"""
    await asyncio.sleep(delay)
    return {"status": "success", "delay": delay}
