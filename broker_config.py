"""
Broker 配置和所有任务定义
"""
from taskiq_redis import ListQueueBroker
from taskiq.middlewares import SimpleRetryMiddleware
import asyncio
import time
import random
import smtplib
import hashlib
import json
import redis
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ============ Redis 客户端（用于分布式锁） ============
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# ============ 分布式锁工具 ============

class DistributedLock:
    """基于 Redis 的分布式锁"""

    def __init__(self, key: str, expire: float = 30.0):
        self.key = f"lock:{key}"
        self.expire = expire
        self.lock_id = None

    def acquire(self) -> bool:
        """尝试获取锁"""
        self.lock_id = f"{time.time()}:{random.random()}"
        # SETNX + 过期时间
        acquired = redis_client.set(self.key, self.lock_id, nx=True, ex=int(self.expire))
        return bool(acquired)

    def release(self) -> bool:
        """释放锁"""
        if self.lock_id is None:
            return False
        # 使用 Lua 脚本保证原子性：只有持有锁才能释放
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            result = redis_client.eval(script, 1, self.key, self.lock_id)
            return bool(result)
        except Exception:
            return False

    async def __aenter__(self):
        """异步上下文管理器入口"""
        while not self.acquire():
            await asyncio.sleep(0.1)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        self.release()


def get_task_lock_key(task_name: str, *args, **kwargs) -> str:
    """生成任务锁的唯一键"""
    task_id = f"{task_name}:{json.dumps((args, kwargs), sort_keys=True)}"
    return hashlib.md5(task_id.encode()).hexdigest()


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


# ============ 超时测试任务 ============

@broker.task(timeout=3)
async def long_running_task(duration: float = 5.0) -> dict:
    """长时间运行任务 - 超时测试
    默认运行5秒，但超时设置3秒，应该超时失败
    """
    print(f"[Worker] 开始长时间任务，预计运行 {duration} 秒")
    await asyncio.sleep(duration)
    return {"status": "success", "duration": duration}


@broker.task(timeout=10)
async def normal_task() -> dict:
    """正常任务 - 不会被超时
    运行时间小于超时限制
    """
    await asyncio.sleep(2)
    return {"status": "success", "message": "正常完成任务"}


# ============ 分布式锁测试任务 ============

@broker.task
async def locked_task(task_id: str) -> dict:
    """
    带分布式锁的任务 - 防止重复执行
    相同 task_id 的任务在同一时间只能有一个在执行
    """
    lock_key = get_task_lock_key("locked_task", task_id)
    lock = DistributedLock(lock_key, expire=30.0)

    # 尝试获取锁，失败则跳过
    if not lock.acquire():
        print(f"[Worker] 任务 {task_id} 获取锁失败，跳过")
        return {"status": "skipped", "message": "任务正在执行中，跳过"}

    try:
        print(f"[Worker] 开始执行锁任务: {task_id}")
        await asyncio.sleep(5)  # 模拟耗时任务
        print(f"[Worker] 完成任务: {task_id}")
    finally:
        lock.release()

    return {"status": "success", "task_id": task_id}


@broker.task
async def unlocked_task(message: str) -> dict:
    """
    无锁任务 - 可以并发执行
    每次调用都会执行，不做并发控制
    """
    print(f"[Worker] 开始无锁任务: {message}")
    await asyncio.sleep(2)
    print(f"[Worker] 完成无锁任务: {message}")
    return {"status": "success", "message": message}
