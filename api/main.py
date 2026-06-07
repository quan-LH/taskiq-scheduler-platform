from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from contextlib import asynccontextmanager
from broker_config import broker


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.startup()
    print("[API] Broker 已启动")
    yield
    await broker.shutdown()
    print("[API] Broker 已关闭")


app = FastAPI(title="TaskIQ 调度平台", lifespan=lifespan)


# ============ 基础任务 API ============

class TaskRequest(BaseModel):
    x: int
    y: int


class MessageRequest(BaseModel):
    message: str


class UrlRequest(BaseModel):
    url: str


class DataRequest(BaseModel):
    data: list[int]


@app.post("/tasks/add")
async def submit_add(req: TaskRequest):
    from broker_config import add
    job = await add.kiq(req.x, req.y)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/multiply")
async def submit_multiply(req: TaskRequest):
    from broker_config import multiply
    job = await multiply.kiq(req.x, req.y)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/sync")
async def submit_sync(req: MessageRequest):
    from broker_config import sync_task
    job = await sync_task.kiq(req.message)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/fetch")
async def submit_fetch(req: UrlRequest):
    from broker_config import fetch_url
    job = await fetch_url.kiq(req.url)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/process")
async def submit_process(req: DataRequest):
    from broker_config import process_data
    job = await process_data.kiq(req.data)
    return {"task_id": job.task_id, "status": "submitted"}


# ============ 邮件 API ============

class EmailRequest(BaseModel):
    to_email: EmailStr
    subject: str
    body: str
    body_type: str = "plain"


class BatchEmailRequest(BaseModel):
    recipients: list[EmailStr]
    subject: str
    body: str


@app.post("/tasks/send_email", summary="发送单封邮件")
async def submit_email(req: EmailRequest):
    from broker_config import send_email
    job = await send_email.kiq(req.to_email, req.subject, req.body, req.body_type)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/send_batch_emails", summary="批量发送邮件")
async def submit_batch_emails(req: BatchEmailRequest):
    from broker_config import send_batch_emails
    job = await send_batch_emails.kiq(req.recipients, req.subject, req.body)
    return {"task_id": job.task_id, "status": "submitted", "count": len(req.recipients)}


# ============ 重试测试 API ============

@app.post("/tasks/flaky", summary="模拟不稳定任务")
async def submit_flaky():
    from broker_config import flaky_task
    job = await flaky_task.kiq()
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/always_fail", summary="永远失败的任务")
async def submit_always_fail():
    from broker_config import always_fail_task
    job = await always_fail_task.kiq()
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/retry_counter", summary="重试计数器")
async def submit_retry_counter():
    from broker_config import retry_counter
    job = await retry_counter.kiq()
    return {"task_id": job.task_id, "status": "submitted"}


class DelayedTaskRequest(BaseModel):
    delay: float = 2.0


@app.post("/tasks/delayed", summary="延迟成功任务")
async def submit_delayed(req: DelayedTaskRequest):
    from broker_config import delayed_success
    job = await delayed_success.kiq(req.delay)
    return {"task_id": job.task_id, "status": "submitted"}


# ============ 超时测试 API ============

class LongRunningRequest(BaseModel):
    duration: float = 5.0


@app.post("/tasks/long_running", summary="长时间任务（测试超时）")
async def submit_long_running(req: LongRunningRequest):
    """
    提交长时间运行任务
    - 默认运行5秒，超时设置3秒，会超时失败
    """
    from broker_config import long_running_task
    job = await long_running_task.kiq(req.duration)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/normal", summary="正常任务（不会超时）")
async def submit_normal():
    """
    提交正常任务
    - 运行2秒，超时设置10秒，不会超时
    """
    from broker_config import normal_task
    job = await normal_task.kiq()
    return {"task_id": job.task_id, "status": "submitted"}


# ============ 分布式锁测试 API ============

class LockedTaskRequest(BaseModel):
    task_id: str


@app.post("/tasks/locked", summary="带锁任务（防重复执行）")
async def submit_locked_task(req: LockedTaskRequest):
    """
    提交带分布式锁的任务
    - 相同 task_id 的任务同一时间只能执行一个
    - 运行5秒，测试并发控制
    """
    from broker_config import locked_task
    job = await locked_task.kiq(req.task_id)
    return {"task_id": job.task_id, "status": "submitted"}


class UnlockedTaskRequest(BaseModel):
    message: str


@app.post("/tasks/unlocked", summary="无锁任务（可并发）")
async def submit_unlocked_task(req: UnlockedTaskRequest):
    """
    提交无锁任务
    - 可以并发执行，无并发控制
    - 运行2秒
    """
    from broker_config import unlocked_task
    job = await unlocked_task.kiq(req.message)
    return {"task_id": job.task_id, "status": "submitted"}


# ============ 通用 API ============

@app.get("/tasks/{task_id}")
async def get_result(task_id: str):
    try:
        result = await broker.result_backend.get_result(task_id)
        if result is None:
            return {"task_id": task_id, "status": "pending", "result": None}
        return {
            "task_id": task_id,
            "is_err": result.is_err,
            "result": result.return_value,
            "error": result.error,
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/tasks/{task_id}/status")
async def get_status(task_id: str):
    try:
        result = await broker.result_backend.get_result(task_id)
        if result is None:
            return {"task_id": task_id, "status": "pending"}
        return {"task_id": task_id, "is_err": result.is_err}
    except Exception:
        return {"task_id": task_id, "status": "pending"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/")
async def root():
    return {"name": "TaskIQ 调度平台", "version": "1.0.0", "docs": "/docs"}
