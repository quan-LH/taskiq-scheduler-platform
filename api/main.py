from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr  # EmailStr 用于邮箱格式验证
from contextlib import asynccontextmanager
from broker import broker
from tasks.basic import add, multiply, sync_task, fetch_url, process_data
from tasks.email_tasks import send_email, send_batch_emails  # 邮件任务（新增）


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.startup()
    print("[API] Broker 已启动")
    yield
    await broker.shutdown()
    print("[API] Broker 已关闭")


app = FastAPI(title="TaskIQ 调度平台", lifespan=lifespan)


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
    job = await add.kiq(req.x, req.y)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/multiply")
async def submit_multiply(req: TaskRequest):
    job = await multiply.kiq(req.x, req.y)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/sync")
async def submit_sync(req: MessageRequest):
    job = await sync_task.kiq(req.message)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/fetch")
async def submit_fetch(req: UrlRequest):
    job = await fetch_url.kiq(req.url)
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/process")
async def submit_process(req: DataRequest):
    job = await process_data.kiq(req.data)
    return {"task_id": job.task_id, "status": "submitted"}


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


# ============ 邮件发送 API（新增）============

class EmailRequest(BaseModel):
    """单封邮件请求模型"""
    to_email: EmailStr          # 收件人邮箱（自动验证格式）
    subject: str                # 邮件主题
    body: str                   # 邮件正文
    body_type: str = "plain"    # 正文类型: plain 或 html


class BatchEmailRequest(BaseModel):
    """批量邮件请求模型"""
    recipients: list[EmailStr]  # 收件人列表（自动验证格式）
    subject: str                # 邮件主题
    body: str                   # 邮件正文


@app.post("/tasks/send_email", summary="发送单封邮件")
async def submit_email(req: EmailRequest):
    """
    提交发送单封邮件任务

    - to_email: 收件人邮箱地址
    - subject: 邮件主题
    - body: 邮件正文内容
    - body_type: plain(普通文本) 或 html(网页格式)
    """
    job = await send_email.kiq(
        to_email=req.to_email,
        subject=req.subject,
        body=req.body,
        body_type=req.body_type
    )
    return {"task_id": job.task_id, "status": "submitted"}


@app.post("/tasks/send_batch_emails", summary="批量发送邮件")
async def submit_batch_emails(req: BatchEmailRequest):
    """
    提交批量发送邮件任务

    - recipients: 收件人邮箱列表
    - subject: 邮件主题（所有收件人收到相同主题）
    - body: 邮件正文
    """
    job = await send_batch_emails.kiq(
        recipients=req.recipients,
        subject=req.subject,
        body=req.body
    )
    return {"task_id": job.task_id, "status": "submitted", "count": len(req.recipients)}
