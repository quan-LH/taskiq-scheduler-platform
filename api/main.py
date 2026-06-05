from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from broker import broker
from tasks.basic import add, multiply, sync_task, fetch_url, process_data


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
