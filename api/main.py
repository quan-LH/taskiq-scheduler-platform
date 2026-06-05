from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager
from broker import broker
from tasks.basic import add

# 定义生命周期管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时的初始化逻辑（替代 @app.on_event("startup")）
    await broker.startup()
    print("[API] Broker 已启动")
    
    yield  # 应用运行期间会停在这里
    
    # 关闭时的清理逻辑（替代 @app.on_event("shutdown")）
    await broker.shutdown()
    print("[API] Broker 已关闭")

# 将 lifespan 参数传给 FastAPI
app = FastAPI(title="TaskIQ调度平台", lifespan=lifespan)

class AddRequest(BaseModel):
    x: int
    y: int

@app.post("/tasks/add")
async def submit_add(req: AddRequest):
    job = await add.kiq(req.x, req.y)
    return {"task_id": job.task_id}

@app.get("/tasks/{task_id}")
async def get_result(task_id: str):
    result = await broker.result_backend.get_result(task_id)
    return {"task_id": task_id, "result": result}

@app.get("/health")
async def health():
    return {"status": "healthy"}
