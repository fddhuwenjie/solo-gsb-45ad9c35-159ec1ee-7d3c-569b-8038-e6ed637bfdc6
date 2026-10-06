"""FastAPI 入口：推演 API + 调度对照 API + 严格两阶段锁推演 API + 静态页面。"""

from pathlib import Path
from typing import List, Union

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .s2pl import run_s2pl
from .simulator import SimError, run_all, run_compare

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="事务交错推演", version="1.0.0")


class SimulateRequest(BaseModel):
    initial: str = Field("", description="初始键值，每行 key=value")
    txns: List[str] = Field(..., description="2~3 条事务脚本")


class CompareRequest(BaseModel):
    initial: str = Field("", description="初始键值，每行 key=value")
    txns: List[str] = Field(..., description="2~3 条事务脚本")
    order: Union[str, List[str]] = Field(
        ..., description="自定义执行顺序：T1/T2/T3 组成的序列，字符串或数组"
    )


@app.exception_handler(SimError)
async def sim_error_handler(_request, exc: SimError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.get("/api/health")
async def health():
    return {"ok": True}


@app.post("/api/simulate")
async def simulate(req: SimulateRequest):
    if len(req.txns) != len([t for t in req.txns if t.strip()]):
        raise HTTPException(status_code=400, detail="事务脚本不能为空")
    try:
        return run_all(req.initial, req.txns)
    except SimError as exc:
        raise SimError(str(exc))


@app.post("/api/compare")
async def compare(req: CompareRequest):
    if len(req.txns) != len([t for t in req.txns if t.strip()]):
        raise HTTPException(status_code=400, detail="事务脚本不能为空")
    try:
        return run_compare(req.initial, req.txns, req.order)
    except SimError as exc:
        raise SimError(str(exc))


@app.post("/api/s2pl")
async def s2pl(req: SimulateRequest):
    if len(req.txns) != len([t for t in req.txns if t.strip()]):
        raise HTTPException(status_code=400, detail="事务脚本不能为空")
    try:
        return run_s2pl(req.initial, req.txns)
    except SimError as exc:
        raise SimError(str(exc))


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")
