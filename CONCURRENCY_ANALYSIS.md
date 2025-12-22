# 并发阻塞问题分析报告

## 问题概述

当前项目使用 `uvicorn app:app --reload` 启动，存在以下并发阻塞问题：

### 1. uvicorn 未启用多进程

**当前配置：**
- README.md: `uvicorn app:app --reload`
- Dockerfile: `uvicorn app:app --host 0.0.0.0 --port 8000`

**问题：**
- 没有使用 `--workers` 参数，uvicorn 以单进程模式运行
- 单进程模式下，所有请求共享同一个事件循环
- 一个长时间运行的请求会阻塞其他所有请求

### 2. 同步阻塞操作在异步端点中

**问题代码位置：** `app.py` 第 452-507 行

```python
@app.post("/api/creators/{creator_id}/generate")
async def generate_creator(creator_id: str):
    # ...
    executor = TaskExecutor()
    # 这是一个同步阻塞操作！
    result = executor.execute_all_steps(
        creator_id=creator_id,
        max_workers=1,
        supabase_url=supabase_url,
        supabase_api_key=supabase_api_key
    )
```

**问题分析：**
- 虽然端点函数使用了 `async def`，但 `executor.execute_all_steps()` 是同步方法
- 同步操作会阻塞整个事件循环，导致其他请求无法处理
- `TaskExecutor.execute_all_steps()` 可能需要几分钟才能完成

### 3. 其他潜在的阻塞操作

检查其他端点：
- `/api/creators` - 使用 `requests.get()` 同步调用（第 256 行）
- `/api/creators/{creator_id}/images` - 使用 `requests.get()` 同步调用（第 256 行）
- `/api/send-email` - 使用 `smtplib` 同步操作（第 424-428 行）

## 影响

1. **并发性能差**：多个用户同时访问时，请求会排队等待
2. **用户体验差**：一个长时间任务会阻塞所有其他请求
3. **资源利用率低**：单进程无法充分利用多核 CPU

## 解决方案

### 方案 1：启用 uvicorn 多进程（推荐用于生产环境）

**优点：**
- 简单，只需修改启动命令
- 可以充分利用多核 CPU
- 进程隔离，一个进程崩溃不影响其他进程

**缺点：**
- 进程间不共享内存（但可以通过数据库/缓存解决）
- 开发环境使用 `--reload` 时，多进程可能导致文件监控问题

**实现：**
```bash
# 生产环境
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4

# 开发环境（单进程，但可以手动设置）
uvicorn app:app --reload --workers 1
```

### 方案 2：将同步操作改为异步（推荐用于开发环境）

**优点：**
- 不阻塞事件循环
- 可以处理更多并发请求
- 适合 I/O 密集型操作

**缺点：**
- 需要重构代码，将同步操作改为异步
- CPU 密集型任务仍然可能阻塞

**实现：**
使用 `asyncio.to_thread()` 或 `run_in_executor()` 在线程池中执行同步操作：

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

@app.post("/api/creators/{creator_id}/generate")
async def generate_creator(creator_id: str):
    # ...
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(max_workers=1)
    
    # 在线程池中执行同步操作
    result = await loop.run_in_executor(
        executor,
        lambda: TaskExecutor().execute_all_steps(
            creator_id=creator_id,
            max_workers=1,
            supabase_url=supabase_url,
            supabase_api_key=supabase_api_key
        )
    )
```

### 方案 3：使用后台任务队列（推荐用于长时间任务）

**优点：**
- 请求立即返回，不阻塞
- 可以跟踪任务状态
- 适合长时间运行的任务

**缺点：**
- 需要额外的任务队列系统（如 Celery、RQ）
- 实现复杂度较高

## 推荐方案

**生产环境：** 方案 1（多进程）+ 方案 2（异步化关键端点）
**开发环境：** 方案 2（异步化）

## 需要修复的文件

1. `app.py` - 将同步操作改为异步
2. `README.md` - 更新启动命令说明
3. `Dockerfile` - 添加 `--workers` 参数（可选）



