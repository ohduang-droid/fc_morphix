import os
import smtplib
import requests
import logging
import uuid
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

from imagetoimage2 import generate_images_to_s3
from imagetovideowithextending import generate_multisegment_videos_to_s3
from steps import step_one
from services.excel_importer import import_creators_from_excel, import_creators_from_json, ExcelImportError
from task_executor import TaskExecutor


def load_env_file(path: str = ".env"):
    """Load environment variables from a .env file (supports `export KEY=value`).
    
    First tries the provided path, then tries relative to the script directory,
    then tries the current working directory.
    """
    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 尝试多个可能的路径
    possible_paths = [
        path,  # 用户提供的路径
        os.path.join(script_dir, path),  # 相对于脚本目录
        os.path.join(os.getcwd(), path),  # 相对于当前工作目录
    ]
    
    env_file = None
    for p in possible_paths:
        if os.path.exists(p):
            env_file = p
            break
    
    if not env_file:
        return

    with open(env_file, "r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :]
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"\'')
            os.environ[key] = value


# 在应用启动时加载 .env 文件
load_env_file()


class ImageToImageRequest(BaseModel):
    prompt: str
    image_url: Optional[str] = None
    image_urls: Optional[List[str]] = None
    bucket: Optional[str] = None
    key_prefix: Optional[str] = None
    model: Optional[str] = None


class ImageToImageResponse(BaseModel):
    urls: List[str]
    texts: List[str]


class ImageToVideoRequest(BaseModel):
    segment_prompts: Optional[List[str]] = None
    image_prompts: Optional[List[str]] = None
    image_urls: Optional[List[str]] = None
    bucket: Optional[str] = None
    key_prefix: Optional[str] = None
    poll_interval: Optional[int] = None
    max_retries: Optional[int] = None


class ImageToVideoResponse(BaseModel):
    url: str


class CreatorResponse(BaseModel):
    creators: List[Dict[str, Any]]
    count: int
    stats: Optional[Dict[str, int]] = None


class SendEmailRequest(BaseModel):
    creator_id: str
    to_email: str
    subject: Optional[str] = None
    body: str


app = FastAPI(title="FC Morphix API", version="1.0.0")

# 创建静态文件目录（如果不存在）
static_dir = "static"
if not os.path.exists(static_dir):
    os.makedirs(static_dir)

# 挂载静态文件服务
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.post("/image-to-image", response_model=ImageToImageResponse)
async def image_to_image(payload: ImageToImageRequest):
    """
    Generate images using Gemini with an input prompt + reference image URL,
    upload them to S3, and return the accessible URLs.
    """
    try:
        image_urls: List[str] = []
        if payload.image_urls:
            image_urls = payload.image_urls
        elif payload.image_url:
            image_urls = [payload.image_url]

        if not image_urls:
            raise HTTPException(status_code=400, detail="At least one image URL is required.")

        result = generate_images_to_s3(
            prompt=payload.prompt,
            image_urls=image_urls,
            bucket=payload.bucket,
            key_prefix=payload.key_prefix,
            model=payload.model,
        )
        return ImageToImageResponse(urls=result["urls"], texts=result["texts"])
    except Exception as exc:  # pragma: no cover - FastAPI handles formatting
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/image-to-video", response_model=ImageToVideoResponse)
async def image_to_video(payload: ImageToVideoRequest):
    """
    Generate multi-segment Veo videos using prompts + optional reference images,
    upload each segment to S3, and return their URLs.
    """
    try:
        final_url = generate_multisegment_videos_to_s3(
            segment_prompts=payload.segment_prompts,
            image_prompts=payload.image_prompts,
            image_urls=payload.image_urls,
            bucket=payload.bucket,
            key_prefix=payload.key_prefix,
            poll_interval=payload.poll_interval,
            max_retries=payload.max_retries,
        )
        return ImageToVideoResponse(url=final_url)
    except Exception as exc:  # pragma: no cover - FastAPI handles formatting
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/creators", response_model=CreatorResponse)
async def get_creators():
    """
    从 Supabase 获取所有 creator 列表
    
    修复：使用线程池执行同步操作，避免阻塞事件循环
    """
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_api_key = os.getenv("SUPABASE_API_KEY")
        
        if not supabase_url or not supabase_api_key:
            raise HTTPException(
                status_code=500,
                detail="缺少 Supabase 配置（SUPABASE_URL 或 SUPABASE_API_KEY 环境变量）"
            )
        
        # 在线程池中执行同步操作，避免阻塞事件循环
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: step_one.execute(
                supabase_url=supabase_url,
                supabase_api_key=supabase_api_key,
                use_cache=False  # 实时从数据库获取，不使用缓存
            )
        )
        
        creators = result.get("creators", [])
        
        # 统计各状态的数量
        stats = {
            "pending": 0,      # 待生成
            "completed": 0,    # 已完成
            "failed": 0        # 失败
        }
        
        for creator in creators:
            status = creator.get("status", "pending")  # 默认为 pending
            if status == "completed":
                stats["completed"] += 1
            elif status == "failed":
                stats["failed"] += 1
            else:
                # pending, generating 或其他状态都算作 pending
                stats["pending"] += 1
        
        return CreatorResponse(
            creators=creators,
            count=result.get("count", 0),
            stats=stats
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/creators/{creator_id}/images")
async def get_creator_images(creator_id: str):
    """
    获取指定 creator 的所有 magnet_image 图片
    
    重要：此接口禁止使用缓存，必须直接从 Supabase 数据库获取最新数据
    """
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_api_key = os.getenv("SUPABASE_API_KEY")
        
        if not supabase_url or not supabase_api_key:
            raise HTTPException(
                status_code=500,
                detail="缺少 Supabase 配置（SUPABASE_URL 或 SUPABASE_API_KEY 环境变量）"
            )
        
        # 重要：直接从数据库获取，不使用任何缓存
        logger.info(f"从数据库获取 creator_id={creator_id} 的图片数据（不使用缓存）")
        
        # 构建 API URL 查询 magnet_image 表
        api_url = f"{supabase_url.rstrip('/')}/rest/v1/magnet_image"
        query_url = f"{api_url}?creator_id=eq.{creator_id}&select=*"
        
        headers = {
            "apikey": supabase_api_key,
            "Authorization": f"Bearer {supabase_api_key}",
            "Content-Type": "application/json"
        }
        
        # 直接从 Supabase 数据库获取数据
        # 使用线程池执行同步的 requests.get，避免阻塞事件循环
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: requests.get(query_url, headers=headers, timeout=300)
        )
        response.raise_for_status()
        
        magnet_images = response.json()
        logger.info(f"从数据库获取到 {len(magnet_images)} 条 magnet_image 记录（creator_id={creator_id}）")
        
        # 提取所有图片 URL（包括 front_image_url 为 null 的记录，用于调试）
        all_images = []
        records_without_image = []
        for magnet in magnet_images:
            front_image_url = magnet.get("front_image_url")
            context_id = magnet.get("context_id", "")
            front_name = magnet.get("front_name", "")
            
            if front_image_url:
                all_images.append({
                    "url": front_image_url,
                    "context_id": context_id,
                    "front_name": front_name
                })
            else:
                # 记录没有图片的记录，用于调试
                records_without_image.append({
                    "context_id": context_id,
                    "front_name": front_name,
                    "generation_status": magnet.get("generation_status", "unknown"),
                    "error_message": magnet.get("error_message"),
                    "front_image_prompt": magnet.get("front_image_prompt")[:100] + "..." if magnet.get("front_image_prompt") and len(magnet.get("front_image_prompt", "")) > 100 else magnet.get("front_image_prompt")
                })
        
        result = {
            "creator_id": creator_id,
            "images": all_images,
            "count": len(all_images),
            "total_records": len(magnet_images),
            "records_without_image": len(records_without_image)
        }
        
        # 如果有记录但没有图片，添加调试信息
        if len(magnet_images) > 0 and len(all_images) == 0:
            result["debug_info"] = {
                "message": f"找到 {len(magnet_images)} 个 magnet 记录，但都没有 front_image_url。可能需要执行 Step Three 来生成图片。",
                "records_without_image": records_without_image
            }
        
        return result
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=f"从 Supabase 获取图片失败: {str(e)}"
        ) from e
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/import-creators")
async def import_creators():
    """
    从 output.json 文件导入 Creator 数据
    导入成功的结果会写入缓存，避免多次执行时重复导入
    """
    try:
        # 获取 Supabase 配置
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_api_key = os.getenv("SUPABASE_API_KEY")
        
        if not supabase_url or not supabase_api_key:
            raise HTTPException(
                status_code=500,
                detail="缺少 Supabase 配置（SUPABASE_URL 或 SUPABASE_API_KEY 环境变量）"
            )
        
        # 获取 output.json 文件路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        json_file_path = os.path.join(script_dir, "json", "creator", "output.json")
        
        # 检查文件是否存在
        if not os.path.exists(json_file_path):
            raise HTTPException(
                status_code=404,
                detail=f"文件不存在: {json_file_path}"
            )
        
        # 调用 JSON 导入服务
        # 获取 Dify 配置（可选）
        dify_url = os.getenv("DIFY_URL")
        dify_user = os.getenv("DIFY_USER", "excel-importer")
        
        # 获取 S3 配置（可选）
        s3_bucket = os.getenv("S3_BUCKET") or os.getenv("S3_BUCKET_NAME")
        s3_key_prefix = os.getenv("S3_CREATOR_PREFIX")
        
        result = import_creators_from_json(
            json_file_path=json_file_path,
            supabase_url=supabase_url,
            supabase_api_key=supabase_api_key,
            dify_url=dify_url,
            dify_user=dify_user,
            s3_bucket=s3_bucket,
            s3_key_prefix=s3_key_prefix
        )
        
        return result
        
    except ExcelImportError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as exc:
        import traceback
        error_detail = f"导入失败: {str(exc)}"
        # 在开发环境中返回详细错误信息
        if os.getenv("DEBUG", "false").lower() == "true":
            error_detail += f"\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail) from exc


@app.post("/api/send-email")
async def send_email(payload: SendEmailRequest):
    """
    发送邮件给 Creator
    """
    try:
        # 从环境变量获取 SMTP 配置
        smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER")
        smtp_password = os.getenv("SMTP_PASSWORD")
        smtp_from = os.getenv("SMTP_FROM", smtp_user)
        
        if not smtp_user or not smtp_password:
            # 提供详细的调试信息
            script_dir = os.path.dirname(os.path.abspath(__file__))
            cwd = os.getcwd()
            env_file_paths = [
                ".env",
                os.path.join(script_dir, ".env"),
                os.path.join(cwd, ".env"),
            ]
            existing_paths = [p for p in env_file_paths if os.path.exists(p)]
            
            error_detail = "缺少 SMTP 配置（SMTP_USER 或 SMTP_PASSWORD 环境变量）\n"
            error_detail += f"脚本目录: {script_dir}\n"
            error_detail += f"当前工作目录: {cwd}\n"
            if existing_paths:
                error_detail += f"找到的 .env 文件: {', '.join(existing_paths)}\n"
            else:
                error_detail += "未找到 .env 文件（已检查: " + ", ".join(env_file_paths) + "）\n"
            error_detail += f"SMTP_USER: {'已设置' if smtp_user else '未设置'}\n"
            error_detail += f"SMTP_PASSWORD: {'已设置' if smtp_password else '未设置'}"
            
            raise HTTPException(
                status_code=500,
                detail=error_detail
            )
        
        # 创建邮件
        msg = MIMEMultipart()
        msg['From'] = smtp_from
        msg['To'] = payload.to_email
        msg['Subject'] = payload.subject or "Fridge Channel Magnet 合作邀请"
        
        # 将邮件正文转换为 HTML 格式（保留换行）
        body_html = payload.body.replace('\n', '<br>')
        msg.attach(MIMEText(body_html, 'html', 'utf-8'))
        
        # 发送邮件
        # 使用线程池执行同步的 SMTP 操作，避免阻塞事件循环
        try:
            def send_email_sync():
                server = smtplib.SMTP(smtp_host, smtp_port)
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
                server.quit()
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, send_email_sync)
            
            return {
                "status": "success",
                "message": f"邮件已成功发送到 {payload.to_email}",
                "creator_id": payload.creator_id
            }
        except smtplib.SMTPException as e:
            raise HTTPException(
                status_code=500,
                detail=f"SMTP 错误: {str(e)}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"发送邮件失败: {str(e)}"
            )
            
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/creators/{creator_id}/generate")
async def generate_creator(creator_id: str):
    """
    为指定的 Creator 执行生成任务（步骤 1-4）
    注意：此操作可能需要较长时间（几分钟），请耐心等待
    
    重要：此接口不使用任何缓存，所有步骤（1-4）都会完全重新执行
    - 步骤1：从 Supabase 重新获取 Creator 信息
    - 步骤2：重新调用 Dify API 生成 prompt
    - 步骤3：重新生成所有 magnet 图片
    - 步骤4：重新生成场景图
    
    修复：使用线程池执行同步操作，避免阻塞事件循环
    """
    try:
        # 获取 Supabase 配置
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_api_key = os.getenv("SUPABASE_API_KEY")
        
        if not supabase_url or not supabase_api_key:
            raise HTTPException(
                status_code=500,
                detail="缺少 Supabase 配置（SUPABASE_URL 或 SUPABASE_API_KEY 环境变量）"
            )
        
        logger.info(f"开始为 Creator {creator_id} 执行生成任务")
        
        # 在线程池中执行同步操作，避免阻塞事件循环
        loop = asyncio.get_event_loop()
        # 使用线程池执行同步的 TaskExecutor
        # 重要：设置 use_cache=False，确保步骤1-4完全重新执行，不使用任何缓存
        result = await loop.run_in_executor(
            None,  # 使用默认线程池
            lambda: TaskExecutor().execute_all_steps(
                creator_id=creator_id,
                max_workers=1,  # 单个 Creator 使用单线程
                supabase_url=supabase_url,
                supabase_api_key=supabase_api_key,
                use_cache=False  # 禁用缓存，完全重新执行所有步骤
            )
        )
        
        if result["status"] == "success":
            logger.info(f"Creator {creator_id} 生成任务完成")
            return {
                "status": "success",
                "message": f"Creator {creator_id} 生成完成",
                "creator_id": creator_id,
                "result": result
            }
        else:
            error_msg = result.get("error", "生成失败")
            logger.error(f"Creator {creator_id} 生成任务失败: {error_msg}")
            raise HTTPException(
                status_code=500,
                detail=error_msg
            )
            
    except HTTPException:
        raise
    except Exception as exc:
        import traceback
        error_detail = f"生成失败: {str(exc)}"
        logger.error(f"Creator {creator_id} 生成任务异常: {error_detail}")
        if os.getenv("DEBUG", "false").lower() == "true":
            error_detail += f"\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail) from exc


@app.get("/", response_class=HTMLResponse)
async def read_root():
    """
    返回 Creator 列表 UI 页面
    """
    html_file = os.path.join(static_dir, "index.html")
    if os.path.exists(html_file):
        with open(html_file, "r", encoding="utf-8") as f:
            return f.read()
    else:
        return """
        <html>
            <head><title>FC Morphix</title></head>
            <body>
                <h1>FC Morphix API</h1>
                <p>请访问 <a href="/static/index.html">/static/index.html</a> 查看 UI</p>
            </body>
        </html>
        """
