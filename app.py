import os
import smtplib
import requests
import logging
import uuid
import json
import asyncio
import boto3
from concurrent.futures import ThreadPoolExecutor
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
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


class CreateCreatorResponse(BaseModel):
    status: str
    message: str
    creator_id: str
    creator: Dict[str, Any]


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
async def get_creators(
    category: Optional[str] = None,
    status: Optional[str] = None,
    is_substack: Optional[int] = None
):
    """
    从 Supabase 获取 creator 列表，支持筛选
    
    查询参数:
    - category: 分类筛选
    - status: 状态筛选 (pending, generating, completed, failed)
    - is_substack: 是否发送筛选 (0=未发送, 1=已发送)
    
    修复：使用线程池执行同步操作，避免阻塞事件循环
    """
    try:
        # 记录接收到的参数
        logger.info(f"接收到的查询参数: category={category}, status={status}, is_substack={is_substack} (type: {type(is_substack)})")
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_api_key = os.getenv("SUPABASE_API_KEY")
        
        if not supabase_url or not supabase_api_key:
            raise HTTPException(
                status_code=500,
                detail="缺少 Supabase 配置（SUPABASE_URL 或 SUPABASE_API_KEY 环境变量）"
            )
        
        # 构建 Supabase 查询 URL
        api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
        query_params = []
        
        # 添加筛选条件
        if status:
            query_params.append(f"status=eq.{status}")
        
        # 处理 is_substack 筛选（int 类型，只有 0 和 1）
        if is_substack is not None:
            # 确保是整数类型
            is_substack_value = int(is_substack)
            query_params.append(f"is_substack=eq.{is_substack_value}")
            logger.info(f"添加 is_substack 筛选条件: is_substack=eq.{is_substack_value}")
        
        # 构建完整 URL
        if query_params:
            api_url += "?" + "&".join(query_params)
        
        logger.info(f"查询参数: category={category}, status={status}, is_substack={is_substack}")
        logger.info(f"查询 URL: {api_url}")
        
        headers = {
            "apikey": supabase_api_key,
            "Authorization": f"Bearer {supabase_api_key}",
            "Content-Type": "application/json"
        }
        
        # 在线程池中执行同步操作，避免阻塞事件循环
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: requests.get(api_url, headers=headers, timeout=30)
        )
        response.raise_for_status()
        
        creators: List[Dict[str, Any]] = response.json()
        logger.info(f"查询结果数量: {len(creators)}, 筛选条件: is_substack={is_substack}")
        
        # 调试：检查前几条数据的 is_substack 值
        if creators and len(creators) > 0:
            sample_is_substack = [c.get('is_substack') for c in creators[:5]]
            logger.info(f"前5条数据的 is_substack 值: {sample_is_substack}")
        
        # 如果有分类筛选，在前端过滤（因为 Supabase 数组包含查询比较复杂）
        if category:
            filtered_creators = []
            for creator in creators:
                categories = creator.get("content_category", [])
                if isinstance(categories, list) and category in categories:
                    filtered_creators.append(creator)
            creators = filtered_creators
        
        # 按 paid_subscribers_est 和 free_subscribers_est 降序排序
        def get_sort_key(creator: Dict[str, Any]) -> tuple:
            paid_subscribers = creator.get("paid_subscribers_est")
            try:
                paid_value = float(paid_subscribers) if paid_subscribers is not None else 0.0
            except (ValueError, TypeError):
                paid_value = 0.0
            
            free_subscribers = creator.get("free_subscribers_est")
            try:
                free_value = float(free_subscribers) if free_subscribers is not None else 0.0
            except (ValueError, TypeError):
                free_value = 0.0
            
            return (-paid_value, -free_value)
        
        creators.sort(key=get_sort_key)
        
        # 统计各状态的数量（从所有数据统计，不受筛选影响）
        # 为了获取准确的统计，需要查询所有数据
        stats_api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
        stats_response = await loop.run_in_executor(
            None,
            lambda: requests.get(stats_api_url, headers=headers, timeout=30)
        )
        stats_response.raise_for_status()
        all_creators = stats_response.json()
        
        stats = {
            "pending": 0,
            "completed": 0,
            "failed": 0
        }
        
        for creator in all_creators:
            creator_status = creator.get("status", "pending")
            if creator_status == "completed":
                stats["completed"] += 1
            elif creator_status == "failed":
                stats["failed"] += 1
            else:
                stats["pending"] += 1
        
        return CreatorResponse(
            creators=creators,
            count=len(creators),
            stats=stats
        )
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=f"从 Supabase 获取数据失败: {str(e)}"
        ) from e
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/creators", response_model=CreateCreatorResponse)
async def create_creator(
    creator_name: str = Form(...),
    handle: str = Form(...),
    website_url: str = Form(...),
    logo: UploadFile = File(...),
    content_category: Optional[str] = Form(None),
    creator_tokens_direct: Optional[str] = Form(None)
):
    """
    创建新的 Creator
    
    表单参数:
    - creator_name: Creator 名称（必填）
    - handle: Slug/Handle（必填，用于生成 Pitch Site）
    - website_url: 网站 URL（必填）
    - logo: Logo 图片文件（必填）
    - content_category: 分类（可选，JSON 数组字符串，如 '["Tech", "AI"]'）
    - creator_id: Creator ID（可选，纯数字。如果未提供将自动生成 6 位数字 ID）
    - creator_tokens_direct: Image With [] Tokens (可选，JSON 数组字符串或逗号分隔字符串)
    """
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_api_key = os.getenv("SUPABASE_API_KEY")
        s3_bucket = os.getenv("S3_BUCKET") or os.getenv("S3_BUCKET_NAME")
        
        if not supabase_url or not supabase_api_key:
            raise HTTPException(
                status_code=500,
                detail="缺少 Supabase 配置（SUPABASE_URL 或 SUPABASE_API_KEY 环境变量）"
            )
        
        if not s3_bucket:
            raise HTTPException(
                status_code=500,
                detail="缺少 S3 配置（S3_BUCKET 或 S3_BUCKET_NAME 环境变量）"
            )
        
        # 验证 logo 文件类型
        if not logo.content_type or not logo.content_type.startswith("image/"):
            raise HTTPException(
                status_code=400,
                detail=f"Logo 必须是图片文件，当前类型: {logo.content_type}"
            )
            
        # 自动生成唯一的 Creator ID
        import random
        final_creator_id = ""
        max_retries = 10
        retry_count = 0
        
        # 准备 Supabase 查询 API
        check_api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
        headers = {
            "apikey": supabase_api_key,
            "Authorization": f"Bearer {supabase_api_key}",
            "Content-Type": "application/json"
        }
        loop = asyncio.get_event_loop()
        
        while retry_count < max_retries:
            # 生成 100000 - 999999 之间的随机数
            generated_id = str(random.randint(100000, 999999))
            
            # 检查 ID 是否存在 (使用 HEAD 请求或者 GET + select=creator_id + limit=1)
            # 这里的查询类似: GET /creator?creator_id=eq.123456&select=creator_id&limit=1
            query_params = {
                "creator_id": f"eq.{generated_id}",
                "select": "creator_id",
                "limit": "1"
            }
            
            try:
                check_response = await loop.run_in_executor(
                    None,
                    lambda: requests.get(check_api_url, headers=headers, params=query_params, timeout=10)
                )
                
                if check_response.ok:
                    existing = check_response.json()
                    if not existing:
                        # 不存在，可以使用
                        final_creator_id = generated_id
                        break
                    else:
                        logger.warning(f"生成的 ID {generated_id} 已存在，正在重试...")
                else:
                    # 如果查询失败（非 409/冲突，而是系统错误），记录日志但尝试继续（或抛出异常）
                    logger.warning(f"检查 ID {generated_id} 失败: {check_response.status_code}")
            except Exception as e:
                logger.error(f"检查 ID 唯一性时发生异常: {str(e)}")
            
            retry_count += 1
            
        if not final_creator_id:
            raise HTTPException(
                status_code=500,
                detail="无法生成唯一的 Creator ID，请稍后重试"
            )
        
        # 读取 logo 文件内容
        logo_content = await logo.read()
        
        # 上传 logo 到 S3
        try:
            s3_client = boto3.client('s3')
            
            # 获取文件扩展名
            file_extension = logo.filename.split('.')[-1] if '.' in logo.filename else 'png'
            # 使用 final_creator_id 作为文件名的一部分
            s3_key = f"creatorAvator/{final_creator_id}.{file_extension}"
            
            # 上传文件
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=logo_content,
                ContentType=logo.content_type,
                ACL='public-read'
            )
            
            # 构建公网 URL
            logo_url = f"https://{s3_bucket}.s3.amazonaws.com/{s3_key}"
            logger.info(f"Logo 已上传到 S3: {logo_url}")
            
        except Exception as e:
            logger.error(f"上传 logo 到 S3 失败: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"上传 logo 失败: {str(e)}"
            )
        
        # 解析分类
        categories = []
        if content_category:
            try:
                categories = json.loads(content_category)
                if not isinstance(categories, list):
                    categories = [categories]
            except json.JSONDecodeError:
                # 如果不是 JSON，尝试按逗号分割
                categories = [cat.strip() for cat in content_category.split(',') if cat.strip()]
                
        # 解析 creator_tokens_direct
        tokens_direct = []
        if creator_tokens_direct:
            try:
                tokens_direct = json.loads(creator_tokens_direct)
                if not isinstance(tokens_direct, list):
                    tokens_direct = [tokens_direct]
            except json.JSONDecodeError:
                # 如果不是 JSON，尝试按逗号分割
                tokens_direct = [t.strip() for t in creator_tokens_direct.split(',') if t.strip()]
        
        # 构建 creator 数据
        creator_data = {
            "creator_id": final_creator_id,
            "creator_name": creator_name,
            "handle": handle,
            "website_url": website_url,
            "creator_signature_image_url": logo_url,
            "newsletter_name": creator_name,  # 使用 creator_name 作为默认值
            "platform": "substack",  # 默认平台
            "status": "pending",  # 初始状态
            "is_substack": 0,  # 默认未发送
            "content_category": categories,
            "creator_tokens_direct": tokens_direct,
            "created_at": datetime.utcnow().isoformat()
        }
        
        # 插入到 Supabase
        api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
        headers = {
            "apikey": supabase_api_key,
            "Authorization": f"Bearer {supabase_api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: requests.post(api_url, headers=headers, json=creator_data, timeout=30)
        )
        
        if not response.ok:
            error_detail = f"HTTP {response.status_code}: {response.text[:200]}"
            logger.error(f"插入 Supabase 失败: {error_detail}")
            raise HTTPException(
                status_code=500,
                detail=f"保存到数据库失败: {error_detail}"
            )
        
        created_creator = response.json()
        if isinstance(created_creator, list) and len(created_creator) > 0:
            created_creator = created_creator[0]
        
        logger.info(f"成功创建 Creator: {final_creator_id} - {creator_name}")
        
        return CreateCreatorResponse(
            status="success",
            message=f"成功创建 Creator: {creator_name}",
            creator_id=final_creator_id,
            creator=created_creator
        )
        
    except HTTPException:
        raise
    except Exception as exc:
        import traceback
        error_detail = f"创建 Creator 失败: {str(exc)}"
        logger.error(f"创建 Creator 异常: {error_detail}")
        if os.getenv("DEBUG", "false").lower() == "true":
            error_detail += f"\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail) from exc





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


class UpdateIsSubstackRequest(BaseModel):
    is_substack: int  # 0 或 1


@app.patch("/api/creators/{creator_id}/is_substack")
async def update_is_substack(creator_id: str, payload: UpdateIsSubstackRequest):
    """
    更新 Creator 的 is_substack 状态
    is_substack: 0 表示未发送，1 表示已发送
    """
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_api_key = os.getenv("SUPABASE_API_KEY")
        
        if not supabase_url or not supabase_api_key:
            raise HTTPException(
                status_code=500,
                detail="缺少 Supabase 配置（SUPABASE_URL 或 SUPABASE_API_KEY 环境变量）"
            )
        
        # 验证 is_substack 值
        if payload.is_substack not in [0, 1]:
            raise HTTPException(
                status_code=400,
                detail="is_substack 必须是 0 或 1"
            )
        
        # 构建更新 URL
        api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
        update_url = f"{api_url}?creator_id=eq.{creator_id}"
        
        headers = {
            "apikey": supabase_api_key,
            "Authorization": f"Bearer {supabase_api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        
        # 更新 is_substack 字段
        update_data = {
            "is_substack": payload.is_substack
        }
        
        # 在线程池中执行同步操作
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: requests.patch(update_url, headers=headers, json=update_data, timeout=30)
        )
        
        response.raise_for_status()
        
        logger.info(f"Creator {creator_id} 的 is_substack 已更新为 {payload.is_substack}")
        
        return {
            "status": "success",
            "message": f"is_substack 已更新为 {payload.is_substack}",
            "creator_id": creator_id,
            "is_substack": payload.is_substack
        }
        
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_body = e.response.json()
                error_detail = f"HTTP {e.response.status_code}: {error_body}"
            except:
                error_detail = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
        raise HTTPException(
            status_code=500,
            detail=f"更新 is_substack 失败: {error_detail}"
        ) from e
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
