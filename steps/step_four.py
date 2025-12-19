"""
第四步：生成场景图，使用第3步生成的图片
"""
import os
import time
import random
from typing import Dict, Any, Optional, Tuple, List
from urllib.parse import urlparse
import requests

from utils.cache import load_cache, save_cache
from utils.logger import log_and_print


def load_env_file(path: str = ".env"):
    """Load environment variables from a .env file (supports `export KEY=value`)."""
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as file:
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


def validate(**kwargs) -> Tuple[bool, Optional[str]]:
    """
    第四步校验：检查第三步结果
    返回: (是否通过, 错误信息)
    """
    # 加载 .env 文件
    load_env_file()
    
    # 检查第三步结果
    step_three_result = kwargs.get("step_three_result")
    if not step_three_result:
        return False, "缺少第三步结果（step_three_result）"
    
    results = step_three_result.get("results", [])
    if not results:
        return False, "第三步结果中没有成功处理的记录"
    
    # 检查每个 creator 是否有足够的 magnet 图片（至少3个）
    for creator_result in results:
        magnet_results = creator_result.get("magnet_results", [])
        if len(magnet_results) < 3:
            creator_id = creator_result.get("creator_id", "未知")
            return False, f"Creator {creator_id} 的 magnet 数量不足3个，无法生成场景图"
    
    return True, None


def build_scene_prompt() -> str:
    """
    构建场景图生成提示词
    """
    prompt = """
    The first image is a template; replace three refrigerator magnets on it.Extreme close-up cinematic shot of three different fridge magnets,\neach magnet appearing as a separate frozen moment,\nclean minimalist product design, no extra decoration,\nperfectly aligned on a modern refrigerator door.\n\nAll magnets:\n- same scale and visual weight\n- consistent lighting and shadow direction\n- flush against the fridge surface\n- premium matte or semi-matte finish\n\nStrong constraints: Stacked panels + transparent glossy adhesive surface, high gloss.\n\nEnvironment:\nmodern minimalist kitchen,\nsoft early morning natural light,\nsubtle reflections on fridge surface,\nshallow depth of field,\ncinematic realism\n\nsize：16:9, 4k.
    """
    
    return prompt


def generate_scene_image(
    prompt: str,
    image_urls: List[str],
    api_url: str
) -> Dict[str, Any]:
    """
    调用图片生成 API 生成场景图
    使用 image-to-image API，传入多个magnet图片URL数组
    """
    # 规范化 API URL：移除末尾斜杠，确保路径正确
    api_url = api_url.rstrip('/')
    # 如果 URL 是根路径或缺少路径，自动添加 /image-to-image
    if not api_url.endswith('/image-to-image'):
        # 检查是否是根路径（只有域名，没有路径）
        # 例如：https://media.datail.ai 或 https://media.datail.ai/
        parsed = urlparse(api_url)
        # 如果路径为空或只有根路径，添加 /image-to-image
        if not parsed.path or parsed.path == '/':
            api_url = f"{api_url.rstrip('/')}/image-to-image"
        # 如果已经有其他路径但不是 /image-to-image，保持原样（可能是自定义路径）
    
    image_tpl = [
        # "https://amzn-s3-fc-bucket.s3.sa-east-1.amazonaws.com/images/2025/12/18/60fadc420a944a4697fe9a119508ac8d.png",
        "https://amzn-s3-fc-bucket.s3.sa-east-1.amazonaws.com/images/2025/12/18/82ce9e656b6b437e9a3277d3dae16d07.png"
        # "https://amzn-s3-fc-bucket.s3.sa-east-1.amazonaws.com/images/2025/12/18/8038c493aa194f699ae43ce435517a8c.png"
    ]
    # 每次请求前随机取一张 image_tpl 中的地址插入 image_urls 第1个位置
    random_image = random.choice(image_tpl)
    image_urls = [random_image] + image_urls
    # 从环境变量获取 model 参数，如果没有则使用默认值
    model = os.getenv("SCENE_MODEL", "gemini-3-pro-image-preview")
    payload = {
        "prompt": prompt,
        "image_urls": image_urls,
        "model": model
    }
    
    # 打印请求参数到控制台
    print(f"      📤 请求参数:")
    print(f"        API URL: {api_url}")
    print(f"        prompt: {prompt}")
    print(f"        image_urls 数量: {len(image_urls)}")
    for idx, url in enumerate(image_urls, 1):
        print(f"          URL {idx}: {url}")
    
    try:
        response = requests.post(api_url, json=payload, timeout=600)  # 10分钟超时
        response.raise_for_status()
        result = response.json()
        
        # 打印场景图生成结果到控制台
        image_result = {
            "status": "success",
            "urls": result.get("urls", []),
            "texts": result.get("texts", [])
        }
        
        print(f"      📥 场景图生成结果:")
        print(f"        status: {image_result['status']}")
        print(f"        urls 数量: {len(image_result['urls'])}")
        if image_result['urls']:
            for idx, url in enumerate(image_result['urls'], 1):
                print(f"          URL {idx}: {url}")
        if image_result.get('texts'):
            print(f"        texts: {image_result['texts']}")
        else:
            print(f"        texts: []")
        
        return image_result
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_detail = f"HTTP {e.response.status_code}"
            try:
                error_body = e.response.json()
                error_detail += f": {error_body}"
            except:
                error_detail += f": {e.response.text[:200]}"
        
        raise RuntimeError(f"调用场景图生成 API 失败 - {error_detail}") from e


def get_task_id_from_step_two_result(
    creator_id: Any,
    step_two_result: Dict[str, Any]
) -> Optional[Any]:
    """
    从 step_two_result 中获取指定 creator 的 task_id
    """
    if not step_two_result:
        return None
    
    results = step_two_result.get("results", [])
    for result in results:
        if str(result.get("creator_id")) == str(creator_id):
            task_id = result.get("task_id")
            if task_id:
                print(f"        ✓ 从 step_two_result 获取 task_id: {task_id}")
                return task_id
    return None


def get_task_id_from_magnet_image(
    creator_id: Any,
    context_id: str,
    supabase_url: str,
    supabase_api_key: str,
    step_two_result: Optional[Dict[str, Any]] = None
) -> Optional[Any]:
    """
    从 magnet_image 表中查询指定 magnet 的 task_id
    查询时优先查询 type=normal 的记录，如果找不到则查询 type 为 null 的记录，最后查询所有类型
    如果数据库查询失败，会尝试从 step_two_result 中获取
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/magnet_image"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        # 策略1: 先尝试查询 type=normal 的记录
        query_url = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&type=eq.normal&select=task_id&limit=1"
        response = requests.get(query_url, headers=headers, timeout=30)
        response.raise_for_status()
        results = response.json()
        
        if results and len(results) > 0:
            task_id = results[0].get("task_id")
            if task_id:
                print(f"        ✓ 成功获取 task_id (type=normal): {task_id}")
                return task_id
        
        # 策略2: 如果找不到 type=normal，尝试查询 type 为 null 的记录
        print(f"        ℹ️  type=normal 未找到，尝试查询 type 为 null 的记录...")
        query_url_null = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&type=is.null&select=task_id&limit=1"
        response_null = requests.get(query_url_null, headers=headers, timeout=30)
        response_null.raise_for_status()
        results_null = response_null.json()
        
        if results_null and len(results_null) > 0:
            task_id = results_null[0].get("task_id")
            if task_id:
                print(f"        ✓ 成功获取 task_id (type=null): {task_id}")
                return task_id
        
        # 策略3: 如果还找不到，查询所有类型（排除 type=cover）
        print(f"        ℹ️  type=null 未找到，尝试查询所有类型（排除 cover）...")
        query_url_all = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&select=task_id,type&limit=10"
        response_all = requests.get(query_url_all, headers=headers, timeout=30)
        response_all.raise_for_status()
        results_all = response_all.json()
        
        # 从结果中过滤掉 type=cover 的记录
        for record in results_all:
            record_type = record.get("type")
            if record_type != "cover":
                task_id = record.get("task_id")
                if task_id:
                    print(f"        ✓ 成功获取 task_id (过滤后): {task_id}")
                    return task_id
        
        print(f"        ⚠️  未找到匹配的记录 (creator_id: {creator_id}, context_id: {context_id})")
        
        # 策略4: 如果数据库查询失败，尝试从 step_two_result 中获取
        if step_two_result:
            task_id = get_task_id_from_step_two_result(creator_id, step_two_result)
            if task_id:
                return task_id
        
        return None
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_detail = f"HTTP {e.response.status_code}"
            try:
                error_body = e.response.json()
                error_detail += f": {error_body}"
            except:
                error_detail += f": {e.response.text[:200]}"
        print(f"        ⚠️  查询 task_id 失败: {error_detail}")
        
        # 如果数据库查询异常，尝试从 step_two_result 中获取
        if step_two_result:
            task_id = get_task_id_from_step_two_result(creator_id, step_two_result)
            if task_id:
                return task_id
        
        return None


def check_record_exists(
    creator_id: Any,
    context_id: str,
    supabase_url: str,
    supabase_api_key: str,
    type: Optional[str] = None
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    检查 magnet_image 表中记录是否存在
    返回: (是否存在, 记录数据或None)
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/magnet_image"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json"
    }
    
    # 构建查询URL：第四步使用 creator_id + context_id + type
    if type is not None:
        query_url = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&type=eq.{type}&select=*&limit=1"
    else:
        query_url = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&select=*&limit=1"
    
    try:
        response = requests.get(query_url, headers=headers, timeout=30)
        response.raise_for_status()
        results = response.json()
        
        if results and len(results) > 0:
            # 如果提供了type参数，需要进一步过滤
            if type is not None:
                for record in results:
                    if record.get("type") == type:
                        return True, record
                return False, None
            else:
                # 如果没有指定type，返回第一条记录
                return True, results[0]
        
        return False, None
    except requests.exceptions.RequestException as e:
        # 查询失败时返回False，让调用方决定如何处理
        return False, None


def save_scene_image_to_supabase(
    creator_id: Any,
    context_id: str,
    task_id: Any,
    scene_image_url: str,
    supabase_url: str,
    supabase_api_key: str,
    front_name: str = "",
    front_style_key: str = "",
    front_image_prompt: str = ""
) -> bool:
    """
    将场景图保存到 Supabase magnet_image 表，type=cover
    写入前先检查是否存在（基于 creator_id + context_id + type=cover），如果存在则更新，否则插入
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/magnet_image"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    # 构建 payload，必须包含 type=cover 参数
    record_type = "cover"
    payload = {
        "task_id": task_id,
        "context_id": context_id,
        "creator_id": str(creator_id),
        "front_image_url": scene_image_url,
        "type": record_type,  # 必需参数：标识这是场景图（封面图）
        "front_name": front_name,
        "front_style_key": front_style_key,
        "front_image_prompt": front_image_prompt
    }
    
    try:
        # 先检查记录是否存在（第四步：检查 creator_id + context_id + type）
        exists, existing_record = check_record_exists(
            creator_id=creator_id,
            context_id=context_id,
            supabase_url=supabase_url,
            supabase_api_key=supabase_api_key,
            type=record_type
        )
        
        if exists:
            # 记录已存在，进行更新
            print(f"        ℹ️  记录已存在，尝试更新 (creator_id: {creator_id}, context_id: {context_id}, type: {record_type})...")
            update_url = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&type=eq.{record_type}"
            
            try:
                update_response = requests.patch(update_url, headers=headers, json=payload, timeout=30)
                
                if not update_response.ok:
                    update_error_detail = f"HTTP {update_response.status_code}"
                    try:
                        update_error_body = update_response.json()
                        update_error_detail += f": {update_error_body}"
                    except:
                        update_error_detail += f": {update_response.text[:500]}"
                    print(f"        ❌ 更新失败: {update_error_detail}")
                    return False
                
                update_response.raise_for_status()
                
                # 检查实际更新的记录数
                try:
                    updated_records = update_response.json()
                    if isinstance(updated_records, list):
                        if len(updated_records) == 0:
                            print(f"        ⚠️  警告: PATCH 请求成功但未更新任何记录（可能查询条件不匹配）")
                            print(f"        ℹ️  查询条件: creator_id={creator_id}, context_id={context_id}, type={record_type}")
                            return False
                        else:
                            print(f"        ✓ 成功更新 {len(updated_records)} 条记录")
                            return True
                    else:
                        # 如果不是列表，可能是单个对象
                        print(f"        ✓ 成功更新记录")
                        return True
                except:
                    # 如果无法解析响应，但状态码是成功的，假设更新成功
                    print(f"        ✓ 更新请求成功 (状态码: {update_response.status_code})")
                    return True
                    
            except requests.exceptions.RequestException as e:
                update_error_detail = str(e)
                if hasattr(e, 'response') and e.response is not None:
                    update_error_detail = f"HTTP {e.response.status_code}"
                    try:
                        update_error_body = e.response.json()
                        update_error_detail += f": {update_error_body}"
                    except:
                        update_error_detail += f": {e.response.text[:500]}"
                print(f"        ❌ 更新 Supabase 场景图失败: {update_error_detail}")
                return False
        else:
            # 记录不存在，进行插入
            print(f"        📤 记录不存在，尝试插入场景图到 Supabase (creator_id: {creator_id}, context_id: {context_id}, type: {record_type})...")
            response = requests.post(api_url, headers=headers, json=payload, timeout=30)
            
            # 如果插入失败
            if not response.ok:
                error_detail = f"HTTP {response.status_code}"
                try:
                    error_body = response.json()
                    error_detail += f": {error_body}"
                except:
                    error_text = response.text[:500] if response.text else "无响应内容"
                    error_detail += f": {error_text}"
                print(f"        ❌ 插入失败，详细错误信息:")
                print(f"          状态码: {response.status_code}")
                print(f"          错误详情: {error_detail}")
                print(f"          Payload: {payload}")
                return False
            
            # 插入成功
            response.raise_for_status()
            try:
                inserted_records = response.json()
                if isinstance(inserted_records, list):
                    print(f"        ✓ 成功插入 {len(inserted_records)} 条记录")
                else:
                    print(f"        ✓ 成功插入记录")
            except:
                print(f"        ✓ 插入请求成功 (状态码: {response.status_code})")
            return True
        
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_detail = f"HTTP {e.response.status_code}"
            try:
                error_body = e.response.json()
                error_detail += f": {error_body}"
            except:
                error_detail += f": {e.response.text[:200]}"
        print(f"        ⚠️  保存 Supabase 场景图失败: {error_detail}")
        return False
    except Exception as e:
        print(f"        ⚠️  保存 Supabase 场景图失败: {str(e)}")
        return False


def execute(**kwargs) -> Dict[str, Any]:
    """
    第四步执行：生成场景图，使用第3步生成的图片
    每个 creator 选择前3个 magnet 的图片生成场景图
    """
    # 先校验
    is_valid, error_msg = validate(**kwargs)
    if not is_valid:
        raise ValueError(f"第四步校验失败: {error_msg}")
    
    # 加载 .env 文件
    load_env_file()
    
    # 获取配置
    step_three_result = kwargs.get("step_three_result", {})
    step_two_result = kwargs.get("step_two_result", {})  # 可选：用于获取 task_id
    results = step_three_result.get("results", [])
    # 场景图生成 API URL（使用 image-to-image API）
    scene_api_url = kwargs.get("scene_api_url") or os.getenv("SCENE_API_URL") or "https://media.datail.ai/image-to-image"
    use_cache = kwargs.get("use_cache", True)  # 第四步默认使用缓存
    # 获取 Supabase 配置
    supabase_url = kwargs.get("supabase_url") or os.getenv("SUPABASE_URL")
    supabase_api_key = kwargs.get("supabase_api_key") or os.getenv("SUPABASE_API_KEY")
    
    if use_cache:
        print("  ℹ️  第四步使用缓存（场景图生成结果）")
    else:
        print("  ℹ️  禁用缓存模式：将重新生成场景图")
    
    # 为每个 creator 生成场景图
    all_scene_results = []
    total_scenes_generated = 0
    errors = []
    cache_hits = 0
    
    for creator_result in results:
        creator_id = creator_result.get("creator_id")
        magnet_results = creator_result.get("magnet_results", [])
        
        print(f"  处理 Creator {creator_id} 的场景图生成...")
        
        # 检查缓存（如果缓存存在，跳过场景图生成和数据库更新）
        cached_result = None
        if use_cache:
            cached_result = load_cache(creator_id, "step_four")
            if cached_result:
                print(f"    ✓ 使用缓存结果，跳过场景图生成和数据库更新")
                cache_hits += 1
                # 统计缓存的场景图数量
                scene_urls = cached_result.get("scene_urls", [])
                total_scenes_generated += len(scene_urls)
                
                all_scene_results.append(cached_result)
                continue
        
        # 选择前3个有图片的 magnet（跳过没有图片的）
        # 筛选出有图片的 magnet
        magnets_with_images = []
        for magnet in magnet_results:
            image_url = magnet.get("front_image_url") or (
                magnet.get("images", [{}])[0].get("urls", [""])[0] if magnet.get("images") else ""
            )
            if image_url:
                magnets_with_images.append({
                    "magnet": magnet,
                    "image_url": image_url
                })
        
        # 检查是否有足够的有图片的 magnet（至少需要3个）
        if len(magnets_with_images) < 3:
            error_msg = f"Creator {creator_id} 的有图片的 magnet 数量不足3个（共 {len(magnet_results)} 个 magnet，其中 {len(magnets_with_images)} 个有图片），无法生成场景图"
            print(f"    ❌ {error_msg}")
            errors.append({
                "creator_id": creator_id,
                "error": error_msg,
                "total_magnets": len(magnet_results),
                "magnets_with_images": len(magnets_with_images)
            })
            continue
        
        # 使用前3个有图片的 magnet
        magnet_1_data = magnets_with_images[0]
        magnet_2_data = magnets_with_images[1]
        magnet_3_data = magnets_with_images[2]
        
        magnet_1 = magnet_1_data["magnet"]
        magnet_2 = magnet_2_data["magnet"]
        magnet_3 = magnet_3_data["magnet"]
        
        magnet_1_image_url = magnet_1_data["image_url"]
        magnet_2_image_url = magnet_2_data["image_url"]
        magnet_3_image_url = magnet_3_data["image_url"]
        
        print(f"    使用以下3个 magnet 图片生成场景图:")
        print(f"      Magnet 1 (context_id: {magnet_1.get('context_id', 'N/A')}): {magnet_1_image_url}")
        print(f"      Magnet 2 (context_id: {magnet_2.get('context_id', 'N/A')}): {magnet_2_image_url}")
        print(f"      Magnet 3 (context_id: {magnet_3.get('context_id', 'N/A')}): {magnet_3_image_url}")
        
        # 构建提示词
        prompt = build_scene_prompt()
        
        # 构建图片URL数组
        image_urls = [magnet_1_image_url, magnet_2_image_url, magnet_3_image_url]
        
        # 生成场景图（传入三个magnet图片URL数组），带重试机制
        max_retries = 3
        scene_result = None
        scene_urls = []
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                if attempt == 1:
                    print(f"    生成场景图（使用3个magnet图片）...")
                else:
                    # 如果是第三次尝试（前2次都失败），等待10秒
                    if attempt == 3:
                        print(f"    🔄 第 {attempt} 次尝试（前2次失败，等待10秒后重试）...")
                        time.sleep(10)
                    else:
                        print(f"    🔄 第 {attempt} 次重试...")
                
                scene_result = generate_scene_image(
                    prompt=prompt,
                    image_urls=image_urls,
                    api_url=scene_api_url
                )
                
                scene_urls = scene_result.get("urls", [])
                if scene_urls:
                    total_scenes_generated += len(scene_urls)
                    print(f"      ✓ 成功生成 {len(scene_urls)} 张场景图")
                    for url_idx, url in enumerate(scene_urls, 1):
                        print(f"        📷 场景图 {url_idx}: {url}")
                    # 成功生成，跳出重试循环
                    break
                else:
                    error_msg = "API 返回的场景图 URL 列表为空"
                    last_error = ValueError(error_msg)
                    print(f"      ❌ 第 {attempt} 次尝试失败: {error_msg}")
                    if attempt < max_retries:
                        continue
                    else:
                        raise last_error
                        
            except Exception as e:
                last_error = e
                error_msg = str(e)
                print(f"      ❌ 第 {attempt} 次尝试失败: {error_msg}")
                if attempt < max_retries:
                    # 如果不是最后一次尝试，继续重试
                    continue
                else:
                    # 最后一次尝试也失败，抛出异常
                    raise
        
        # 如果所有重试都失败，scene_urls 仍为空，抛出异常
        if not scene_urls:
            if last_error:
                raise last_error
            else:
                raise ValueError("API 返回的场景图 URL 列表为空")
        
        # 继续后续处理（保存到 Supabase 等）
        try:
            # 保存场景图到 Supabase magnet_image 表（type=cover）
            if scene_urls and supabase_url and supabase_api_key:
                # 只使用第一个 magnet 的 context_id 作为场景图的 context_id
                magnet_1_context_id = magnet_1.get("context_id", "")
                scene_context_id = magnet_1_context_id
                
                # 从第一个 magnet 获取必需字段
                magnet_1_front_name = magnet_1.get("front_name", "")
                magnet_1_front_style_key = magnet_1.get("front_style_key", "")
                magnet_1_front_image_prompt = magnet_1.get("front_image_prompt", "")
                
                # 获取第一个 magnet 的 task_id（用于查询，使用第一个 magnet 的 context_id）
                task_id = None
                if magnet_1_context_id:
                    print(f"      🔄 查询 task_id (creator_id: {creator_id}, context_id: {magnet_1_context_id})...")
                    task_id = get_task_id_from_magnet_image(
                        creator_id=creator_id,
                        context_id=magnet_1_context_id,
                        supabase_url=supabase_url,
                        supabase_api_key=supabase_api_key,
                        step_two_result=step_two_result
                    )
                
                if task_id:
                    save_msg = f"🔄 保存场景图到 Supabase magnet_image 表 (type=cover, context_id: {scene_context_id})..."
                    print(f"      {save_msg}")
                    log_and_print(creator_id, "step_four", save_msg)
                    
                    # 为每个场景图 URL 保存一条记录（通常只有一张场景图）
                    for scene_idx, scene_url in enumerate(scene_urls, 1):
                        # 如果有多张场景图，使用不同的 context_id 后缀区分
                        if len(scene_urls) > 1:
                            current_context_id = f"{scene_context_id}_SCENE_{scene_idx}"
                        else:
                            current_context_id = scene_context_id
                        
                        save_success = save_scene_image_to_supabase(
                            creator_id=creator_id,
                            context_id=current_context_id,
                            task_id=task_id,
                            scene_image_url=scene_url,
                            supabase_url=supabase_url,
                            supabase_api_key=supabase_api_key,
                            front_name=magnet_1_front_name,
                            front_style_key=magnet_1_front_style_key,
                            front_image_prompt=magnet_1_front_image_prompt
                        )
                        if save_success:
                            success_msg = f"✓ 成功保存场景图 {scene_idx} 到 Supabase (context_id: {current_context_id}, url: {scene_url})"
                            print(f"        {success_msg}")
                            log_and_print(creator_id, "step_four", success_msg)
                        else:
                            warning_msg = f"⚠️  保存场景图 {scene_idx} 到 Supabase 失败 (context_id: {current_context_id})"
                            print(f"        {warning_msg}")
                            log_and_print(creator_id, "step_four", warning_msg, "WARNING")
                else:
                    warning_msg = "⚠️  无法获取 task_id，跳过 Supabase 保存"
                    print(f"      {warning_msg}")
                    log_and_print(creator_id, "step_four", warning_msg, "WARNING")
            elif not scene_urls:
                warning_msg = "⚠️  没有可用的场景图 URL，跳过 Supabase 保存"
                print(f"      {warning_msg}")
                log_and_print(creator_id, "step_four", warning_msg, "WARNING")
            elif not supabase_url or not supabase_api_key:
                warning_msg = "⚠️  缺少 Supabase 配置，跳过保存"
                print(f"      {warning_msg}")
                log_and_print(creator_id, "step_four", warning_msg, "WARNING")
            
            creator_scene_result = {
                "creator_id": creator_id,
                "magnet_1": {
                    "context_id": magnet_1.get("context_id", ""),
                    "front_name": magnet_1.get("front_name", ""),
                    "image_url": magnet_1_image_url
                },
                "magnet_2": {
                    "context_id": magnet_2.get("context_id", ""),
                    "front_name": magnet_2.get("front_name", ""),
                    "image_url": magnet_2_image_url
                },
                "magnet_3": {
                    "context_id": magnet_3.get("context_id", ""),
                    "front_name": magnet_3.get("front_name", ""),
                    "image_url": magnet_3_image_url
                },
                "scene_urls": scene_urls,
                "scene_texts": scene_result.get("texts", []),
                "prompt": prompt,
                "status": "success"
            }
            
            # 检查场景图是否成功生成（scene_urls 不为空）
            scene_generation_success = scene_urls and len(scene_urls) > 0
            
            # 只有场景图生成成功时才保存缓存
            if use_cache and scene_generation_success:
                save_cache(creator_id, creator_scene_result, "step_four")
                print(f"    ✓ 结果已保存到缓存")
            elif use_cache and not scene_generation_success:
                print(f"    ⚠️  场景图生成失败，跳过缓存保存")
            
            # 只有场景图生成成功时才添加到结果列表
            if scene_generation_success:
                all_scene_results.append(creator_scene_result)
            else:
                error_msg = "场景图生成失败（scene_urls 为空）"
                print(f"    ❌ {error_msg}")
                errors.append({
                    "creator_id": creator_id,
                    "error": error_msg
                })
            
        except Exception as e:
            error_msg = str(e)
            print(f"    ❌ 生成场景图失败（已重试 {max_retries} 次）: {error_msg}")
            print(f"    ⚠️  执行失败，未保存缓存")
            errors.append({
                "creator_id": creator_id,
                "error": error_msg
            })
    
    # 构建返回结果
    result = {
        "step": 4,
        "status": "completed",
        "message": f"成功为 {len(all_scene_results)} 个 creator 生成场景图，共生成 {total_scenes_generated} 张场景图（缓存命中: {cache_hits}）",
        "results": all_scene_results,
        "total_creators": len(results),
        "success_count": len(all_scene_results),
        "total_scenes_generated": total_scenes_generated,
        "errors": errors,
        "cache_hits": cache_hits
    }
    
    return result

