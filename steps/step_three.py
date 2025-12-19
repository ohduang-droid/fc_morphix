"""
第三步：为每个 magnet 的 prompt 生成图片
"""
import os
import time
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
    第三步校验：检查第二步结果和图片生成 API 配置
    返回: (是否通过, 错误信息)
    """
    # 加载 .env 文件
    load_env_file()
    
    # 检查第二步结果
    step_two_result = kwargs.get("step_two_result")
    if not step_two_result:
        return False, "缺少第二步结果（step_two_result）"
    
    results = step_two_result.get("results", [])
    if not results:
        return False, "第二步结果中没有成功处理的记录"
    
    # 图片生成 API URL 固定配置
    # API 端点固定为: https://media.datail.ai/image-to-image
    
    # 检查参考图 URL（可选，有默认值）
    default_image_url = kwargs.get("default_image_url") or os.getenv("DEFAULT_IMAGE_URL")
    
    return True, None




def generate_image(
    prompt: str,
    image_url: str,
    api_url: str,
    key_prefix: str = "images"
) -> Dict[str, Any]:
    """
    调用图片生成 API 生成一张图片
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
    
    model = os.getenv("SCENE_MODEL", "gemini-3-pro-image-preview")
    payload = {
        "prompt": prompt,
        "image_url": image_url,
        "model": model,
        "key_prefix": key_prefix
    }
    
    # 打印请求参数到控制台
    print(f"      📤 请求参数:")
    print(f"        API URL: {api_url}")
    print(f"        key_prefix: {key_prefix}")
    print(f"        image_url: {image_url}")
    print(f"        prompt (长度: {len(prompt)} 字符):")
    # 如果 prompt 太长，只打印前 500 个字符
    if len(prompt) > 500:
        print(f"          {prompt[:500]}...")
    else:
        print(f"          {prompt}")
    
    try:
        response = requests.post(api_url, json=payload, timeout=600)  # 10分钟超时
        response.raise_for_status()
        result = response.json()
        
        # 打印图片生成结果到控制台
        image_result = {
            "status": "success",
            "urls": result.get("urls", []),
            "texts": result.get("texts", [])
        }
        
        print(f"      📥 图片生成结果:")
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
        
        raise RuntimeError(f"调用图片生成 API 失败 - {error_detail}") from e


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
    
    # 构建查询URL：第三步使用 creator_id + context_id + type
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


def update_supabase_image_url(
    creator_id: Any,
    context_id: str,
    front_image_url: str,
    supabase_url: str,
    supabase_api_key: str,
    type: Optional[str] = None,
    magnet_record: Optional[Dict[str, Any]] = None
) -> bool:
    """
    更新 Supabase magnet_image 表中的 front_image_url
    写入前先检查是否存在（基于 creator_id + context_id + type），如果存在则更新，否则插入新记录
    第三步必须提供 type 参数
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/magnet_image"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    # 第三步必须提供 type 参数，默认为 "normal"
    if type is None:
        type = "normal"
    
    # 先检查记录是否存在（第三步：检查 creator_id + context_id + type）
    exists, existing_record = check_record_exists(
        creator_id=creator_id,
        context_id=context_id,
        supabase_url=supabase_url,
        supabase_api_key=supabase_api_key,
        type=type
    )
    
    if not exists:
        # 记录不存在，尝试插入新记录
        print(f"        ℹ️  记录不存在，尝试插入新记录 (creator_id={creator_id}, context_id={context_id}, type={type})")
        
        # 构建插入的 payload，包含基本必需字段
        insert_payload = {
            "creator_id": str(creator_id),
            "context_id": context_id,
            "type": type,
            "front_image_url": front_image_url
        }
        
        # 如果提供了 magnet_record，尝试添加更多字段
        if magnet_record:
            if "front_name" in magnet_record:
                insert_payload["front_name"] = magnet_record.get("front_name", "")
            if "front_image_prompt" in magnet_record:
                insert_payload["front_image_prompt"] = magnet_record.get("front_image_prompt", "")
            if "front_style_key" in magnet_record:
                insert_payload["front_style_key"] = magnet_record.get("front_style_key", "")
            if "task_id" in magnet_record:
                insert_payload["task_id"] = magnet_record.get("task_id")
        
        try:
            response = requests.post(api_url, headers=headers, json=insert_payload, timeout=30)
            response.raise_for_status()
            print(f"        ✓ 成功插入新记录 (creator_id={creator_id}, context_id={context_id}, type={type})")
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
            print(f"        ⚠️  插入新记录失败 (creator_id={creator_id}, context_id={context_id}, type={type}): {error_detail}")
            return False
    
    # 记录存在，进行更新
    update_url = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&type=eq.{type}"
    
    payload = {
        "front_image_url": front_image_url
    }
    
    try:
        response = requests.patch(update_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
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
        print(f"        ⚠️  更新 Supabase front_image_url 失败: {error_detail}")
        return False


def execute(**kwargs) -> Dict[str, Any]:
    """
    第三步执行：为每个 magnet 的 prompt 生成图片
    默认每个 magnet 生成 1 张图片，可通过 images_per_magnet 参数配置
    """
    # 先校验
    is_valid, error_msg = validate(**kwargs)
    if not is_valid:
        raise ValueError(f"第三步校验失败: {error_msg}")
    
    # 加载 .env 文件
    load_env_file()
    
    # 获取配置
    step_two_result = kwargs.get("step_two_result", {})
    results = step_two_result.get("results", [])
    # 图片生成 API URL（从环境变量获取，如果没有则使用默认值）
    image_api_url = os.getenv("IMAGE_API_URL", "https://media.datail.ai/image-to-image")
    # key_prefix 固定为 "images"
    key_prefix = "images"
    # 默认参考图 URL（从环境变量或参数获取，如果没有则使用示例中的 URL）
    default_image_url = kwargs.get("default_image_url") or os.getenv("DEFAULT_IMAGE_URL") or "https://substackcdn.com/image/fetch/$s_!8MSN!,w_80,h_80,c_fill,f_webp,q_auto:good,fl_progressive:steep,g_auto/https%3A%2F%2Fsubstack-post-media.s3.amazonaws.com%2Fpublic%2Fimages%2F441213db-4824-4e48-9d28-a3a18952cbfc_592x592.png"
    use_cache = kwargs.get("use_cache", True)  # 第三步默认使用缓存（图片生成结果）
    # 每个 magnet 生成的图片数量（默认为 1）
    images_per_magnet = kwargs.get("images_per_magnet", 1)
    # 获取 Supabase 配置
    supabase_url = kwargs.get("supabase_url") or os.getenv("SUPABASE_URL")
    supabase_api_key = kwargs.get("supabase_api_key") or os.getenv("SUPABASE_API_KEY")
    
    if use_cache:
        print("  ℹ️  第三步使用缓存（图片生成结果），但 Supabase 更新每次都会执行")
    
    # 为每个 creator 的结果生成图片
    all_image_results = []
    total_magnets = 0
    total_images_generated = 0
    errors = []
    cache_hits = 0
    
    for creator_result in results:
        creator_id = creator_result.get("creator_id")
        saved_records = creator_result.get("saved_records", [])
        
        # 如果 saved_records 为空，尝试从 dify_response.magnet_sku_list 获取数据
        if not saved_records:
            dify_response = creator_result.get("dify_response", {})
            magnet_sku_list = dify_response.get("magnet_sku_list", [])
            if magnet_sku_list:
                print(f"  注意：Creator {creator_id} 的 saved_records 为空，使用 magnet_sku_list 中的数据")
                saved_records = magnet_sku_list
        
        print(f"  处理 Creator {creator_id} 的 {len(saved_records)} 个 magnet...")
        
        # 检查缓存（如果缓存存在，跳过图片生成和数据库更新）
        cached_result = None
        if use_cache:
            cached_result = load_cache(creator_id, "step_three")
            if cached_result:
                print(f"    ✓ 使用缓存结果，跳过图片生成和数据库更新")
                cache_hits += 1
                # 统计缓存的图片数量
                for magnet_result in cached_result.get("magnet_results", []):
                    total_magnets += 1
                    # 统计每张图片的 URL 数量
                    for img in magnet_result.get("images", []):
                        total_images_generated += len(img.get("urls", []))
                
                all_image_results.append(cached_result)
                continue
        
        creator_image_results = []
        
        # 构建一个从 dify_response.magnet_sku_list 获取 front_logo_url 和 front_image_prompt 的映射（作为备用）
        dify_response = creator_result.get("dify_response", {})
        magnet_sku_list = dify_response.get("magnet_sku_list", [])
        front_logo_url_map = {}
        front_image_prompt_map = {}
        for magnet_sku in magnet_sku_list:
            sku_context_id = magnet_sku.get("context_id", "")
            sku_front_logo_url = magnet_sku.get("front_logo_url", "")
            sku_front_image_prompt = magnet_sku.get("front_image_prompt", "")
            if sku_context_id:
                if sku_front_logo_url:
                    front_logo_url_map[sku_context_id] = sku_front_logo_url
                if sku_front_image_prompt:
                    front_image_prompt_map[sku_context_id] = sku_front_image_prompt
        
        # 如果所有 magnet 都使用相同的 creator_signature_image_url，也可以作为备用
        creator = creator_result.get("creator", {})
        creator_signature_image_url = creator.get("creator_signature_image_url", "")
        
        for magnet_idx, magnet_record in enumerate(saved_records, 1):
            # 获取 magnet 信息
            context_id = magnet_record.get("context_id", "")
            front_name = magnet_record.get("front_name", "")
            front_logo_url = magnet_record.get("front_logo_url", "")
            front_image_prompt = magnet_record.get("front_image_prompt", "")
            
            # 如果 front_logo_url 为空，尝试从 magnet_sku_list 中查找
            if not front_logo_url:
                if context_id in front_logo_url_map:
                    front_logo_url = front_logo_url_map[context_id]
                    print(f"    ℹ️  Magnet {magnet_idx} (context_id: {context_id}) 从 magnet_sku_list 中获取 front_logo_url")
                elif creator_signature_image_url:
                    # 如果还是没有，使用 creator 的 signature image URL
                    front_logo_url = creator_signature_image_url
                    print(f"    ℹ️  Magnet {magnet_idx} (context_id: {context_id}) 使用 creator_signature_image_url")
            
            # 如果 front_image_prompt 为空，尝试从 magnet_sku_list 中查找
            if not front_image_prompt:
                if context_id in front_image_prompt_map:
                    front_image_prompt = front_image_prompt_map[context_id]
                    print(f"    ℹ️  Magnet {magnet_idx} (context_id: {context_id}) 从 magnet_sku_list 中获取 front_image_prompt")
            
            if not front_name:
                print(f"    ⚠ Magnet {magnet_idx} (context_id: {context_id}) 缺少 front_name，跳过")
                continue
            
            # front_image_prompt 是必需的，如果找不到应该报错
            if not front_image_prompt:
                error_msg = f"Magnet {magnet_idx} (context_id: {context_id}) 缺少 front_image_prompt，无法生成图片"
                print(f"    ❌ {error_msg}")
                errors.append({
                    "context_id": context_id,
                    "error": error_msg
                })
                continue
            
            # 使用 front_logo_url 作为 image_url，如果没有则使用默认值
            image_url = front_logo_url if front_logo_url else default_image_url
            
            # 使用从 magnet_sku_list 或 saved_records 中获取的 front_image_prompt
            prompt = front_image_prompt
            
            print(f"    处理 Magnet {magnet_idx}/{len(saved_records)}: {front_name} (context_id: {context_id})")
            print(f"      Image URL: {image_url}")
            print(f"      Prompt 长度: {len(prompt)} 字符")
            
            # 为每个 magnet 生成指定数量的图片
            magnet_images = []
            magnet_errors = []
            
            for image_idx in range(1, images_per_magnet + 1):
                max_retries = 4  # 总共4次尝试：1次初始 + 3次重试
                retry_count = 0
                success = False
                last_error = None
                
                while retry_count < max_retries and not success:
                    try:
                        if retry_count > 0:
                            print(f"      生成图片 {image_idx}/{images_per_magnet}... (重试 {retry_count}/{max_retries - 1})")
                            # 重试前等待：前2次重试等待2秒，第3次重试等待10秒
                            if retry_count >= 3:
                                wait_time = 10
                                print(f"        ⏳ 等待 {wait_time} 秒后重试...")
                            else:
                                wait_time = 2
                            time.sleep(wait_time)
                        else:
                            print(f"      生成图片 {image_idx}/{images_per_magnet}...")
                        
                        image_result = generate_image(
                            prompt=prompt,
                            image_url=image_url,
                            api_url=image_api_url,
                            key_prefix=key_prefix
                        )
                        
                        # 每张图片可能有多个 URL（如果 API 返回多个）
                        image_urls = image_result.get("urls", [])
                        if image_urls:
                            magnet_images.append({
                                "image_index": image_idx,
                                "urls": image_urls,
                                "texts": image_result.get("texts", []),
                                "status": "success"
                            })
                            total_images_generated += len(image_urls)
                            print(f"        ✓ 成功生成 {len(image_urls)} 张图片")
                            # 打印每张图片的详细信息
                            for url_idx, url in enumerate(image_urls, 1):
                                print(f"          📷 图片 {url_idx}: {url}")
                            success = True
                        else:
                            raise ValueError("API 返回的图片 URL 列表为空")
                            
                    except Exception as e:
                        last_error = e
                        error_msg = str(e)
                        retry_count += 1
                        if retry_count < max_retries:
                            # 根据重试次数显示不同的等待时间
                            if retry_count >= 3:
                                wait_time_msg = "10 秒"
                            else:
                                wait_time_msg = "2 秒"
                            print(f"        ⚠️  生成图片 {image_idx} 失败: {error_msg}，将在 {wait_time_msg} 后重试 ({retry_count}/{max_retries - 1})")
                        else:
                            print(f"        ❌ 生成图片 {image_idx} 失败: {error_msg}（已重试 {max_retries - 1} 次）")
                
                # 如果所有重试都失败了，记录错误
                if not success and last_error:
                    magnet_errors.append({
                        "image_index": image_idx,
                        "error": str(last_error),
                        "retry_count": retry_count
                    })
            
            # 更新 Supabase 的 front_image_url（使用第一张成功生成的图片的第一个 URL）
            front_image_url = None
            if magnet_images and len(magnet_images) > 0:
                first_image_urls = magnet_images[0].get("urls", [])
                if first_image_urls and len(first_image_urls) > 0:
                    front_image_url = first_image_urls[0]
            
            # 如果成功生成了图片，更新 Supabase
            if front_image_url and supabase_url and supabase_api_key:
                update_msg = f"🔄 更新 Supabase front_image_url (context_id: {context_id}): {front_image_url}"
                print(f"      {update_msg}")
                log_and_print(creator_id, "step_three", update_msg)
                
                update_success = update_supabase_image_url(
                    creator_id=creator_id,
                    context_id=context_id,
                    front_image_url=front_image_url,
                    type="normal",
                    supabase_url=supabase_url,
                    supabase_api_key=supabase_api_key,
                    magnet_record=magnet_record
                )
                if update_success:
                    success_msg = f"✓ 成功更新 Supabase front_image_url (context_id: {context_id})"
                    print(f"        {success_msg}")
                    log_and_print(creator_id, "step_three", success_msg)
                else:
                    warning_msg = f"⚠️  更新 Supabase front_image_url 失败 (context_id: {context_id})"
                    print(f"        {warning_msg}")
                    log_and_print(creator_id, "step_three", warning_msg, "WARNING")
            elif not front_image_url:
                warning_msg = f"⚠️  没有可用的图片 URL，跳过 Supabase 更新 (context_id: {context_id})"
                print(f"      {warning_msg}")
                log_and_print(creator_id, "step_three", warning_msg, "WARNING")
            elif not supabase_url or not supabase_api_key:
                warning_msg = "⚠️  缺少 Supabase 配置，跳过更新"
                print(f"      {warning_msg}")
                log_and_print(creator_id, "step_three", warning_msg, "WARNING")
            
            creator_image_results.append({
                "context_id": context_id,
                "front_name": front_name,
                "front_logo_url": front_logo_url,
                "front_image_prompt": front_image_prompt,  # 保存 front_image_prompt 到缓存
                "image_url": image_url,
                "front_image_url": front_image_url,  # 保存更新后的 front_image_url
                "images": magnet_images,
                "errors": magnet_errors,
                "image_count": len(magnet_images),
                "error_count": len(magnet_errors)
            })
            
            total_magnets += 1
        
        creator_result_data = {
            "creator_id": creator_id,
            "magnet_results": creator_image_results
        }
        all_image_results.append(creator_result_data)
        
        # 检查是否有成功的图片生成结果，只有成功时才保存缓存
        has_success = False
        for magnet_result in creator_image_results:
            images = magnet_result.get("images", [])
            if images and len(images) > 0:
                # 检查是否有成功的图片（status 为 "success" 或包含 urls）
                for img in images:
                    if img.get("status") == "success" or img.get("urls"):
                        has_success = True
                        break
                if has_success:
                    break
        
        # 为每个 creator 保存缓存（只有成功时才保存）
        if use_cache and has_success:
            save_cache(creator_id, creator_result_data, "step_three")
            print(f"    ✓ 结果已保存到缓存")
        elif use_cache and not has_success:
            print(f"    ⚠️  没有成功的图片生成结果，跳过缓存保存")
        
        # 每个 Creator 处理完成后，立即确认所有 Supabase 更新已完成
        if has_success:
            print(f"  ✓ Creator {creator_id} 的所有图片生成和 Supabase 更新已完成")
        else:
            print(f"  ⚠️  Creator {creator_id} 的图片生成失败，未保存缓存")
    
    # 构建返回结果
    result = {
        "step": 3,
        "status": "completed",
        "message": f"成功为 {total_magnets} 个 magnet 生成图片，共生成 {total_images_generated} 张图片（缓存命中: {cache_hits}）",
        "results": all_image_results,
        "total_magnets": total_magnets,
        "total_images_generated": total_images_generated,
        "errors": errors,
        "cache_hits": cache_hits
    }
    
    return result

