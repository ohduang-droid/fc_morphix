"""
第二步：使用每个 Creator 信息调用 Dify 接口生成 prompt，并写入 Supabase
"""
import os
import json
from typing import Dict, Any, Optional, Tuple, List
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
    第二步校验：检查 creators 列表、Dify 配置和 Supabase 配置
    返回: (是否通过, 错误信息)
    """
    # 加载 .env 文件
    load_env_file()
    
    # 检查 creators 列表
    creators = kwargs.get("creators")
    if not creators:
        return False, "缺少 creators 列表（第一步应提供 creators）"
    
    if not isinstance(creators, list):
        return False, "creators 必须是列表类型"
    
    if len(creators) == 0:
        return False, "creators 列表为空"
    
    # 检查 Dify 配置
    dify_url = kwargs.get("dify_url") or os.getenv("DIFY_URL")
    dify_api_key = kwargs.get("dify_api_key") or os.getenv("DIFY_API_KEY")
    dify_user = kwargs.get("dify_user") or os.getenv("DIFY_USER", "task-executor")
    
    if not dify_url:
        return False, "缺少 Dify URL 配置（dify_url 或 DIFY_URL 环境变量）"
    
    if not dify_api_key:
        return False, "缺少 Dify API Key 配置（dify_api_key 或 DIFY_API_KEY 环境变量）"
    
    # 检查 Supabase 配置
    supabase_url = kwargs.get("supabase_url") or os.getenv("SUPABASE_URL")
    supabase_api_key = kwargs.get("supabase_api_key") or os.getenv("SUPABASE_API_KEY")
    
    if not supabase_url:
        return False, "缺少 Supabase URL 配置（supabase_url 或 SUPABASE_URL 环境变量）"
    
    if not supabase_api_key:
        return False, "缺少 Supabase API Key 配置（supabase_api_key 或 SUPABASE_API_KEY 环境变量）"
    
    return True, None


def parse_sse_response(response: requests.Response) -> str:
    """
    解析 Dify 流式响应（Server-Sent Events 格式）
    返回完整的 JSON 字符串
    """
    full_text = ""
    event_type = None
    
    for line in response.iter_lines(decode_unicode=True):
        if not line:
            continue
        
        # SSE 格式：event: xxx 或 data: {...}
        if line.startswith("event: "):
            event_type = line[7:].strip()
            continue
        
        if line.startswith("data: "):
            data_str = line[6:]  # 移除 "data: " 前缀
            
            # 跳过特殊事件
            if data_str == "[DONE]" or data_str.strip() == "":
                continue
            
            try:
                data = json.loads(data_str)
                
                # 根据 Dify API 的响应格式提取文本
                # 可能的格式：
                # 1. {"answer": "text"}
                # 2. {"message": {"answer": "text"}}
                # 3. {"text": "text"}
                # 4. {"event": "message", "answer": "text"}
                
                if "answer" in data:
                    answer = data["answer"]
                    if isinstance(answer, str):
                        full_text += answer
                elif "message" in data:
                    if isinstance(data["message"], dict) and "answer" in data["message"]:
                        full_text += data["message"]["answer"]
                    elif isinstance(data["message"], str):
                        full_text += data["message"]
                elif "text" in data:
                    full_text += data["text"]
                elif "content" in data:
                    full_text += data["content"]
                    
            except json.JSONDecodeError:
                # 如果不是 JSON 格式，可能是纯文本数据
                if data_str and not data_str.startswith("{"):
                    full_text += data_str
    
    return full_text.strip()


def parse_dify_response(response_text: str) -> Dict[str, Any]:
    """
    解析 Dify 返回的 JSON 响应
    期望格式：
    {
      "outlook": "...",
      "video_prompt": "...",
      "magnet_sku_list": [...]
    }
    """
    try:
        # 尝试直接解析 JSON
        data = json.loads(response_text)
        return data
    except json.JSONDecodeError:
        # 如果失败，尝试提取 JSON 对象
        # 查找第一个 { 和最后一个 }
        start_idx = response_text.find("{")
        end_idx = response_text.rfind("}")
        
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            json_str = response_text[start_idx:end_idx + 1]
            try:
                data = json.loads(json_str)
                return data
            except json.JSONDecodeError:
                pass
        
        raise ValueError(f"无法解析 Dify 响应为 JSON: {response_text[:200]}")


def update_creator_outreach_email_body(
    creator_id: Any,
    outlook_text: str,
    supabase_url: str,
    supabase_api_key: str,
    message_text: Optional[str] = None
) -> bool:
    """
    更新 Supabase creator 表的 outreach_email_body 和 message 字段
    从 dify_response.outlook 和 dify_response.message 字段提取内容，清理 markdown 代码块标记后写入
    """
    def clean_markdown(text: str) -> str:
        """清理 markdown 代码块标记"""
        cleaned = text.strip()
        # 移除开头的 ``` 和可能的语言标识符（如 ```markdown, ```text 等）
        if cleaned.startswith("```"):
            # 找到第一个换行符
            first_newline = cleaned.find("\n")
            if first_newline != -1:
                cleaned = cleaned[first_newline + 1:]
            else:
                # 如果没有换行符，直接移除开头的 ```
                cleaned = cleaned[3:]
        # 移除结尾的 ```
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].rstrip()
        # 再次清理首尾空白
        return cleaned.strip()
    
    # 清理 outlook 文本
    cleaned_outlook = clean_markdown(outlook_text) if outlook_text else ""
    
    # 清理 message 文本（如果提供）
    cleaned_message = clean_markdown(message_text) if message_text else None
    
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
    update_url = f"{api_url}?creator_id=eq.{creator_id}"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    # 构建 payload，同时更新 outreach_email_body 和 message
    payload = {
        "outreach_email_body": cleaned_outlook
    }
    
    # 如果提供了 message_text，则同时更新 message 字段
    if cleaned_message is not None:
        payload["message"] = cleaned_message
    
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
        # 注意：这里不记录日志，因为 creator_id 可能还未确定
        print(f"        ⚠️  更新 Supabase creator.outreach_email_body 和 message 失败: {error_detail}")
        return False


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
    
    # 构建查询URL：第二步只使用 creator_id + context_id
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


def write_to_supabase(
    magnet_data: Dict[str, Any],
    creator_id: Any,
    task_id: Any,
    supabase_url: str,
    supabase_api_key: str
) -> Dict[str, Any]:
    """
    将单个 magnet 数据写入 Supabase
    写入前先检查是否存在（基于 creator_id + context_id + type），如果存在则更新，否则插入
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/magnet_image"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    # 获取 type 字段，如果不存在则默认为 "normal"
    record_type = magnet_data.get("type", "normal")
    context_id = magnet_data.get("context_id", "")
    
    payload = {
        "task_id": task_id,
        "context_id": context_id,
        "creator_id": str(creator_id),
        "front_name": magnet_data.get("front_name", ""),
        # "front_logo_url": magnet_data.get("front_logo_url", ""),
        "front_style_key": magnet_data.get("front_style_key", ""),
        "front_image_prompt": magnet_data.get("front_image_prompt", ""),
        "type": record_type  # 包含 type 字段以确保联合唯一性
    }
    
    try:
        # 先检查记录是否存在（基于 creator_id + context_id + type）
        exists, existing_record = check_record_exists(
            creator_id=creator_id,
            context_id=context_id,
            supabase_url=supabase_url,
            supabase_api_key=supabase_api_key,
            type=record_type  # 检查时包含 type 字段，确保唯一约束匹配
        )
        
        if exists:
            # 记录已存在，进行更新
            # 更新时使用 creator_id + context_id + type 作为查询条件，确保唯一性
            update_url = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&type=eq.{record_type}"
            
            try:
                update_response = requests.patch(update_url, headers=headers, json=payload, timeout=30)
                
                if not update_response.ok:
                    error_detail = f"HTTP {update_response.status_code}"
                    try:
                        error_body = update_response.json()
                        error_detail += f": {error_body}"
                    except:
                        error_detail += f": {update_response.text[:200]}"
                    raise RuntimeError(f"更新 Supabase 失败 - {error_detail}")
                
                update_response.raise_for_status()
                result = update_response.json()
                
                # Supabase 可能返回数组或单个对象
                if isinstance(result, list):
                    result = result[0] if result else {}
                # 标记为更新操作
                if isinstance(result, dict):
                    result["_operation"] = "updated"
                return result
            except requests.exceptions.RequestException as e:
                raise RuntimeError(f"记录已存在但更新失败 (creator_id={creator_id}, context_id={context_id}, type={record_type}): {str(e)}") from e
        else:
            # 记录不存在，进行插入
            try:
                response = requests.post(api_url, headers=headers, json=payload, timeout=30)
                
                # 如果插入失败
                if not response.ok:
                    # 如果是唯一约束冲突（HTTP 409），转为更新操作
                    if response.status_code == 409:
                        print(f"        ℹ️  插入时发现记录已存在（唯一约束冲突），转为更新操作 (creator_id={creator_id}, context_id={context_id}, type={record_type})")
                        # 尝试更新现有记录
                        update_url = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&type=eq.{record_type}"
                        update_response = requests.patch(update_url, headers=headers, json=payload, timeout=30)
                        
                        if not update_response.ok:
                            error_detail = f"HTTP {update_response.status_code}"
                            try:
                                error_body = update_response.json()
                                error_detail += f": {error_body}"
                            except:
                                error_detail += f": {update_response.text[:200]}"
                            raise RuntimeError(f"更新 Supabase 失败 - {error_detail}")
                        
                        update_response.raise_for_status()
                        result = update_response.json()
                        
                        # Supabase 可能返回数组或单个对象
                        if isinstance(result, list):
                            result = result[0] if result else {}
                        # 标记为更新操作
                        if isinstance(result, dict):
                            result["_operation"] = "updated"
                        return result
                    else:
                        # 其他错误，直接抛出
                        error_detail = f"HTTP {response.status_code}"
                        try:
                            error_body = response.json()
                            error_detail += f": {error_body}"
                        except:
                            error_detail += f": {response.text[:200]}"
                        raise RuntimeError(f"写入 Supabase 失败 - {error_detail}")
                
                # 插入成功
                response.raise_for_status()
                result = response.json()
                
                # Supabase 可能返回数组或单个对象
                if isinstance(result, list):
                    result = result[0] if result else {}
                
                # 标记为插入操作
                if isinstance(result, dict):
                    result["_operation"] = "inserted"
                
                return result
            except requests.exceptions.RequestException as e:
                # 如果请求异常且是 409 错误，尝试更新
                if hasattr(e, 'response') and e.response is not None and e.response.status_code == 409:
                    print(f"        ℹ️  插入时发现记录已存在（唯一约束冲突），转为更新操作 (creator_id={creator_id}, context_id={context_id}, type={record_type})")
                    # 尝试更新现有记录
                    update_url = f"{api_url}?creator_id=eq.{creator_id}&context_id=eq.{context_id}&type=eq.{record_type}"
                    try:
                        update_response = requests.patch(update_url, headers=headers, json=payload, timeout=30)
                        update_response.raise_for_status()
                        result = update_response.json()
                        
                        # Supabase 可能返回数组或单个对象
                        if isinstance(result, list):
                            result = result[0] if result else {}
                        # 标记为更新操作
                        if isinstance(result, dict):
                            result["_operation"] = "updated"
                        return result
                    except requests.exceptions.RequestException as update_e:
                        raise RuntimeError(f"插入失败后更新也失败 (creator_id={creator_id}, context_id={context_id}, type={record_type}): {str(update_e)}") from update_e
                else:
                    raise RuntimeError(f"写入 Supabase 请求异常: {str(e)}") from e
        
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"写入 Supabase 请求异常: {str(e)}") from e
    except Exception as e:
        raise RuntimeError(f"写入 Supabase 失败: {str(e)}") from e


def call_dify_api_and_save(
    creator: Dict[str, Any],
    dify_url: str,
    dify_api_key: str,
    dify_user: str,
    supabase_url: str,
    supabase_api_key: str,
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    为单个 creator 调用 Dify API 生成 prompt，并写入 Supabase
    如果 use_cache 为 True，会先检查缓存，如果存在则直接使用缓存结果
    """
    # 获取 creator_id
    creator_id = creator.get("creator_id") or creator.get("id")
    if not creator_id:
        raise ValueError(f"Creator 缺少 creator_id 或 id 字段: {creator}")
    
    # 构建 API URL
    api_url = f"{dify_url.rstrip('/')}/v1/chat-messages"
    
    # 设置请求头
    headers = {
        "Authorization": f"Bearer {dify_api_key}",
        "Content-Type": "application/json"
    }
    
    # 构建请求体
    payload = {
        "inputs": {
            "creator_id": creator_id
        },
        "query": "creator name",
        "response_mode": "streaming",
        "conversation_id": "",
        "user": dify_user
    }
    
    try:
        # 发送 POST 请求
        response = requests.post(api_url, headers=headers, json=payload, timeout=60, stream=True)
        response.raise_for_status()
        
        # 解析流式响应
        response_text = parse_sse_response(response)
        
        if not response_text:
            raise ValueError(f"从 Dify API 获取的响应为空（creator_id: {creator_id}）")
        
        # 解析 JSON 响应
        dify_data = parse_dify_response(response_text)
        
        # 打印解析后的数据结构（用于调试）
        print(f"    Dify 响应解析成功，包含字段: {list(dify_data.keys())}")
        
        # 验证必需字段：task_id（顶层必需字段）
        if "task_id" not in dify_data:
            raise ValueError(f"Dify 响应缺少必需的 task_id 字段（creator_id: {creator_id}）。可用字段: {list(dify_data.keys())}")
        
        task_id = dify_data.get("task_id")
        if task_id is None or task_id == "":
            raise ValueError(f"Dify 响应中的 task_id 为空（creator_id: {creator_id}）")
        
        print(f"    任务 ID: {task_id}")
        
        # 验证必需字段：outlook
        if "outlook" not in dify_data:
            raise ValueError(f"Dify 响应缺少必需的 outlook 字段（creator_id: {creator_id}）")
        
        # 验证必需字段：video_prompt
        if "video_prompt" not in dify_data:
            raise ValueError(f"Dify 响应缺少必需的 video_prompt 字段（creator_id: {creator_id}）")
        
        # 验证必需字段：magnet_sku_list
        if "magnet_sku_list" not in dify_data:
            raise ValueError(f"Dify 响应缺少必需的 magnet_sku_list 字段（creator_id: {creator_id}）。可用字段: {list(dify_data.keys())}")
        
        magnet_sku_list = dify_data.get("magnet_sku_list", [])
        if not isinstance(magnet_sku_list, list):
            raise ValueError(f"magnet_sku_list 必须是数组类型（creator_id: {creator_id}），实际类型: {type(magnet_sku_list)}")
        
        print(f"    找到 {len(magnet_sku_list)} 个 magnet 记录")
        
        # 写入 Supabase
        saved_records = []
        save_errors = []
        
        print(f"    准备写入 {len(magnet_sku_list)} 个 magnet 记录到 Supabase...")
        log_and_print(creator_id, "step_two", f"准备写入 {len(magnet_sku_list)} 个 magnet 记录到 Supabase...")
        
        for idx, magnet_data in enumerate(magnet_sku_list, 1):
            try:
                record_type = magnet_data.get("type", "normal")
                context_id = magnet_data.get('context_id', 'N/A')
                front_name = magnet_data.get('front_name', 'N/A')
                
                log_msg = f"写入 magnet {idx}/{len(magnet_sku_list)}: task_id={task_id}, context_id={context_id}, type={record_type}, front_name={front_name}"
                print(f"      {log_msg}")
                log_and_print(creator_id, "step_two", log_msg)
                
                saved_record = write_to_supabase(
                    magnet_data,
                    creator_id,
                    task_id,
                    supabase_url,
                    supabase_api_key
                )
                saved_records.append(saved_record)
                
                # 判断是插入还是更新
                operation = saved_record.get("_operation", "inserted")
                if operation == "updated":
                    success_msg = f"✓ 成功更新 magnet {idx} (context_id: {context_id})"
                    print(f"        {success_msg}")
                    log_and_print(creator_id, "step_two", success_msg)
                else:
                    success_msg = f"✓ 成功插入 magnet {idx} (context_id: {context_id})"
                    print(f"        {success_msg}")
                    log_and_print(creator_id, "step_two", success_msg)
            except Exception as e:
                error_msg = str(e)
                error_log = f"❌ 保存失败 magnet {idx}: {error_msg}"
                print(f"        {error_log}")
                log_and_print(creator_id, "step_two", error_log, "ERROR")
                save_errors.append({
                    "magnet_data": magnet_data,
                    "error": error_msg
                })
        
        result = {
            "creator_id": creator_id,
            "creator": creator,
            "task_id": task_id,
            "dify_response": dify_data,
            "outlook": dify_data.get("outlook", ""),
            "video_prompt": dify_data.get("video_prompt", ""),
            "saved_records": saved_records,
            "save_errors": save_errors,
            "magnet_count": len(magnet_sku_list),
            "saved_count": len(saved_records),
            "error_count": len(save_errors),
            "status": "success"
        }
        
        # 保存到缓存
        if use_cache:
            save_cache(creator_id, result, "step_two")
            print(f"    ✓ 结果已保存到缓存")
        
        return result
        
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"调用 Dify API 失败（creator_id: {creator_id}）: {str(e)}") from e


def execute(**kwargs) -> Dict[str, Any]:
    """
    第二步执行：为每个 Creator 调用 Dify 接口生成 prompt，并写入 Supabase
    """
    # 先校验
    is_valid, error_msg = validate(**kwargs)
    if not is_valid:
        raise ValueError(f"第二步校验失败: {error_msg}")
    
    # 加载 .env 文件
    load_env_file()
    
    # 获取配置
    creators = kwargs.get("creators", [])
    dify_url = kwargs.get("dify_url") or os.getenv("DIFY_URL")
    dify_api_key = kwargs.get("dify_api_key") or os.getenv("DIFY_API_KEY")
    dify_user = kwargs.get("dify_user") or os.getenv("DIFY_USER", "task-executor")
    supabase_url = kwargs.get("supabase_url") or os.getenv("SUPABASE_URL")
    supabase_api_key = kwargs.get("supabase_api_key") or os.getenv("SUPABASE_API_KEY")
    use_cache = kwargs.get("use_cache", True)  # 默认使用缓存
    if not use_cache:
        print("  ℹ️  禁用缓存模式：将重新调用 Dify API 生成 prompt")
    
    # 为每个 creator 调用 Dify API 并写入 Supabase
    results = []
    errors = []
    total_magnets = 0
    total_saved = 0
    cache_hits = 0
    
    for i, creator in enumerate(creators, 1):
        creator_id = creator.get("creator_id") or creator.get("id")
        try:
            print(f"  处理 Creator {i}/{len(creators)} (creator_id: {creator_id})...")
            
            # 检查是否使用缓存
            is_from_cache = False
            if use_cache:
                cached_result = load_cache(creator_id, "step_two")
                if cached_result:
                    print(f"    ✓ 使用缓存结果，跳过 Dify API 调用和数据库更新")
                    result = cached_result
                    cache_hits += 1
                    is_from_cache = True
                else:
                    result = call_dify_api_and_save(
                        creator,
                        dify_url,
                        dify_api_key,
                        dify_user,
                        supabase_url,
                        supabase_api_key,
                        use_cache=use_cache
                    )
            else:
                result = call_dify_api_and_save(
                    creator,
                    dify_url,
                    dify_api_key,
                    dify_user,
                    supabase_url,
                    supabase_api_key,
                    use_cache=False
                )
            results.append(result)
            total_magnets += result.get("magnet_count", 0)
            total_saved += result.get("saved_count", 0)
            
            saved_count = result.get('saved_count', 0)
            magnet_count = result.get('magnet_count', 0)
            error_count = result.get('error_count', 0)
            
            if error_count > 0:
                print(f"    ⚠ 保存结果: {saved_count}/{magnet_count} 成功，{error_count} 失败")
                # 打印前3个错误详情
                save_errors = result.get('save_errors', [])
                for err_idx, err_info in enumerate(save_errors[:3], 1):
                    print(f"      错误 {err_idx}: {err_info.get('error', '未知错误')}")
                if len(save_errors) > 3:
                    print(f"      ... 还有 {len(save_errors) - 3} 个错误")
            else:
                print(f"    ✓ 成功保存 {saved_count}/{magnet_count} 个 magnet")
            
            # 更新 creator 表的 outreach_email_body 和 message 字段
            # 如果使用缓存，跳过数据库更新
            if not is_from_cache and supabase_url and supabase_api_key:
                dify_response = result.get("dify_response", {})
                outlook_text = dify_response.get("outlook", "")
                message_text = dify_response.get("message", "")
                if outlook_text:
                    update_msg = "🔄 更新 creator 表的 outreach_email_body 和 message 字段..."
                    print(f"    {update_msg}")
                    log_and_print(creator_id, "step_two", update_msg)
                    
                    update_success = update_creator_outreach_email_body(
                        creator_id,
                        outlook_text,
                        supabase_url,
                        supabase_api_key,
                        message_text=message_text if message_text else None
                    )
                    if update_success:
                        success_msg = "✓ 成功更新 creator.outreach_email_body 和 message"
                        print(f"        {success_msg}")
                        log_and_print(creator_id, "step_two", success_msg)
                    else:
                        warning_msg = "⚠️  更新 creator.outreach_email_body 和 message 失败"
                        print(f"        {warning_msg}")
                        log_and_print(creator_id, "step_two", warning_msg, "WARNING")
                else:
                    warning_msg = "⚠️  dify_response.outlook 字段为空，跳过更新"
                    print(f"        {warning_msg}")
                    log_and_print(creator_id, "step_two", warning_msg, "WARNING")
            elif is_from_cache:
                print(f"    ℹ️  使用缓存结果，跳过数据库更新")
        except Exception as e:
            error_info = {
                "creator": creator,
                "error": str(e)
            }
            errors.append(error_info)
            print(f"  ❌ Creator {i} 处理失败: {str(e)}")
    
    # 构建返回结果
    result = {
        "step": 2,
        "status": "completed",
        "message": f"成功处理 {len(results)}/{len(creators)} 个 creator，共保存 {total_saved} 个 magnet 记录（缓存命中: {cache_hits}）",
        "results": results,
        "errors": errors,
        "total": len(creators),
        "success_count": len(results),
        "error_count": len(errors),
        "total_magnets": total_magnets,
        "total_saved": total_saved,
        "cache_hits": cache_hits
    }
    
    return result

