"""
第一步：从 Supabase 获取所有 creator 信息
"""
import os
from typing import Dict, Any, Optional, Tuple, List
import requests

from utils.cache import load_cache, save_cache


def validate(**kwargs) -> Tuple[bool, Optional[str]]:
    """
    第一步校验：检查 Supabase 配置是否完整
    返回: (是否通过, 错误信息)
    """
    # 从 kwargs 或环境变量获取配置
    supabase_url = kwargs.get("supabase_url") or os.getenv("SUPABASE_URL")
    supabase_api_key = kwargs.get("supabase_api_key") or os.getenv("SUPABASE_API_KEY")
    
    if not supabase_url:
        return False, "缺少 Supabase URL 配置（supabase_url 或 SUPABASE_URL 环境变量）"
    
    if not supabase_api_key:
        return False, "缺少 Supabase API Key 配置（supabase_api_key 或 SUPABASE_API_KEY 环境变量）"
    
    return True, None


def execute(**kwargs) -> Dict[str, Any]:
    """
    第一步执行：从 Supabase 获取所有 creator 信息
    返回包含 creator 列表的结果
    """
    # 先校验
    is_valid, error_msg = validate(**kwargs)
    if not is_valid:
        raise ValueError(f"第一步校验失败: {error_msg}")
    
    # 检查缓存（第一步缓存所有 creator 列表，使用特殊标识 "all_creators"）
    use_cache = kwargs.get("use_cache", True)
    if not use_cache:
        print("  ℹ️  禁用缓存模式：将从数据库重新获取 creator 信息")
    if use_cache:
        cached_result = load_cache("all_creators", "step_one")
        if cached_result:
            print("  ✓ 使用缓存结果（所有 creator 列表）")
            return cached_result
    
    # 从 kwargs 或环境变量获取配置
    supabase_url = kwargs.get("supabase_url") or os.getenv("SUPABASE_URL")
    supabase_api_key = kwargs.get("supabase_api_key") or os.getenv("SUPABASE_API_KEY")
    
    # 构建完整的 API URL（获取所有 creator，不添加过滤条件）
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
    
    # 设置请求头
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        # 发送 GET 请求获取所有 creator
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()  # 如果状态码不是 2xx，会抛出异常
        
        creators: List[Dict[str, Any]] = response.json()
        
        # 按 paid_subscribers_est 和 free_subscribers_est 降序排序
        def get_sort_key(creator: Dict[str, Any]) -> tuple:
            """获取用于排序的键值
            优先级：paid_subscribers_est（降序） > free_subscribers_est（降序）
            使用负数实现降序排序
            """
            # 获取付费订阅者数量，如果不存在或无效则视为0
            paid_subscribers = creator.get("paid_subscribers_est")
            try:
                paid_value = float(paid_subscribers) if paid_subscribers is not None else 0.0
            except (ValueError, TypeError):
                paid_value = 0.0
            
            # 获取免费订阅者数量，如果不存在或无效则视为0
            free_subscribers = creator.get("free_subscribers_est")
            try:
                free_value = float(free_subscribers) if free_subscribers is not None else 0.0
            except (ValueError, TypeError):
                free_value = 0.0
            
            # 返回负数以实现降序排序（Python的sort是升序，负数可以实现降序）
            return (-paid_value, -free_value)
        
        creators.sort(key=get_sort_key)
        
        result = {
            "step": 1,
            "status": "completed",
            "message": f"成功获取 {len(creators)} 个 creator 信息",
            "creators": creators,
            "count": len(creators)
        }
        
        # 保存到缓存
        if use_cache:
            save_cache("all_creators", result, "step_one")
            print("  ✓ 结果已保存到缓存")
        
        return result
        
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"从 Supabase 获取 creator 信息失败: {str(e)}") from e
    except ValueError as e:
        raise RuntimeError(f"解析 Supabase 响应失败: {str(e)}") from e

