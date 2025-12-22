"""
Creator状态更新工具
"""
import os
import requests
from typing import Optional, Any


# 状态常量
PENDING = "pending"           # 待生成
GENERATING = "generating"     # 生成中
COMPLETED = "completed"       # 生成完成
FAILED = "failed"             # 生成失败


def update_creator_status(
    creator_id: Any,
    status: str,
    supabase_url: str,
    supabase_api_key: str
) -> bool:
    """
    更新 Supabase creator 表的 status 字段
    
    Args:
        creator_id: Creator ID
        status: 状态值 (pending, generating, completed, failed)
        supabase_url: Supabase URL
        supabase_api_key: Supabase API Key
    
    Returns:
        bool: 更新是否成功
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
    update_url = f"{api_url}?creator_id=eq.{creator_id}"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    payload = {
        "status": status
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
        print(f"        ⚠️  更新 Creator 状态失败 (creator_id: {creator_id}, status: {status}): {error_detail}")
        return False



