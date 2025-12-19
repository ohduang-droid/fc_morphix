#!/usr/bin/env python3
"""
更新 Creator 表中的 website_url 和 handle 字段
数据来源：json/creator/output.json
"""

import json
import os
import sys
import requests
from typing import Dict, Any, List
from pathlib import Path


def load_env_file(path: str = ".env"):
    """从 .env 文件加载环境变量"""
    if not os.path.exists(path):
        return
    
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            
            # 处理 export 开头的行
            if line.startswith("export "):
                line = line[len("export "):]
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"\'')
            os.environ[key] = value


def update_creator_fields(
    creator_id: str,
    website_url: str,
    handle: str,
    supabase_url: str,
    supabase_api_key: str
) -> bool:
    """
    更新 Supabase creator 表的 website_url 和 handle 字段
    
    Args:
        creator_id: Creator ID
        website_url: 网站 URL
        handle: Handle 值
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
    
    # 构建 payload
    payload = {}
    if website_url:
        payload["website_url"] = website_url
    if handle:
        payload["handle"] = handle
    
    # 如果没有要更新的字段，跳过
    if not payload:
        return False
    
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
        print(f"        ⚠️  更新 Creator {creator_id} 失败: {error_detail}")
        return False


def main():
    """主函数"""
    # 加载环境变量
    load_env_file()
    
    # 从环境变量获取 Supabase 配置
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_api_key = os.getenv("SUPABASE_API_KEY")
    
    if not supabase_url:
        print("错误: 未找到 SUPABASE_URL 环境变量")
        sys.exit(1)
    
    if not supabase_api_key:
        print("错误: 未找到 SUPABASE_API_KEY 环境变量")
        sys.exit(1)
    
    # 读取 JSON 文件
    json_file = Path("json/creator/output.json")
    if not json_file.exists():
        print(f"错误: 文件不存在 {json_file}")
        sys.exit(1)
    
    print(f"正在读取 {json_file}...")
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            creators = json.load(f)
    except Exception as e:
        print(f"错误: 读取 JSON 文件失败: {e}")
        sys.exit(1)
    
    if not isinstance(creators, list):
        print("错误: JSON 文件格式不正确，应为数组")
        sys.exit(1)
    
    print(f"找到 {len(creators)} 个 Creator 记录")
    print("开始更新...")
    print("-" * 60)
    
    # 统计信息
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    # 遍历每个 creator 并更新
    for idx, creator in enumerate(creators, 1):
        creator_id = creator.get("creator_id")
        website_url = creator.get("website_url", "")
        handle = creator.get("handle", "")
        
        if not creator_id:
            print(f"[{idx}/{len(creators)}] 跳过: creator_id 为空")
            skip_count += 1
            continue
        
        # 检查是否有要更新的字段
        if not website_url and not handle:
            print(f"[{idx}/{len(creators)}] 跳过 Creator {creator_id}: website_url 和 handle 都为空")
            skip_count += 1
            continue
        
        creator_name = creator.get("creator_name", "未知")
        print(f"[{idx}/{len(creators)}] 更新 Creator {creator_id} ({creator_name})...")
        print(f"    website_url: {website_url}")
        print(f"    handle: {handle}")
        
        success = update_creator_fields(
            creator_id=str(creator_id),
            website_url=website_url,
            handle=handle,
            supabase_url=supabase_url,
            supabase_api_key=supabase_api_key
        )
        
        if success:
            print(f"    ✓ 更新成功")
            success_count += 1
        else:
            print(f"    ✗ 更新失败")
            fail_count += 1
        
        print()
    
    # 输出统计信息
    print("-" * 60)
    print("更新完成!")
    print(f"总计: {len(creators)} 条记录")
    print(f"成功: {success_count} 条")
    print(f"失败: {fail_count} 条")
    print(f"跳过: {skip_count} 条")


if __name__ == "__main__":
    main()

