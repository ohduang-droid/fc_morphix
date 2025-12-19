#!/usr/bin/env python3
"""
更新 Creator 表中的 message 字段
使用新模板：Hi {{FirstName}} ... {{handle}} ...
"""

import os
import sys
import requests
import re
import argparse
import threading
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed


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


def extract_first_name(creator_name: str) -> str:
    """
    从 creator_name 提取 FirstName（第一个词）
    例如: "Noah Smith" -> "Noah"
    """
    if not creator_name:
        return ""
    # 取第一个词
    first_name = creator_name.split()[0] if creator_name.split() else creator_name
    return first_name.strip()


def extract_handle_from_url(creator_url: str) -> Optional[str]:
    """
    从 creator_url 提取 handle
    例如: "https://substack.com/@noahpinion" -> "noahpinion"
    """
    if not creator_url:
        return None
    
    # 匹配 @ 后面的部分
    match = re.search(r'@([^/]+)', creator_url)
    if match:
        return match.group(1)
    return None


def get_handle(creator: Dict[str, Any]) -> str:
    """
    获取 handle，优先级：
    1. 如果 creator 有 handle 字段且不为空，使用它
    2. 否则从 creator_url 提取
    3. 如果都获取不到，返回空字符串
    """
    # 优先使用 handle 字段
    handle = creator.get("handle")
    if handle:
        # 如果 handle 以 @ 开头，去掉 @
        if handle.startswith("@"):
            return handle[1:]
        return str(handle)
    
    # 从 creator_url 提取
    creator_url = creator.get("creator_url", "")
    if creator_url:
        extracted = extract_handle_from_url(creator_url)
        if extracted:
            return extracted
    
    return ""


def generate_message(first_name: str, handle: str, newsletter_name: Optional[str] = None) -> str:
    """
    使用新模板生成 message
    
    Args:
        first_name: FirstName
        handle: handle
        newsletter_name: Newsletter 名称（可选，如果提供则使用，否则使用 handle）
    """
    # 如果没有提供 newsletter_name，使用 handle
    newsletter = newsletter_name if newsletter_name else handle
    
    message_template = """Hi {{FirstName}}

This is Billy from Fridge Channel. I put together a quick "fridge edition" concept for {{Newsletter}}: https://FridgeChannels.com/{{handle}}

https://calendly.com/billy-fridgechannels/30min (grab any slot that works)

Newsletters don't lose readers because the content isn't good — they lose them because consumption drops (read later = never), which hits conversion + retention

FC adds a household touchpoint and turns each issue into a 1–2 min AI fridge edition, then routes readers to the right next step (open full issue / subscribe when enabled). Net effect: Owned touchpoint → more full-issue opens → better retention → higher LTV.

"""
    
    # 替换模板变量
    message = message_template.replace("{{FirstName}}", first_name)
    message = message.replace("{{Newsletter}}", newsletter)
    message = message.replace("{{handle}}", handle)
    
    return message


def update_creator_message(
    creator_id: str,
    message: str,
    supabase_url: str,
    supabase_api_key: str
) -> bool:
    """
    更新 Supabase creator 表的 message 字段
    
    Args:
        creator_id: Creator ID
        message: 新的 message 内容
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
        "message": message
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
        print(f"        ⚠️  更新 Creator {creator_id} 失败: {error_detail}")
        return False


def get_all_creators(supabase_url: str, supabase_api_key: str, creator_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    从 Supabase 获取 creator 信息
    
    Args:
        supabase_url: Supabase URL
        supabase_api_key: Supabase API Key
        creator_id: 可选的 creator_id，如果指定则只获取该 creator
    
    Returns:
        List[Dict[str, Any]]: Creator 列表
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
    
    # 如果指定了 creator_id，添加过滤条件
    if creator_id:
        api_url = f"{api_url}?creator_id=eq.{creator_id}"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        # 打印调试信息
        if creator_id:
            print(f"    调试: 查询 URL: {api_url}")
            print(f"    调试: 查询 creator_id: {creator_id} (类型: {type(creator_id).__name__})")
        
        response = requests.get(api_url, headers=headers, timeout=30)
        
        # 打印响应状态
        print(f"    调试: HTTP 状态码: {response.status_code}")
        
        response.raise_for_status()
        creators = response.json()
        
        # 打印查询结果
        if creator_id:
            print(f"    调试: 查询结果数量: {len(creators) if isinstance(creators, list) else 'N/A'}")
            if isinstance(creators, list) and len(creators) > 0:
                print(f"    调试: 返回的 creator_id: {creators[0].get('creator_id')} (类型: {type(creators[0].get('creator_id')).__name__})")
        
        return creators if isinstance(creators, list) else []
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_detail = f"HTTP {e.response.status_code}"
            try:
                error_body = e.response.json()
                error_detail += f": {error_body}"
            except:
                error_detail += f": {e.response.text[:500]}"
            print(f"    调试: 响应内容: {e.response.text[:500]}")
        print(f"错误: 获取 Creator 列表失败: {error_detail}")
        return []


def process_single_creator(
    creator: Dict[str, Any],
    creator_idx: int,
    total_creators: int,
    supabase_url: str,
    supabase_api_key: str,
    stats_lock: threading.Lock,
    stats: Dict[str, int],
    skipped_records: List[Dict[str, Any]],
    skipped_records_lock: threading.Lock
) -> Tuple[bool, Dict[str, Any]]:
    """
    处理单个 Creator 的 message 更新（用于并发执行）
    
    Args:
        creator: Creator 信息
        creator_idx: Creator 索引（从1开始）
        total_creators: Creator 总数
        supabase_url: Supabase URL
        supabase_api_key: Supabase API Key
        stats_lock: 统计信息的线程锁
        stats: 统计信息字典（success_count, fail_count, skip_count）
        skipped_records: 跳过的记录列表
        skipped_records_lock: 跳过记录列表的线程锁
    
    Returns:
        (是否成功, 结果字典)
    """
    creator_id = creator.get("creator_id")
    creator_name = creator.get("creator_name", "")
    
    # 检查 creator_id
    if not creator_id:
        reason = "creator_id 为空"
        with stats_lock:
            stats["skip_count"] += 1
        with skipped_records_lock:
            skipped_records.append({
                "index": creator_idx,
                "creator_id": None,
                "creator_name": creator_name or "未知",
                "reason": reason
            })
        print(f"[{creator_idx}/{total_creators}] 跳过: {reason}")
        return False, {"creator_id": None, "reason": reason}
    
    # 检查 creator_name
    if not creator_name:
        reason = "creator_name 为空"
        with stats_lock:
            stats["skip_count"] += 1
        with skipped_records_lock:
            skipped_records.append({
                "index": creator_idx,
                "creator_id": creator_id,
                "creator_name": "未知",
                "reason": reason
            })
        print(f"[{creator_idx}/{total_creators}] 跳过 Creator {creator_id}: {reason}")
        return False, {"creator_id": creator_id, "reason": reason}
    
    # 提取 FirstName
    first_name = extract_first_name(creator_name)
    if not first_name:
        reason = "无法提取 FirstName"
        with stats_lock:
            stats["skip_count"] += 1
        with skipped_records_lock:
            skipped_records.append({
                "index": creator_idx,
                "creator_id": creator_id,
                "creator_name": creator_name,
                "reason": reason
            })
        print(f"[{creator_idx}/{total_creators}] 跳过 Creator {creator_id}: {reason}")
        return False, {"creator_id": creator_id, "reason": reason}
    
    # 获取 handle
    handle = get_handle(creator)
    if not handle:
        reason = f"无法获取 handle (handle字段: {creator.get('handle')}, creator_url: {creator.get('creator_url')})"
        with stats_lock:
            stats["skip_count"] += 1
        with skipped_records_lock:
            skipped_records.append({
                "index": creator_idx,
                "creator_id": creator_id,
                "creator_name": creator_name,
                "reason": reason,
                "handle_field": creator.get("handle"),
                "creator_url": creator.get("creator_url")
            })
        print(f"[{creator_idx}/{total_creators}] 跳过 Creator {creator_id} ({creator_name}): 无法获取 handle")
        return False, {"creator_id": creator_id, "reason": reason}
    
    # 获取 newsletter 名称（如果有的话，可以使用 creator 的其他字段）
    newsletter_name = creator.get("newsletter_name") or creator.get("name") or None
    
    # 生成新的 message
    new_message = generate_message(first_name, handle, newsletter_name)
    
    print(f"[{creator_idx}/{total_creators}] 更新 Creator {creator_id} ({creator_name})...")
    print(f"    FirstName: {first_name}")
    print(f"    Handle: {handle}")
    print(f"    Message 预览: {new_message[:100]}...")
    
    success = update_creator_message(
        creator_id=str(creator_id),
        message=new_message,
        supabase_url=supabase_url,
        supabase_api_key=supabase_api_key
    )
    
    if success:
        print(f"    ✓ 更新成功")
        with stats_lock:
            stats["success_count"] += 1
        return True, {"creator_id": creator_id, "creator_name": creator_name}
    else:
        print(f"    ✗ 更新失败")
        with stats_lock:
            stats["fail_count"] += 1
        return False, {"creator_id": creator_id, "creator_name": creator_name}


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description="更新 Creator 表中的 message 字段",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 更新所有 creator
  python update_creator_messages.py
  
  # 只更新指定的 creator
  python update_creator_messages.py --creator-id 123
  
  # 指定并发数
  python update_creator_messages.py --max-workers 5
        """
    )
    parser.add_argument(
        "--creator-id",
        type=str,
        help="指定要更新的 creator_id（如果不指定，则更新所有 creator）"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=3,
        help="最大并发数（默认: 3）"
    )
    
    args = parser.parse_args()
    
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
    
    # 根据是否指定 creator_id 显示不同的提示
    if args.creator_id:
        print(f"正在从 Supabase 获取 Creator 信息 (creator_id: {args.creator_id})...")
        # 先尝试直接查询
        creators = get_all_creators(supabase_url, supabase_api_key, creator_id=args.creator_id)
        
        # 如果没找到，尝试获取所有 creator 然后过滤（处理类型不匹配的情况）
        if not creators:
            print(f"    未找到匹配的 creator，尝试获取所有 creator 后过滤...")
            all_creators = get_all_creators(supabase_url, supabase_api_key)
            # 尝试多种类型匹配
            target_id_str = str(args.creator_id)
            target_id_int = None
            try:
                target_id_int = int(args.creator_id)
            except (ValueError, TypeError):
                pass
            
            creators = []
            for creator in all_creators:
                cid = creator.get("creator_id")
                if cid is None:
                    continue
                # 尝试字符串和整数匹配
                if str(cid) == target_id_str or cid == target_id_int:
                    creators.append(creator)
                    break
            
            if creators:
                print(f"    ✓ 通过过滤找到 creator (creator_id: {creators[0].get('creator_id')})")
    else:
        print("正在从 Supabase 获取所有 Creator 信息...")
        creators = get_all_creators(supabase_url, supabase_api_key)
    
    if not creators:
        if args.creator_id:
            print(f"错误: 未找到 creator_id 为 {args.creator_id} 的 Creator")
        else:
            print("错误: 未获取到任何 Creator 信息")
        sys.exit(1)
    
    print(f"找到 {len(creators)} 个 Creator 记录")
    print(f"开始并发更新 message 字段（并发数: {args.max_workers}）...")
    print("-" * 60)
    
    # 统计信息（使用字典和锁来保证线程安全）
    stats = {
        "success_count": 0,
        "fail_count": 0,
        "skip_count": 0
    }
    stats_lock = threading.Lock()
    skipped_records = []  # 记录跳过的记录详情（需要线程锁保护）
    skipped_records_lock = threading.Lock()
    
    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # 提交所有任务
        future_to_creator = {}
        for idx, creator in enumerate(creators, 1):
            future = executor.submit(
                process_single_creator,
                creator=creator,
                creator_idx=idx,
                total_creators=len(creators),
                supabase_url=supabase_url,
                supabase_api_key=supabase_api_key,
                stats_lock=stats_lock,
                stats=stats,
                skipped_records=skipped_records,
                skipped_records_lock=skipped_records_lock
            )
            future_to_creator[future] = (idx, creator)
        
        # 等待所有任务完成并收集结果
        completed_count = 0
        for future in as_completed(future_to_creator):
            creator_idx, creator = future_to_creator[future]
            completed_count += 1
            
            try:
                success, result = future.result()
            except Exception as e:
                creator_id = creator.get("creator_id", "未知")
                print(f"[{creator_idx}/{len(creators)}] ❌ Creator {creator_id} 处理异常: {str(e)}")
                with stats_lock:
                    stats["fail_count"] += 1
            
            # 打印进度
            if completed_count % 10 == 0 or completed_count == len(creators):
                print(f"\n[进度] {completed_count}/{len(creators)} 个Creator处理完成（成功: {stats['success_count']}, 失败: {stats['fail_count']}, 跳过: {stats['skip_count']}）\n")
    
    # 从 stats 字典获取统计信息
    success_count = stats["success_count"]
    fail_count = stats["fail_count"]
    skip_count = stats["skip_count"]
    
    # 输出统计信息
    print("-" * 60)
    print("更新完成!")
    print(f"总计: {len(creators)} 条记录")
    print(f"成功: {success_count} 条")
    print(f"失败: {fail_count} 条")
    print(f"跳过: {skip_count} 条")
    
    # 如果有跳过的记录，输出详细信息
    if skipped_records:
        print("\n" + "=" * 60)
        print("跳过的记录详情:")
        print("=" * 60)
        for record in skipped_records:
            print(f"\n[{record['index']}/{len(creators)}] Creator ID: {record['creator_id']}")
            print(f"    Creator Name: {record['creator_name']}")
            print(f"    跳过原因: {record['reason']}")
            if 'handle_field' in record:
                print(f"    Handle 字段: {record.get('handle_field', '无')}")
                print(f"    Creator URL: {record.get('creator_url', '无')}")


if __name__ == "__main__":
    main()

