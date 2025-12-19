"""
更新没有 magnet_image 记录（且 front_image_url 不为空）的 creator 状态为待处理

脚本功能：
1. 从 creator 表获取所有 creator_id
2. 对于每个 creator_id，查询 magnet_image 表
3. 如果查询不到结果，或者所有记录的 front_image_url 都为空，则更新 creator 的状态为 pending（待处理）
"""
import os
import sys
import requests
from typing import List, Dict, Any, Tuple, Optional
from utils.creator_status import update_creator_status, PENDING


def load_env_file(path: str = ".env"):
    """Load environment variables from a .env file (supports `export KEY=value`)."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    possible_paths = [
        path,
        os.path.join(script_dir, path),
        os.path.join(os.getcwd(), path),
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


def get_all_creator_ids(supabase_url: str, supabase_api_key: str) -> List[str]:
    """
    从 creator 表获取所有 creator_id
    
    Args:
        supabase_url: Supabase URL
        supabase_api_key: Supabase API Key
    
    Returns:
        List[str]: creator_id 列表
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
    query_url = f"{api_url}?select=creator_id"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(query_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        creators = response.json()
        creator_ids = [creator.get("creator_id") for creator in creators if creator.get("creator_id")]
        
        print(f"✓ 成功获取 {len(creator_ids)} 个 creator_id")
        return creator_ids
    except requests.exceptions.RequestException as e:
        print(f"✗ 获取 creator_id 列表失败: {str(e)}")
        raise


def check_magnet_image_exists(creator_id: str, supabase_url: str, supabase_api_key: str) -> Tuple[bool, Dict[str, Any]]:
    """
    检查 magnet_image 表中是否存在指定 creator_id 的记录，且 front_image_url 不为空
    
    Args:
        creator_id: Creator ID
        supabase_url: Supabase URL
        supabase_api_key: Supabase API Key
    
    Returns:
        tuple[bool, dict]: (是否存在有效记录, 详细信息)
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/magnet_image"
    # 查询所有记录，检查是否有 front_image_url 不为空的记录
    query_url = f"{api_url}?creator_id=eq.{creator_id}&select=front_image_url"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(query_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        results = response.json()
        total_records = len(results)
        valid_urls_count = 0
        empty_urls_count = 0
        
        # 检查是否有 front_image_url 不为空的记录
        for record in results:
            front_image_url = record.get("front_image_url")
            if front_image_url and front_image_url.strip():  # 不为 None 且不为空字符串
                valid_urls_count += 1
            else:
                empty_urls_count += 1
        
        has_valid = valid_urls_count > 0
        detail = {
            "total_records": total_records,
            "valid_urls_count": valid_urls_count,
            "empty_urls_count": empty_urls_count
        }
        
        return has_valid, detail
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_detail = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
        detail = {
            "error": error_detail,
            "total_records": 0,
            "valid_urls_count": 0,
            "empty_urls_count": 0
        }
        return False, detail


def get_creator_current_status(creator_id: str, supabase_url: str, supabase_api_key: str) -> Optional[str]:
    """
    获取 creator 的当前状态
    
    Args:
        creator_id: Creator ID
        supabase_url: Supabase URL
        supabase_api_key: Supabase API Key
    
    Returns:
        Optional[str]: 当前状态，如果查询失败返回 None
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
    query_url = f"{api_url}?creator_id=eq.{creator_id}&select=status"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(query_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        results = response.json()
        if results and len(results) > 0:
            return results[0].get("status")
        return None
    except requests.exceptions.RequestException as e:
        return None


def main():
    """主函数"""
    # 加载环境变量
    load_env_file()
    
    # 获取 Supabase 配置
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_api_key = os.getenv("SUPABASE_API_KEY")
    
    if not supabase_url or not supabase_api_key:
        print("✗ 错误: 缺少 Supabase 配置（SUPABASE_URL 或 SUPABASE_API_KEY 环境变量）")
        print("请确保 .env 文件中包含这些配置")
        sys.exit(1)
    
    print("=" * 60)
    print("开始更新没有 magnet_image 记录（且 front_image_url 不为空）的 creator 状态")
    print("=" * 60)
    
    try:
        # 1. 获取所有 creator_id
        print("\n步骤 1: 获取所有 creator_id...")
        creator_ids = get_all_creator_ids(supabase_url, supabase_api_key)
        
        if not creator_ids:
            print("没有找到任何 creator_id，退出")
            return
        
        # 2. 检查并分批更新（每检查10条更新一次）
        print(f"\n步骤 2: 检查并分批更新 {len(creator_ids)} 个 creator（每10条一批）...")
        print("-" * 60)
        
        BATCH_SIZE = 10  # 每批处理的条数
        
        creators_without_images = []
        creators_with_images = []
        updated_count = 0
        failed_count = 0
        already_pending_count = 0
        failed_details = []
        
        # 待更新的批次列表
        pending_batch = []
        
        def update_batch(batch: List[str], batch_num: int):
            """更新一批 creator 的状态"""
            nonlocal updated_count, failed_count, already_pending_count, failed_details
            
            print(f"\n  >>> 开始更新第 {batch_num} 批（{len(batch)} 条）...")
            
            for i, creator_id in enumerate(batch, 1):
                print(f"\n  [{i}/{len(batch)}] 处理 creator_id: {creator_id}")
                
                # 获取当前状态
                current_status = get_creator_current_status(creator_id, supabase_url, supabase_api_key)
                print(f"    当前状态: {current_status if current_status else '未知'}")
                
                # 如果已经是 pending，跳过
                if current_status == PENDING:
                    print(f"    → 状态已是 {PENDING}，跳过更新")
                    already_pending_count += 1
                    continue
                
                # 执行更新
                print(f"    → 正在更新状态为 {PENDING}...")
                
                # 直接调用 API 更新，以便获取详细错误信息
                api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
                update_url = f"{api_url}?creator_id=eq.{creator_id}"
                
                headers = {
                    "apikey": supabase_api_key,
                    "Authorization": f"Bearer {supabase_api_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=representation"
                }
                
                payload = {
                    "status": PENDING
                }
                
                try:
                    response = requests.patch(update_url, headers=headers, json=payload, timeout=30)
                    response.raise_for_status()
                    
                    # 获取更新后的数据
                    updated_data = response.json()
                    print(f"    → API 请求成功 (HTTP {response.status_code})")
                    
                    # 验证更新是否成功
                    new_status = get_creator_current_status(creator_id, supabase_url, supabase_api_key)
                    if new_status == PENDING:
                        print(f"    ✓ 更新成功！新状态: {new_status}")
                        updated_count += 1
                    else:
                        print(f"    ⚠️  更新请求成功，但验证失败！期望状态: {PENDING}，实际状态: {new_status}")
                        print(f"    → API 返回数据: {updated_data}")
                        failed_count += 1
                        failed_details.append({
                            "creator_id": creator_id,
                            "reason": f"验证失败：期望 {PENDING}，实际 {new_status}",
                            "api_response": updated_data
                        })
                except requests.exceptions.RequestException as e:
                    error_detail = str(e)
                    if hasattr(e, 'response') and e.response is not None:
                        status_code = e.response.status_code
                        try:
                            error_body = e.response.json()
                            error_detail = f"HTTP {status_code}: {error_body}"
                        except:
                            error_detail = f"HTTP {status_code}: {e.response.text[:500]}"
                    
                    print(f"    ✗ 更新失败: {error_detail}")
                    failed_count += 1
                    failed_details.append({
                        "creator_id": creator_id,
                        "reason": error_detail
                    })
            
            print(f"  >>> 第 {batch_num} 批更新完成")
        
        # 遍历所有 creator_id，检查并分批更新
        batch_num = 0
        for i, creator_id in enumerate(creator_ids, 1):
            print(f"  检查 [{i}/{len(creator_ids)}] creator_id: {creator_id}...", end=" ")
            
            has_image, detail = check_magnet_image_exists(creator_id, supabase_url, supabase_api_key)
            
            if has_image:
                print(f"✓ 存在有效记录（{detail['valid_urls_count']}/{detail['total_records']} 条记录有 front_image_url）")
                creators_with_images.append(creator_id)
            else:
                if detail.get("error"):
                    print(f"✗ 查询失败: {detail['error']}")
                elif detail['total_records'] == 0:
                    print("✗ 不存在记录")
                else:
                    print(f"✗ 不存在有效记录（{detail['total_records']} 条记录，但 front_image_url 都为空）")
                creators_without_images.append(creator_id)
                pending_batch.append(creator_id)
                
                # 每收集到 BATCH_SIZE 条，就更新一次
                if len(pending_batch) >= BATCH_SIZE:
                    batch_num += 1
                    update_batch(pending_batch, batch_num)
                    pending_batch = []  # 清空批次
        
        # 处理剩余的不足 BATCH_SIZE 条的数据
        if pending_batch:
            batch_num += 1
            update_batch(pending_batch, batch_num)
        
        # 4. 输出统计信息
        print("\n" + "=" * 60)
        print("更新完成！统计信息：")
        print("=" * 60)
        print(f"总 creator 数量: {len(creator_ids)}")
        print(f"有有效 magnet_image 记录（front_image_url 不为空）: {len(creators_with_images)}")
        print(f"没有有效 magnet_image 记录（无记录或 front_image_url 为空）: {len(creators_without_images)}")
        print(f"  - 已更新状态: {updated_count}")
        print(f"  - 状态已是 pending（跳过）: {already_pending_count}")
        print(f"  - 更新失败: {failed_count}")
        print("=" * 60)
        
        if failed_details:
            print("\n更新失败的 creator_id 详情:")
            for detail in failed_details:
                print(f"  - {detail['creator_id']}: {detail['reason']}")
        
        if updated_count > 0:
            print(f"\n✓ 成功更新了 {updated_count} 个 creator 的状态为 {PENDING}")
        
    except Exception as e:
        print(f"\n✗ 执行失败: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

