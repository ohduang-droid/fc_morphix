"""
删除状态为"失败"的 Creator 的缓存文件，并更新状态为"待生成"

脚本功能：
1. 查询 creator 表中状态为 "failed" 的 Creator
2. 删除这些 Creator 对应的缓存文件：
   - creator_xxx_step_three.json
   - creator_xxx_step_four.json
   其中 xxx 是 creator_id
3. 删除成功后，更新 creator 状态为 "pending"（待生成）
"""
import os
import sys
import requests
from typing import List, Dict, Any, Optional
from utils.cache import get_cache_dir, get_cache_file_path
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
    
    with open(env_file, "r", encoding="utf-8") as f:
        for raw_line in f:
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


def get_failed_creator_ids(supabase_url: str, supabase_api_key: str) -> List[str]:
    """
    从 creator 表获取所有状态为 "failed" 的 creator_id
    
    Args:
        supabase_url: Supabase URL
        supabase_api_key: Supabase API Key
    
    Returns:
        List[str]: creator_id 列表
    """
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
    query_url = f"{api_url}?status=eq.failed&select=creator_id"
    
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
        
        print(f"✓ 成功获取 {len(creator_ids)} 个状态为 'failed' 的 creator_id")
        return creator_ids
    except requests.exceptions.RequestException as e:
        error_detail = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_detail = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
        print(f"✗ 获取 failed creator_id 列表失败: {error_detail}")
        raise


def delete_cache_files(creator_id: str) -> Dict[str, bool]:
    """
    删除指定 creator_id 的 step_three 和 step_four 缓存文件
    
    Args:
        creator_id: Creator ID
    
    Returns:
        Dict[str, bool]: 删除结果，key 为文件名，value 为是否成功删除
    """
    results = {}
    
    # 要删除的步骤
    steps_to_delete = ["step_three", "step_four"]
    
    for step in steps_to_delete:
        cache_file = get_cache_file_path(creator_id, step)
        file_exists = os.path.exists(cache_file)
        
        if file_exists:
            try:
                os.remove(cache_file)
                results[cache_file] = True
            except Exception as e:
                results[cache_file] = False
                print(f"    ⚠️  删除失败: {cache_file} - {str(e)}")
        else:
            results[cache_file] = None  # 文件不存在
    
    return results


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
    print("删除状态为 'failed' 的 Creator 的缓存文件")
    print("=" * 60)
    
    try:
        # 1. 获取所有状态为 "failed" 的 creator_id
        print("\n步骤 1: 查询状态为 'failed' 的 Creator...")
        failed_creator_ids = get_failed_creator_ids(supabase_url, supabase_api_key)
        
        if not failed_creator_ids:
            print("没有找到状态为 'failed' 的 Creator，退出")
            return
        
        # 2. 删除这些 Creator 的缓存文件，并更新状态
        print(f"\n步骤 2: 删除 {len(failed_creator_ids)} 个 Creator 的缓存文件并更新状态...")
        print("-" * 60)
        
        deleted_count = 0
        not_found_count = 0
        failed_count = 0
        deleted_files = []
        not_found_files = []
        failed_files = []
        
        # 状态更新统计
        status_updated_count = 0
        status_update_failed_count = 0
        status_updated_creators = []
        status_update_failed_creators = []
        
        for i, creator_id in enumerate(failed_creator_ids, 1):
            print(f"\n[{i}/{len(failed_creator_ids)}] 处理 creator_id: {creator_id}")
            
            results = delete_cache_files(creator_id)
            
            has_success = False  # 是否有成功删除的文件
            has_failure = False  # 是否有删除失败的文件
            
            for cache_file, result in results.items():
                filename = os.path.basename(cache_file)
                if result is True:
                    print(f"  ✓ 已删除: {filename}")
                    deleted_count += 1
                    deleted_files.append(cache_file)
                    has_success = True
                elif result is False:
                    print(f"  ✗ 删除失败: {filename}")
                    failed_count += 1
                    failed_files.append(cache_file)
                    has_failure = True
                else:  # result is None, 文件不存在
                    print(f"  - 文件不存在: {filename}")
                    not_found_count += 1
                    not_found_files.append(cache_file)
                    # 文件不存在也算作清理成功（可能之前已经删除过）
                    has_success = True
            
            # 如果至少有一个文件被成功删除或不存在（且没有删除失败），则更新状态
            if has_success and not has_failure:
                print(f"  → 正在更新状态为 {PENDING}...")
                success = update_creator_status(
                    creator_id=creator_id,
                    status=PENDING,
                    supabase_url=supabase_url,
                    supabase_api_key=supabase_api_key
                )
                
                if success:
                    print(f"  ✓ 状态更新成功: failed -> {PENDING}")
                    status_updated_count += 1
                    status_updated_creators.append(creator_id)
                else:
                    print(f"  ✗ 状态更新失败")
                    status_update_failed_count += 1
                    status_update_failed_creators.append(creator_id)
            elif has_failure:
                print(f"  ⚠️  由于删除失败，跳过状态更新")
        
        # 3. 输出统计信息
        print("\n" + "=" * 60)
        print("处理完成！统计信息：")
        print("=" * 60)
        print(f"状态为 'failed' 的 Creator 数量: {len(failed_creator_ids)}")
        print(f"\n缓存文件删除统计：")
        print(f"  - 成功删除的缓存文件: {deleted_count}")
        print(f"  - 文件不存在（跳过）: {not_found_count}")
        print(f"  - 删除失败: {failed_count}")
        print(f"\n状态更新统计：")
        print(f"  - 成功更新状态为 '{PENDING}': {status_updated_count} 条")
        print(f"  - 状态更新失败: {status_update_failed_count} 条")
        print("=" * 60)
        
        if deleted_files:
            print(f"\n✓ 成功删除的缓存文件 ({len(deleted_files)} 个):")
            for cache_file in deleted_files:
                print(f"  - {os.path.basename(cache_file)}")
        
        if failed_files:
            print(f"\n✗ 删除失败的缓存文件 ({len(failed_files)} 个):")
            for cache_file in failed_files:
                print(f"  - {os.path.basename(cache_file)}")
        
        if not_found_files and len(not_found_files) < 20:  # 如果文件太多，不全部显示
            print(f"\n- 不存在的缓存文件 ({len(not_found_files)} 个):")
            for cache_file in not_found_files[:20]:  # 只显示前20个
                print(f"  - {os.path.basename(cache_file)}")
            if len(not_found_files) > 20:
                print(f"  ... 还有 {len(not_found_files) - 20} 个文件不存在")
        
        if status_updated_creators:
            print(f"\n✓ 成功更新状态的 Creator ({len(status_updated_creators)} 条):")
            if len(status_updated_creators) <= 50:
                for creator_id in status_updated_creators:
                    print(f"  - {creator_id}")
            else:
                for creator_id in status_updated_creators[:50]:
                    print(f"  - {creator_id}")
                print(f"  ... 还有 {len(status_updated_creators) - 50} 个 Creator")
        
        if status_update_failed_creators:
            print(f"\n✗ 状态更新失败的 Creator ({len(status_update_failed_creators)} 条):")
            for creator_id in status_update_failed_creators:
                print(f"  - {creator_id}")
        
    except Exception as e:
        print(f"\n✗ 执行失败: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

