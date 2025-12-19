#!/usr/bin/env python3
"""
从 Creator 表中导出 creator 信息，用于发送消息
导出格式：
[
  {
    "creator_name": "...",
    "creator_url": "...",  # 对应表中的 website_url
    "message": "...",      # 对应表中的 message 字段
    "image_url": "..."     # 从 magnet_image 表获取，type=cover 的 front_image_url
  }
]
按 paid_subscribers_est 倒序排序，支持设置导出数量
"""
import os
import sys
import json
import argparse
import requests
from typing import List, Dict, Any, Optional


def load_env_file(path: str = ".env"):
    """加载环境变量文件"""
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


def get_creators_from_supabase(
    supabase_url: str,
    supabase_api_key: str,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    从 Supabase 获取 creator 数据
    按 paid_subscribers_est 倒序排序
    
    Args:
        supabase_url: Supabase URL
        supabase_api_key: Supabase API Key
        limit: 可选，限制返回数量
    
    Returns:
        Creator 列表
    """
    # 构建 API URL
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/creator"
    
    # 设置请求头
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json"
    }
    
    # 构建查询参数：选择需要的字段，按 paid_subscribers_est 降序排序
    params = {
        "select": "creator_id,creator_name,website_url,message,paid_subscribers_est",
        "order": "paid_subscribers_est.desc.nullslast"  # 降序，null 值排在最后
    }
    
    # 如果设置了 limit，添加到查询参数
    if limit:
        params["limit"] = str(limit)
    
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        creators = response.json()
        
        # 如果数据库不支持 nullslast，我们需要手动排序
        # 按 paid_subscribers_est 降序排序（null 值视为 0）
        def get_sort_key(creator: Dict[str, Any]) -> float:
            paid_subscribers = creator.get("paid_subscribers_est")
            try:
                return float(paid_subscribers) if paid_subscribers is not None else 0.0
            except (ValueError, TypeError):
                return 0.0
        
        creators.sort(key=get_sort_key, reverse=True)
        
        # 如果设置了 limit，截取前 limit 个
        if limit and len(creators) > limit:
            creators = creators[:limit]
        
        return creators
        
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"从 Supabase 获取 creator 信息失败: {str(e)}") from e


def get_magnet_images_by_creator_ids(
    creator_ids: List[str],
    supabase_url: str,
    supabase_api_key: str
) -> Dict[str, str]:
    """
    批量获取 magnet_image 表中 type=cover 的 front_image_url
    按 creator_id 分组
    
    Args:
        creator_ids: Creator ID 列表
        supabase_url: Supabase URL
        supabase_api_key: Supabase API Key
    
    Returns:
        字典，key 为 creator_id，value 为 front_image_url（如果存在）
    """
    if not creator_ids:
        return {}
    
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/magnet_image"
    
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json"
    }
    
    # 由于 Supabase 的 in 操作符限制，我们需要分批查询
    # 每批最多 100 个 creator_id
    batch_size = 100
    result_dict = {}
    
    for i in range(0, len(creator_ids), batch_size):
        batch_ids = creator_ids[i:i + batch_size]
        
        # 构建查询 URL，使用 in 操作符
        query_url = f"{api_url}?creator_id=in.({','.join(batch_ids)})&type=eq.cover&select=creator_id,front_image_url"
        
        try:
            response = requests.get(query_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            images = response.json()
            
            # 将结果转换为字典
            for image in images:
                creator_id = str(image.get("creator_id", ""))
                front_image_url = image.get("front_image_url", "")
                if creator_id and front_image_url:
                    result_dict[creator_id] = front_image_url
                    
        except requests.exceptions.RequestException as e:
            # 如果批量查询失败，尝试逐个查询
            print(f"  警告: 批量查询 magnet_image 失败，尝试逐个查询: {str(e)}")
            for creator_id in batch_ids:
                try:
                    query_url = f"{api_url}?creator_id=eq.{creator_id}&type=eq.cover&select=creator_id,front_image_url&limit=1"
                    response = requests.get(query_url, headers=headers, timeout=10)
                    response.raise_for_status()
                    images = response.json()
                    if images and len(images) > 0:
                        front_image_url = images[0].get("front_image_url", "")
                        if front_image_url:
                            result_dict[str(creator_id)] = front_image_url
                except:
                    # 忽略单个查询失败
                    continue
    
    return result_dict


def export_creators(
    output_file: Optional[str] = None,
    limit: Optional[int] = None,
    supabase_url: Optional[str] = None,
    supabase_api_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    导出 creator 信息
    
    Args:
        output_file: 可选，输出文件路径（如果不指定则输出到 stdout）
        limit: 可选，限制导出数量
        supabase_url: 可选，Supabase URL（如果不指定则从环境变量获取）
        supabase_api_key: 可选，Supabase API Key（如果不指定则从环境变量获取）
    
    Returns:
        导出的 creator 列表
    """
    # 加载环境变量
    load_env_file()
    
    # 获取配置
    supabase_url = supabase_url or os.getenv("SUPABASE_URL")
    supabase_api_key = supabase_api_key or os.getenv("SUPABASE_API_KEY")
    
    if not supabase_url:
        raise ValueError("缺少 Supabase URL 配置（supabase_url 参数或 SUPABASE_URL 环境变量）")
    
    if not supabase_api_key:
        raise ValueError("缺少 Supabase API Key 配置（supabase_api_key 参数或 SUPABASE_API_KEY 环境变量）")
    
    # 从 Supabase 获取数据
    print(f"正在从 Supabase 获取 creator 数据...")
    if limit:
        print(f"限制数量: {limit}")
    
    creators = get_creators_from_supabase(
        supabase_url=supabase_url,
        supabase_api_key=supabase_api_key,
        limit=limit
    )
    
    print(f"成功获取 {len(creators)} 个 creator")
    
    # 获取所有 creator_id，用于查询 magnet_image 表
    creator_ids = []
    for creator in creators:
        creator_id = creator.get("creator_id")
        if creator_id:
            creator_ids.append(str(creator_id))
    
    # 批量获取 magnet_image 数据（type=cover）
    print(f"正在从 magnet_image 表获取封面图片（type=cover）...")
    image_url_map = get_magnet_images_by_creator_ids(
        creator_ids=creator_ids,
        supabase_url=supabase_url,
        supabase_api_key=supabase_api_key
    )
    print(f"成功获取 {len(image_url_map)} 个封面图片")
    
    # 转换为导出格式
    export_data = []
    for creator in creators:
        creator_name = creator.get("creator_name", "")
        website_url = creator.get("website_url", "")
        message = creator.get("message", "")
        creator_id = str(creator.get("creator_id", ""))
        
        # 只导出有 creator_name 和 website_url 的记录
        if creator_name and website_url:
            # 获取对应的 image_url
            image_url = image_url_map.get(creator_id, "")
            
            export_data.append({
                "creator_name": creator_name,
                "creator_url": website_url,
                "message": message if message else "",
                "image_url": image_url if image_url else ""
            })
    
    print(f"导出 {len(export_data)} 条有效记录（已过滤掉 creator_name 或 website_url 为空的记录）")
    
    # 输出结果
    if output_file:
        # 写入文件
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        print(f"结果已保存到: {output_file}")
    else:
        # 输出到 stdout
        print("\n导出结果:")
        print(json.dumps(export_data, ensure_ascii=False, indent=2))
    
    return export_data


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="从 Creator 表中导出 creator 信息，用于发送消息",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 导出前 10 条记录到文件
  python send_message.py --limit 10 --output output.json
  
  # 导出所有记录到 stdout
  python send_message.py
  
  # 导出前 5 条记录到 stdout
  python send_message.py --limit 5
        """
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="限制导出数量（可选）"
    )
    
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="输出文件路径（可选，如果不指定则输出到 stdout）"
    )
    
    parser.add_argument(
        "--supabase-url",
        type=str,
        default=None,
        help="Supabase URL（可选，如果不指定则从环境变量 SUPABASE_URL 获取）"
    )
    
    parser.add_argument(
        "--supabase-api-key",
        type=str,
        default=None,
        help="Supabase API Key（可选，如果不指定则从环境变量 SUPABASE_API_KEY 获取）"
    )
    
    args = parser.parse_args()
    
    try:
        export_creators(
            output_file=args.output,
            limit=args.limit,
            supabase_url=args.supabase_url,
            supabase_api_key=args.supabase_api_key
        )
    except Exception as e:
        print(f"错误: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

