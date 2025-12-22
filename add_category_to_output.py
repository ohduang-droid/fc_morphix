#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为 output.json 添加 category 字段
根据 json/source/xxx.json 的文件名，匹配 category 字典中的 slug，找到对应的 name 作为 category 字段的值
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional

# Category 字典
CATEGORY_DICT = [
    { "id": 96, "name": "Culture", "slug": "culture" },
    { "id": 4, "name": "Technology", "slug": "technology" },
    { "id": 62, "name": "Business", "slug": "business" },
    { "id": 76739, "name": "U.S. Politics", "slug": "us-politics" },
    { "id": 153, "name": "Finance", "slug": "finance" },
    { "id": 13645, "name": "Food & Drink", "slug": "food" },
    { "id": 94, "name": "Sports", "slug": "sports" },
    { "id": 15417, "name": "Art & Illustration", "slug": "art" },
    { "id": 76740, "name": "World Politics", "slug": "world-politics" },
    { "id": 76741, "name": "Health Politics", "slug": "health-politics" },
    { "id": 103, "name": "News", "slug": "news" },
    { "id": 49715, "name": "Fashion & Beauty", "slug": "fashionandbeauty" },
    { "id": 11, "name": "Music", "slug": "music" },
    { "id": 223, "name": "Faith & Spirituality", "slug": "faith" },
    { "id": 15414, "name": "Climate & Environment", "slug": "climate" },
    { "id": 134, "name": "Science", "slug": "science" },
    { "id": 339, "name": "Literature", "slug": "literature" },
    { "id": 284, "name": "Fiction", "slug": "fiction" },
    { "id": 355, "name": "Health & Wellness", "slug": "health" },
    { "id": 61, "name": "Design", "slug": "design" },
    { "id": 109, "name": "Travel", "slug": "travel" },
    { "id": 1796, "name": "Parenting", "slug": "parenting" },
    { "id": 114, "name": "Philosophy", "slug": "philosophy" },
    { "id": 387, "name": "Comics", "slug": "comics" },
    { "id": 51282, "name": "International", "slug": "international" },
    { "id": 118, "name": "Crypto", "slug": "crypto" },
    { "id": 18, "name": "History", "slug": "history" },
    { "id": 49692, "name": "Humor", "slug": "humor" },
    { "id": 34, "name": "Education", "slug": "education" },
    { "id": 76782, "name": "Film & TV", "slug": "film-and-tv" }
]

# 创建 slug 到 name 的映射
SLUG_TO_NAME = {cat["slug"]: cat["name"] for cat in CATEGORY_DICT}


def get_creator_id_from_item(item: Dict[str, Any]) -> Optional[str]:
    """从 source 文件中的 item 提取 creator_id"""
    publication = item.get("publication", {})
    user = item.get("user", {})
    
    # 优先使用 user.id，否则使用 publication.id
    if user and user.get("id"):
        return str(user["id"])
    elif publication.get("id"):
        return str(publication["id"])
    return None


def build_creator_to_category_mapping(source_dir: str = "json/source") -> Dict[str, str]:
    """
    构建 creator_id 到 category 的映射
    
    Args:
        source_dir: source JSON 文件目录
        
    Returns:
        creator_id 到 category name 的字典
    """
    source_path = Path(source_dir)
    if not source_path.exists():
        print(f"错误: 目录 {source_dir} 不存在")
        return {}
    
    # 查找所有 JSON 文件
    json_files = list(source_path.glob("*.json"))
    
    if not json_files:
        print(f"在 {source_dir} 目录下未找到 JSON 文件")
        return {}
    
    creator_to_category = {}
    
    for json_file in json_files:
        try:
            # 获取文件名（不含扩展名）作为 slug
            slug = json_file.stem
            
            # 根据 slug 查找对应的 category name
            category_name = SLUG_TO_NAME.get(slug)
            
            if not category_name:
                print(f"警告: 文件 {json_file.name} 的 slug '{slug}' 在 category 字典中未找到")
                continue
            
            print(f"处理文件: {json_file.name} (slug: {slug}, category: {category_name})...")
            
            # 读取 JSON 文件
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # 确保 data 是列表
            if not isinstance(data, list):
                data = [data]
            
            # 遍历所有 item，提取 creator_id
            for item in data:
                creator_id = get_creator_id_from_item(item)
                if creator_id:
                    creator_to_category[creator_id] = category_name
            
            print(f"  ✓ 已处理 {len(data)} 条记录\n")
        except Exception as e:
            print(f"  ✗ 处理失败: {json_file}")
            print(f"    错误: {str(e)}\n")
    
    return creator_to_category


def add_category_to_output(
    output_file: str = "json/creator/output.json",
    source_dir: str = "json/source"
) -> None:
    """
    为 output.json 添加 category 字段
    
    Args:
        output_file: 输出 JSON 文件路径
        source_dir: source JSON 文件目录
    """
    output_path = Path(output_file)
    if not output_path.exists():
        print(f"错误: 文件 {output_file} 不存在")
        return
    
    print(f"开始为 {output_file} 添加 category 字段...\n")
    
    # 构建 creator_id 到 category 的映射
    print("步骤 1: 构建 creator_id 到 category 的映射...")
    creator_to_category = build_creator_to_category_mapping(source_dir)
    print(f"✓ 映射构建完成，共 {len(creator_to_category)} 个 creator_id\n")
    
    # 读取 output.json
    print("步骤 2: 读取 output.json...")
    with open(output_path, "r", encoding="utf-8") as f:
        output_data = json.load(f)
    
    if not isinstance(output_data, list):
        print("错误: output.json 格式不正确，应该是数组")
        return
    
    print(f"✓ 读取完成，共 {len(output_data)} 条记录\n")
    
    # 为每条记录添加 category 字段
    print("步骤 3: 为每条记录添加 category 字段...")
    updated_count = 0
    not_found_count = 0
    
    for item in output_data:
        creator_id = item.get("creator_id")
        if not creator_id:
            continue
        
        creator_id_str = str(creator_id)
        category = creator_to_category.get(creator_id_str)
        
        if category:
            item["category"] = category
            updated_count += 1
        else:
            # 如果找不到对应的 category，设置为 null
            item["category"] = None
            not_found_count += 1
    
    print(f"✓ 更新完成: {updated_count} 条记录找到 category，{not_found_count} 条记录未找到\n")
    
    # 保存更新后的数据
    print("步骤 4: 保存更新后的数据...")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"✓ 保存完成: {output_file}\n")
    print(f"{'='*60}")
    print(f"✓ 所有操作完成！")
    print(f"{'='*60}")
    print(f"📊 统计信息:")
    print(f"  总记录数: {len(output_data)}")
    print(f"  成功添加 category: {updated_count}")
    print(f"  未找到 category: {not_found_count}")
    print(f"{'='*60}")


if __name__ == "__main__":
    add_category_to_output()



