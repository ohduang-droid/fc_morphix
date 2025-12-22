#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
对 output.json 文件进行去重
根据 creator_id 去重，保留第一次出现的记录
"""

import json
from pathlib import Path
from typing import Dict, List, Any

def deduplicate_output_json(
    input_file: str = "json/creator/output.json",
    output_file: str = None
) -> None:
    """
    对 output.json 文件进行去重
    
    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径，如果为 None 则覆盖原文件
    """
    input_path = Path(input_file)
    if not input_path.exists():
        print(f"错误: 文件 {input_file} 不存在")
        return
    
    print(f"开始处理文件: {input_file}")
    
    # 读取 JSON 文件
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            creators = json.load(f)
    except json.JSONDecodeError as e:
        print(f"错误: JSON 文件格式错误: {str(e)}")
        return
    except Exception as e:
        print(f"错误: 读取文件失败: {str(e)}")
        return
    
    if not isinstance(creators, list):
        print("错误: JSON 文件格式错误：根元素必须是数组")
        return
    
    print(f"✓ 读取成功，共 {len(creators)} 条记录")
    
    # 使用字典去重，key 为 creator_id，value 为 creator 记录
    # 保留第一次出现的记录
    unique_creators = {}
    duplicate_count = 0
    
    for creator in creators:
        creator_id = creator.get("creator_id")
        
        if not creator_id:
            print(f"警告: 发现缺少 creator_id 的记录，跳过: {json.dumps(creator, ensure_ascii=False)[:100]}")
            continue
        
        creator_id_str = str(creator_id)
        
        if creator_id_str in unique_creators:
            duplicate_count += 1
            print(f"  发现重复: creator_id={creator_id_str}, creator_name={creator.get('creator_name', 'Unknown')}")
        else:
            unique_creators[creator_id_str] = creator
    
    # 转换为列表（保持原始顺序）
    deduplicated_creators = list(unique_creators.values())
    
    print(f"\n去重统计:")
    print(f"  原始记录数: {len(creators)}")
    print(f"  去重后记录数: {len(deduplicated_creators)}")
    print(f"  重复记录数: {duplicate_count}")
    
    # 确定输出路径
    if output_file is None:
        output_path = input_path
    else:
        output_path = Path(output_file)
    
    # 确保输出目录存在
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 保存去重后的数据
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(deduplicated_creators, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ 去重完成，已保存到: {output_path}")
        print(f"  减少了 {duplicate_count} 条重复记录")
    except Exception as e:
        print(f"错误: 保存文件失败: {str(e)}")


if __name__ == "__main__":
    deduplicate_output_json()



