#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSON 文件转换脚本
将 json/source 目录下的 JSON 文件转换为指定的结构格式
输出到 json/creator/output.json
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

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


def get_website_url(publication: Dict[str, Any]) -> str:
    """获取网站 URL"""
    if publication.get("custom_domain"):
        domain = publication["custom_domain"]
        if not domain.startswith("http"):
            return f"https://{domain}"
        return domain
    elif publication.get("subdomain"):
        return f"https://{publication['subdomain']}.substack.com"
    return ""


def get_platform_url(publication: Dict[str, Any]) -> str:
    """获取平台 URL"""
    if publication.get("subdomain"):
        return f"https://{publication['subdomain']}.substack.com"
    return ""


def parse_order_of_magnitude(value: Any) -> int:
    """
    解析数量级值，支持数字和字符串格式（如 "1.1M+"）
    
    Args:
        value: 数量级值，可能是数字或字符串
        
    Returns:
        解析后的数字，如果无法解析则返回 0
    """
    if value is None:
        return 0
    
    # 如果是数字，直接返回
    if isinstance(value, (int, float)):
        return int(value)
    
    # 如果是字符串，尝试解析
    if isinstance(value, str):
        value = value.strip().upper()
        # 移除 + 号
        value = value.rstrip("+")
        
        # 处理 M (百万)
        if "M" in value:
            num_str = value.replace("M", "").strip()
            try:
                num = float(num_str)
                return int(num * 1000000)
            except ValueError:
                pass
        
        # 处理 K (千)
        if "K" in value:
            num_str = value.replace("K", "").strip()
            try:
                num = float(num_str)
                return int(num * 1000)
            except ValueError:
                pass
        
        # 尝试直接解析为数字
        try:
            return int(float(value))
        except ValueError:
            pass
    
    return 0


def is_qualified_creator(item: Dict[str, Any]) -> Tuple[bool, Dict[str, bool]]:
    """
    判断 creator 是否符合筛选条件
    
    Args:
        item: JSON 项，包含 publication 和 user 字段
        
    Returns:
        (是否符合条件, 各条件检查结果字典)
    """
    publication = item.get("publication", {})
    user = item.get("user", {})
    
    results = {}
    
    # 条件 ①：英文写作（EN）
    is_english = publication.get("language") == "en"
    results["is_english"] = is_english
    
    # 条件 ②：US Based
    is_us = (
        publication.get("stripe_country") == "US" or
        publication.get("stripe_platform_account") == "US"
    )
    results["is_us"] = is_us
    
    # 条件 ③：付费订阅 > 1,000 OR 免费订阅 > 50,000
    # 3.1 免费订阅 > 50,000
    free_subscribers_ok = False
    free_magnitude = publication.get("freeSubscriberCountOrderOfMagnitude")
    free_ranking = publication.get("rankingDetailFreeIncludedOrderOfMagnitude")
    
    if free_magnitude is not None:
        free_value = parse_order_of_magnitude(free_magnitude)
        free_subscribers_ok = free_value >= 50000
    
    if not free_subscribers_ok and free_ranking is not None:
        free_value = parse_order_of_magnitude(free_ranking)
        free_subscribers_ok = free_value >= 50000
    
    # 3.2 付费订阅 > 1,000
    paid_subscribers_ok = False
    paid_ranking = publication.get("rankingDetailOrderOfMagnitude")
    author_bestseller_tier = publication.get("author_bestseller_tier")
    user_bestseller_tier = user.get("bestseller_tier") if user else None
    
    if paid_ranking is not None:
        paid_value = parse_order_of_magnitude(paid_ranking)
        paid_subscribers_ok = paid_value >= 1000
    
    if not paid_subscribers_ok and author_bestseller_tier is not None:
        bestseller_value = parse_order_of_magnitude(author_bestseller_tier)
        paid_subscribers_ok = bestseller_value >= 1000
    
    if not paid_subscribers_ok and user_bestseller_tier is not None:
        bestseller_value = parse_order_of_magnitude(user_bestseller_tier)
        paid_subscribers_ok = bestseller_value >= 1000
    
    subscription_ok = free_subscribers_ok or paid_subscribers_ok
    results["subscription_ok"] = subscription_ok
    results["free_subscribers_ok"] = free_subscribers_ok
    results["paid_subscribers_ok"] = paid_subscribers_ok
    
    # 条件 ④：必须能联系到 Creator
    has_contact = False
    
    # 4.1 直接 Email
    if publication.get("support_email"):
        has_contact = True
    
    # 4.2 Support / About 页面
    if not has_contact:
        nav_items = publication.get("navigationBarItems", [])
        for nav_item in nav_items:
            # 检查 standard_key
            if nav_item.get("standard_key") == "about":
                has_contact = True
                break
            # 检查 post slug
            post = nav_item.get("post")
            if post and post.get("slug"):
                slug = post.get("slug", "").lower()
                if "support" in slug:
                    has_contact = True
                    break
    
    # 4.3 作者 DM（Substack 内）
    if not has_contact:
        if user and user.get("handle"):
            has_contact = True
    
    results["has_contact"] = has_contact
    
    # 最终判定
    qualified = is_english and is_us and subscription_ok and has_contact
    
    return qualified, results


def get_category_from_slug(slug: str) -> Optional[str]:
    """
    根据 slug 获取 category name
    
    Args:
        slug: category 的 slug 值
        
    Returns:
        category name，如果未找到则返回 None
    """
    return SLUG_TO_NAME.get(slug)


def get_leaderboard_ranking_info(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    获取 leaderboard_ranking 信息
    
    Args:
        item: JSON 项，包含 publication 和 user 字段
        
    Returns:
        leaderboard_ranking 字典，如果不存在则返回 None
    """
    publication = item.get("publication", {})
    return publication.get("leaderboard_ranking")


def deduplicate_items(items: List[Tuple[Dict[str, Any], str]]) -> List[Tuple[Dict[str, Any], str]]:
    """
    对记录列表进行去重（保留有 leaderboard_ranking 的记录，如果都有则保留 rank 更好的）
    
    Args:
        items: (item, source_slug) 元组列表
        
    Returns:
        去重后的 (item, source_slug) 元组列表
    """
    unique_items = {}  # key: creator_id, value: (item, source_slug, sort_key, has_ranking)
    
    for item, source_slug in items:
        # 先转换以获取 creator_id
        converted_item = convert_item(item, source_slug=source_slug)
        creator_id = converted_item.get("creator_id")
        
        if not creator_id:
            continue
        
        creator_id_str = str(creator_id)
        sort_key = get_sort_key_for_leaderboard(item)
        has_ranking = get_leaderboard_ranking_info(item) is not None
        
        # 如果 creator_id 已存在，比较并保留更好的记录
        if creator_id_str in unique_items:
            existing_item, existing_slug, existing_sort_key, existing_has_ranking = unique_items[creator_id_str]
            
            # 优先保留有 leaderboard_ranking 的记录
            if has_ranking and not existing_has_ranking:
                # 当前记录有 ranking，已存在的没有，替换
                unique_items[creator_id_str] = (item, source_slug, sort_key, has_ranking)
            elif not has_ranking and existing_has_ranking:
                # 已存在的记录有 ranking，当前没有，跳过当前记录
                pass
            elif has_ranking and existing_has_ranking:
                # 两个都有 ranking，比较 sort_key，保留 rank 更好的
                # sort_key 越小越好（(ranking_priority, rank)）
                if sort_key < existing_sort_key:
                    # 当前记录的 rank 更好，替换
                    unique_items[creator_id_str] = (item, source_slug, sort_key, has_ranking)
                # 否则保留已存在的记录
            else:
                # 两个都没有 ranking，保留第一个
                pass
        else:
            # 新记录，直接添加
            unique_items[creator_id_str] = (item, source_slug, sort_key, has_ranking)
    
    # 返回去重后的列表
    return [(item, source_slug) for item, source_slug, _, _ in unique_items.values()]


def get_sort_key_for_leaderboard(item: Dict[str, Any]) -> Tuple[int, int]:
    """
    获取用于排序的键值
    排序规则：
    1. 优先按 ranking 类型排序：rising > trending > paid > 其他
    2. 然后按 rank 排序（rank 越小越好）
    3. 没有 leaderboard_ranking 的排在最后
    
    Args:
        item: JSON 项
        
    Returns:
        (ranking_priority, rank) 元组，用于排序
    """
    ranking_info = get_leaderboard_ranking_info(item)
    
    if not ranking_info:
        return (999, 999999)  # 没有 ranking 的排在最后
    
    ranking = ranking_info.get("ranking", "").lower()
    rank = ranking_info.get("rank")
    
    # 确保 rank 是数字
    if rank is None:
        rank = 999999
    
    # 定义 ranking 优先级：rising > trending > paid > 其他
    ranking_priority_map = {
        "rising": 1,
        "trending": 2,
        "paid": 3,
    }
    
    ranking_priority = ranking_priority_map.get(ranking, 4)
    
    return (ranking_priority, rank)


def convert_item(item: Dict[str, Any], source_slug: Optional[str] = None) -> Dict[str, Any]:
    """
    将单个 JSON 项转换为目标格式
    
    Args:
        item: 原始 JSON 项，包含 publication 和 user 字段
        
    Returns:
        转换后的字典
    """
    publication = item.get("publication", {})
    user = item.get("user", {})
    
    # 获取 creator_id（优先使用 user.id，否则使用 publication.id）
    creator_id = ""
    if user and user.get("id"):
        creator_id = str(user["id"])
    elif publication.get("id"):
        creator_id = str(publication["id"])
    
    # 获取 creator_name
    creator_name = ""
    if user and user.get("name"):
        creator_name = user["name"]
    elif publication.get("author_name"):
        creator_name = publication["author_name"]
    
    # 获取 newsletter_name
    newsletter_name = publication.get("name", "")
    
    # 获取 contact_email（从 publication 中查找，如果没有则留空）
    contact_email = publication.get("support_email", "") or publication.get("email_from", "") or ""
    
    # 获取 handle（用于生成 website_url）
    handle = ""
    if user and user.get("handle"):
        handle = user["handle"]
    
    # 获取 website_url（基于 handle 字段）
    if handle:
        website_url = f"https://substack.com/@{handle}"
    else:
        # 如果没有 handle，使用原来的逻辑
        website_url = get_website_url(publication)
    
    # 获取 creator_signature_image_url（对应 logo_url）
    creator_signature_image_url = publication.get("logo_url", "") or ""
    
    # 判断平台（根据数据结构判断，这里假设是 Substack）
    platform = "Substack"
    
    # 获取 platform_url
    platform_url = get_platform_url(publication)
    
    # 获取免费订阅数估算
    free_subscribers_est = ""
    free_magnitude = publication.get("freeSubscriberCountOrderOfMagnitude")
    free_ranking = publication.get("rankingDetailFreeIncludedOrderOfMagnitude")
    
    if free_magnitude is not None:
        # 如果是字符串格式，直接使用；如果是数字，转换为字符串
        if isinstance(free_magnitude, str):
            free_subscribers_est = free_magnitude
        else:
            free_value = parse_order_of_magnitude(free_magnitude)
            free_subscribers_est = str(free_value) if free_value > 0 else ""
    
    if not free_subscribers_est and free_ranking is not None:
        if isinstance(free_ranking, str):
            free_subscribers_est = free_ranking
        else:
            free_value = parse_order_of_magnitude(free_ranking)
            free_subscribers_est = str(free_value) if free_value > 0 else ""
    
    # 获取付费订阅数估算（转换为整数）
    paid_subscribers_est = None
    paid_ranking = publication.get("rankingDetailOrderOfMagnitude")
    
    if paid_ranking is not None:
        if isinstance(paid_ranking, str):
            # 如果是字符串，尝试解析
            paid_value = parse_order_of_magnitude(paid_ranking)
            paid_subscribers_est = paid_value if paid_value > 0 else None
        else:
            paid_value = parse_order_of_magnitude(paid_ranking)
            paid_subscribers_est = paid_value if paid_value > 0 else None
    
    # 获取免费订阅数估算（转换为整数）
    free_subscribers_est_int = None
    if free_subscribers_est:
        free_value = parse_order_of_magnitude(free_subscribers_est)
        free_subscribers_est_int = free_value if free_value > 0 else None
    
    # 获取付费订阅 URL
    paid_subscribe_url = ""
    if platform_url:
        paid_subscribe_url = f"{platform_url}/subscribe"
    
    # 获取付费价格（USD）
    paid_price_monthly_usd = None
    paid_price_yearly_usd = None
    plans = publication.get("plans", [])
    for plan in plans:
        if not plan.get("active", False):
            continue
        interval = plan.get("interval", "")
        currency = plan.get("currency", "")
        if currency.lower() == "usd":
            amount = plan.get("amount", 0)
            # Stripe 金额以分为单位，需要除以 100
            price_usd = amount / 100.0 if amount > 0 else None
            if interval == "month" and paid_price_monthly_usd is None:
                paid_price_monthly_usd = price_usd
            elif interval == "year" and paid_price_yearly_usd is None:
                paid_price_yearly_usd = price_usd
    
    # 获取 creator DM URL（使用已获取的 handle）
    creator_dm_url = ""
    if handle:
        creator_dm_url = f"https://substack.com/@{handle}"
    
    # 根据 source_slug 获取 category
    category = None
    if source_slug:
        category = get_category_from_slug(source_slug)
    
    # 按数据库字段顺序返回
    return {
        "creator_id": creator_id,
        "creator_name": creator_name,
        "newsletter_name": newsletter_name,
        "platform": platform,
        "website_url": website_url,
        "paid_subscribe_url": paid_subscribe_url,
        "contact_email": contact_email,
        "content_category": category,  # 与 category 字段使用相同的值
        "paid_subscriber_image": None,  # 暂时为 None
        "paid_offer_summary": None,  # 暂时为 None
        "paid_price_monthly_usd": paid_price_monthly_usd,
        "paid_price_yearly_usd": paid_price_yearly_usd,
        "paid_subscribers_est": paid_subscribers_est,
        "free_subscribers_est": free_subscribers_est_int,
        "creator_tokens_direct": None,  # ARRAY 类型，暂时为 None
        "creator_tokens_implied": None,  # ARRAY 类型，暂时为 None
        "outreach_email_subject": None,  # 暂时为 None
        "outreach_email_body": None,  # 暂时为 None
        "creator_signature_image_url": creator_signature_image_url,
        "creator_dm_url": creator_dm_url,
        "category": category,  # 根据 source 文件名添加的 category 字段
        "handle": handle,  # 从 source/xxx.json 文件中的 user.handle 获取
    }


def convert_json_file(input_path: str, output_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    转换 JSON 文件
    
    Args:
        input_path: 输入 JSON 文件路径
        output_path: 输出 JSON 文件路径，如果为 None 则覆盖原文件
        
    Returns:
        转换后的数据列表
    """
    # 读取原始 JSON 文件
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # 确保 data 是列表
    if not isinstance(data, list):
        data = [data]
    
    # 从输入文件路径获取 slug（文件名不含扩展名）
    input_file = Path(input_path)
    source_slug = input_file.stem
    
    # 转换每个项
    converted_data = []
    for item in data:
        converted_item = convert_item(item, source_slug=source_slug)
        converted_data.append(converted_item)
    
    # 确定输出路径
    if output_path is None:
        # 在原文件名后添加 _converted
        input_file = Path(input_path)
        output_path = input_file.parent / f"{input_file.stem}_converted{input_file.suffix}"
    
    # 保存转换后的数据
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(converted_data, f, ensure_ascii=False, indent=2)
    
    print(f"✓ 转换完成: {input_path} -> {output_path}")
    print(f"  共转换 {len(converted_data)} 条记录")
    
    return converted_data


def convert_all_json_files(source_dir: str = "json/source", output_file: str = "json/creator/output.json") -> None:
    """
    转换 json/source 目录下的所有 JSON 文件，合并输出到 json/creator/output.json
    
    Args:
        source_dir: 源 JSON 文件目录
        output_file: 输出 JSON 文件路径
    """
    source_path = Path(source_dir)
    if not source_path.exists():
        print(f"错误: 目录 {source_dir} 不存在")
        return
    
    # 查找所有 JSON 文件
    json_files = list(source_path.glob("*.json"))
    
    if not json_files:
        print(f"在 {source_dir} 目录下未找到 JSON 文件")
        return
    
    print(f"找到 {len(json_files)} 个 JSON 文件，开始转换和筛选...\n")
    
    # 第一步：收集所有符合条件的记录（带来源信息）
    all_qualified_items = []  # 存储 (item, source_slug) 元组
    
    # 统计信息
    total_items = 0
    qualified_items = 0
    filter_stats = {
        "not_english": 0,
        "not_us": 0,
        "subscription_not_ok": 0,
        "no_contact": 0,
    }
    
    for json_file in json_files:
        try:
            print(f"处理文件: {json_file.name}...")
            # 读取原始 JSON 文件
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # 确保 data 是列表
            if not isinstance(data, list):
                data = [data]
            
            file_qualified = 0
            
            # 从文件名获取 slug（不含扩展名）
            source_slug = json_file.stem
            
            # 第一步：筛选出符合条件的记录
            qualified_items_list = []
            for item in data:
                total_items += 1
                
                # 检查是否符合条件
                qualified, check_results = is_qualified_creator(item)
                
                if qualified:
                    qualified_items_list.append((item, source_slug))
                    qualified_items += 1
                else:
                    # 统计被筛选掉的原因
                    if not check_results.get("is_english"):
                        filter_stats["not_english"] += 1
                    if not check_results.get("is_us"):
                        filter_stats["not_us"] += 1
                    if not check_results.get("subscription_ok"):
                        filter_stats["subscription_not_ok"] += 1
                    if not check_results.get("has_contact"):
                        filter_stats["no_contact"] += 1
            
            # 第二步：按 leaderboard_ranking 排序
            qualified_items_list.sort(key=lambda x: get_sort_key_for_leaderboard(x[0]))
            
            # 第三步：去重（保留有 leaderboard_ranking 的记录，如果都有则保留 rank 更好的）
            deduplicated_items = deduplicate_items(qualified_items_list)
            
            # 第四步：取前30个
            selected_items = deduplicated_items[:30]
            
            # 添加到总列表
            all_qualified_items.extend(selected_items)
            
            print(f"  ✓ 已处理 {len(data)} 条记录，合格 {len(qualified_items_list)} 条，去重后 {len(deduplicated_items)} 条，选择 {len(selected_items)} 条\n")
        except Exception as e:
            print(f"  ✗ 转换失败: {json_file}")
            print(f"    错误: {str(e)}\n")
    
    print(f"\n{'='*60}")
    print(f"第一步完成：每个文件取前30条，共收集 {len(all_qualified_items)} 条记录")
    print(f"{'='*60}\n")
    
    # 第二步：合并后再次去重（保留有 leaderboard_ranking 的记录，如果都有则保留 rank 更好的）
    print("第二步：合并后再次去重（保留有 leaderboard_ranking 的记录，如果都有则保留 rank 更好的）...")
    final_unique_items = deduplicate_items(all_qualified_items)
    duplicate_count = len(all_qualified_items) - len(final_unique_items)
    print(f"✓ 去重完成，去重后剩余 {len(final_unique_items)} 条记录，跳过 {duplicate_count} 条重复记录\n")
    
    # 第三步：按 leaderboard_ranking 重新排序
    print("第三步：重新排序...")
    final_unique_items.sort(key=lambda x: get_sort_key_for_leaderboard(x[0]))
    print(f"✓ 排序完成，最终输出 {len(final_unique_items)} 条记录\n")
    
    # 第四步：转换选中的记录
    print("第四步：转换选中的记录...")
    all_converted_data = []
    for item, source_slug in final_unique_items:
        converted_item = convert_item(item, source_slug=source_slug)
        all_converted_data.append(converted_item)
    print(f"✓ 转换完成\n")
    
    # 确保输出目录存在
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 保存合并后的数据
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_converted_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"✓ 所有文件转换完成！")
    print(f"{'='*60}")
    print(f"📊 统计信息:")
    print(f"  总记录数: {total_items}")
    print(f"  合格记录数: {qualified_items}")
    print(f"  最终输出记录数: {len(all_converted_data)}")
    print(f"  重复记录数（已跳过）: {duplicate_count}")
    print(f"  筛选率: {qualified_items/total_items*100:.1f}%" if total_items > 0 else "  筛选率: 0%")
    print(f"\n📉 筛选原因统计:")
    print(f"  非英文: {filter_stats['not_english']}")
    print(f"  非美国: {filter_stats['not_us']}")
    print(f"  订阅数不足: {filter_stats['subscription_not_ok']}")
    print(f"  无联系方式: {filter_stats['no_contact']}")
    print(f"\n📁 输出文件: {output_file}")
    print(f"{'='*60}")


if __name__ == "__main__":
    import sys
    
    # 如果提供了命令行参数，使用指定的文件
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        convert_json_file(input_file, output_file)
    else:
        # 默认转换 json/source 目录下的所有文件到 json/creator/output.json
        convert_all_json_files()

