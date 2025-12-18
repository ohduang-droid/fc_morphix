"""
缓存工具：以 creator_id 为标识保存和读取结果
"""
import os
import json
from typing import Dict, Any, Optional
from pathlib import Path


CACHE_DIR = "cache"


def get_cache_dir() -> str:
    """获取缓存目录路径"""
    cache_path = Path(CACHE_DIR)
    cache_path.mkdir(exist_ok=True)
    return str(cache_path)


def get_cache_file_path(creator_id: Any, step: str = "step_two") -> str:
    """获取缓存文件路径"""
    cache_dir = get_cache_dir()
    filename = f"creator_{creator_id}_{step}.json"
    return os.path.join(cache_dir, filename)


def load_cache(creator_id: Any, step: str = "step_two") -> Optional[Dict[str, Any]]:
    """
    从缓存中加载结果
    返回: 如果存在缓存则返回数据，否则返回 None
    """
    cache_file = get_cache_file_path(creator_id, step)
    
    if not os.path.exists(cache_file):
        return None
    
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    except (json.JSONDecodeError, IOError) as e:
        # 如果缓存文件损坏，删除它
        try:
            os.remove(cache_file)
        except:
            pass
        return None


def save_cache(creator_id: Any, data: Dict[str, Any], step: str = "step_two") -> bool:
    """
    保存结果到缓存
    返回: 是否保存成功
    """
    cache_file = get_cache_file_path(creator_id, step)
    
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except (IOError, OSError) as e:
        print(f"保存缓存失败 (creator_id: {creator_id}, step: {step}): {str(e)}")
        return False


def clear_cache(creator_id: Optional[Any] = None, step: Optional[str] = None) -> int:
    """
    清除缓存
    - 如果 creator_id 为 None，清除所有缓存
    - 如果 step 为 None，清除该 creator_id 的所有步骤缓存
    返回: 删除的文件数量
    """
    cache_dir = get_cache_dir()
    
    if not os.path.exists(cache_dir):
        return 0
    
    deleted_count = 0
    
    if creator_id is None:
        # 清除所有缓存
        for filename in os.listdir(cache_dir):
            file_path = os.path.join(cache_dir, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    deleted_count += 1
            except:
                pass
    else:
        # 清除特定 creator_id 的缓存
        if step is None:
            # 清除该 creator_id 的所有步骤缓存
            pattern = f"creator_{creator_id}_"
            for filename in os.listdir(cache_dir):
                if filename.startswith(pattern):
                    file_path = os.path.join(cache_dir, filename)
                    try:
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            deleted_count += 1
                    except:
                        pass
        else:
            # 清除特定 creator_id 和 step 的缓存
            cache_file = get_cache_file_path(creator_id, step)
            if os.path.exists(cache_file):
                try:
                    os.remove(cache_file)
                    deleted_count += 1
                except:
                    pass
    
    return deleted_count

