"""
日志工具：为每个Creator创建独立的日志文件，记录第2-4步的写入过程
"""
import os
import logging
from typing import Optional, Any
from pathlib import Path
from datetime import datetime


LOGS_DIR = "logs"


def get_logs_dir() -> str:
    """获取日志目录路径"""
    logs_path = Path(LOGS_DIR)
    logs_path.mkdir(exist_ok=True)
    return str(logs_path)


def get_log_file_path(creator_id: Any, step: str) -> str:
    """获取日志文件路径"""
    logs_dir = get_logs_dir()
    filename = f"creator_{creator_id}_step_{step}.log"
    return os.path.join(logs_dir, filename)


def get_creator_logger(creator_id: Any, step: str) -> logging.Logger:
    """
    为指定的Creator和步骤创建独立的日志记录器
    返回: Logger 实例
    """
    logger_name = f"creator_{creator_id}_step_{step}"
    logger = logging.getLogger(logger_name)
    
    # 如果logger已经有handler，直接返回（避免重复添加）
    if logger.handlers:
        return logger
    
    # 设置日志级别
    logger.setLevel(logging.INFO)
    
    # 创建文件处理器
    log_file = get_log_file_path(creator_id, step)
    
    # 确保日志目录存在
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # 创建文件处理器，使用追加模式
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # 创建格式器
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    
    # 添加处理器到logger
    logger.addHandler(file_handler)
    
    # 防止日志传播到根logger（避免重复输出）
    logger.propagate = False
    
    return logger


def log_write_operation(
    creator_id: Any,
    step: str,
    message: str,
    level: str = "INFO"
):
    """
    记录写入操作的日志
    creator_id: Creator ID
    step: 步骤名称 (step_two, step_three, step_four)
    message: 日志消息
    level: 日志级别 (INFO, WARNING, ERROR)
    """
    logger = get_creator_logger(creator_id, step)
    
    if level.upper() == "INFO":
        logger.info(message)
    elif level.upper() == "WARNING":
        logger.warning(message)
    elif level.upper() == "ERROR":
        logger.error(message)
    else:
        logger.info(message)


def log_and_print(
    creator_id: Any,
    step: str,
    message: str,
    level: str = "INFO"
):
    """
    同时记录日志和打印到控制台
    creator_id: Creator ID
    step: 步骤名称 (step_two, step_three, step_four)
    message: 日志消息
    level: 日志级别 (INFO, WARNING, ERROR)
    """
    # 打印到控制台
    print(message)
    
    # 记录到日志文件
    log_write_operation(creator_id, step, message, level)



