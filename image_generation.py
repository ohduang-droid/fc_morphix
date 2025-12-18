"""
使用 DashScope ImageSynthesis API 生成图片的脚本
支持同步和异步调用方式
"""
import argparse
import os
from http import HTTPStatus
from pathlib import PurePosixPath
from urllib.parse import urlparse, unquote

import requests
from dashscope import ImageSynthesis


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


def parse_args():
    """解析命令行参数"""
    load_env_file()
    parser = argparse.ArgumentParser(
        description="使用 DashScope ImageSynthesis API 生成图片"
    )
    parser.add_argument(
        "--prompt",
        default="Eagle flying freely in the blue sky and white clouds",
        help="图片生成的提示词"
    )
    parser.add_argument(
        "--negative-prompt",
        default="garfield",
        help="负面提示词，用于排除不想要的元素"
    )
    parser.add_argument(
        "--model",
        default="stable-diffusion-3.5-large",
        help="使用的模型名称，默认为 stable-diffusion-3.5-large"
    )
    parser.add_argument(
        "--size",
        default="1024*1024",
        choices=["512*512", "1024*1024", "720*1280", "1280*720"],
        help="生成图片的尺寸，默认为 1024*1024"
    )
    parser.add_argument(
        "--n",
        type=int,
        default=1,
        help="生成图片的数量，默认为 1"
    )
    parser.add_argument(
        "--output-dir",
        default="./",
        help="图片保存目录，默认为当前目录"
    )
    parser.add_argument(
        "--async",
        action="store_true",
        dest="use_async",
        help="使用异步调用方式（默认使用同步调用）"
    )
    return parser.parse_args()


def save_image_from_url(url: str, output_dir: str = "./") -> str:
    """
    从 URL 下载图片并保存到本地
    
    Args:
        url: 图片的 URL
        output_dir: 保存目录
        
    Returns:
        保存的文件路径
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 从 URL 中提取文件名
    file_name = PurePosixPath(unquote(urlparse(url).path)).parts[-1]
    if not file_name:
        file_name = "generated_image.png"
    
    # 构建完整文件路径
    file_path = os.path.join(output_dir, file_name)
    
    # 下载并保存图片
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    with open(file_path, 'wb+') as f:
        f.write(response.content)
    
    return file_path


def sample_block_call(
    model: str,
    prompt: str,
    negative_prompt: str = "garfield",
    n: int = 1,
    size: str = "1024*1024",
    output_dir: str = "./"
):
    """
    同步调用图片生成 API
    
    Args:
        model: 模型名称
        prompt: 提示词
        negative_prompt: 负面提示词
        n: 生成图片数量
        size: 图片尺寸
        output_dir: 输出目录
    """
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        raise ValueError("缺少 DASHSCOPE_API_KEY 环境变量")
    
    print(f"开始同步生成图片...")
    print(f"模型: {model}")
    print(f"提示词: {prompt}")
    print(f"负面提示词: {negative_prompt}")
    print(f"数量: {n}, 尺寸: {size}")
    
    rsp = ImageSynthesis.call(
        model=model,
        api_key=api_key,
        prompt=prompt,
        negative_prompt=negative_prompt,
        n=n,
        size=size
    )
    
    if rsp.status_code == HTTPStatus.OK:
        print(f"生成成功！")
        print(f"响应: {rsp}")
        
        # 保存图片到指定文件夹
        saved_files = []
        for result in rsp.output.results:
            try:
                file_path = save_image_from_url(result.url, output_dir)
                saved_files.append(file_path)
                print(f"图片已保存: {file_path}")
            except Exception as e:
                print(f"保存图片失败 {result.url}: {str(e)}")
        
        return saved_files
    else:
        error_msg = (
            f'生成失败, status_code: {rsp.status_code}, '
            f'code: {rsp.code}, message: {rsp.message}'
        )
        print(error_msg)
        raise RuntimeError(error_msg)


def sample_async_call(
    model: str,
    prompt: str,
    negative_prompt: str = "garfield",
    n: int = 1,
    size: str = "512*512",
    output_dir: str = "./"
):
    """
    异步调用图片生成 API
    
    Args:
        model: 模型名称
        prompt: 提示词
        negative_prompt: 负面提示词
        n: 生成图片数量
        size: 图片尺寸
        output_dir: 输出目录
    """
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        raise ValueError("缺少 DASHSCOPE_API_KEY 环境变量")
    
    print(f"开始异步生成图片...")
    print(f"模型: {model}")
    print(f"提示词: {prompt}")
    print(f"负面提示词: {negative_prompt}")
    print(f"数量: {n}, 尺寸: {size}")
    
    # 发起异步请求
    rsp = ImageSynthesis.async_call(
        model=model,
        api_key=api_key,
        prompt=prompt,
        negative_prompt=negative_prompt,
        n=n,
        size=size
    )
    
    if rsp.status_code == HTTPStatus.OK:
        print(f"异步任务已提交")
        print(f"响应: {rsp}")
    else:
        error_msg = (
            f'提交异步任务失败, status_code: {rsp.status_code}, '
            f'code: {rsp.code}, message: {rsp.message}'
        )
        print(error_msg)
        raise RuntimeError(error_msg)
    
    # 查询任务状态
    print("查询任务状态...")
    status = ImageSynthesis.fetch(rsp)
    if status.status_code == HTTPStatus.OK:
        print(f"任务状态: {status.output.task_status}")
    else:
        error_msg = (
            f'查询任务状态失败, status_code: {status.status_code}, '
            f'code: {status.code}, message: {status.message}'
        )
        print(error_msg)
        raise RuntimeError(error_msg)
    
    # 等待任务完成
    print("等待任务完成...")
    rsp = ImageSynthesis.wait(rsp)
    if rsp.status_code == HTTPStatus.OK:
        print(f"任务完成！")
        print(f"响应: {rsp}")
        
        # 保存图片到指定文件夹
        saved_files = []
        for result in rsp.output.results:
            try:
                file_path = save_image_from_url(result.url, output_dir)
                saved_files.append(file_path)
                print(f"图片已保存: {file_path}")
            except Exception as e:
                print(f"保存图片失败 {result.url}: {str(e)}")
        
        return saved_files
    else:
        error_msg = (
            f'任务执行失败, status_code: {rsp.status_code}, '
            f'code: {rsp.code}, message: {rsp.message}'
        )
        print(error_msg)
        raise RuntimeError(error_msg)


def main():
    """主函数"""
    args = parse_args()
    
    try:
        if args.use_async:
            saved_files = sample_async_call(
                model=args.model,
                prompt=args.prompt,
                negative_prompt=args.negative_prompt,
                n=args.n,
                size=args.size,
                output_dir=args.output_dir
            )
        else:
            saved_files = sample_block_call(
                model=args.model,
                prompt=args.prompt,
                negative_prompt=args.negative_prompt,
                n=args.n,
                size=args.size,
                output_dir=args.output_dir
            )
        
        print(f"\n所有图片已成功保存到: {args.output_dir}")
        print(f"共生成 {len(saved_files)} 张图片")
        
    except Exception as e:
        print(f"错误: {str(e)}")
        raise


if __name__ == '__main__':
    main()

