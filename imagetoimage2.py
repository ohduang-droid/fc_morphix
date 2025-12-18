import argparse
import base64
import os
import uuid
from datetime import datetime
from io import BytesIO
from typing import Dict, List

import boto3
import requests
from PIL import Image
from google import genai
from google.genai import types


def load_env_file(path: str = ".env"):
    """Load environment variables from a .env file (supports `export KEY=value`)."""
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as file:
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
    load_env_file()
    parser = argparse.ArgumentParser(
        description="Generate an image from prompt + reference image and upload to S3."
    )
    parser.add_argument("prompt", help="Prompt used to instruct Gemini.")
    parser.add_argument(
        "image_urls",
        nargs="+",
        help="One or more reference image URLs to feed Gemini.",
    )
    parser.add_argument(
        "--bucket",
        default=get_default_bucket(),
        help="Destination S3 bucket. Defaults to env or 'amzn-s3-fc-bucket'.",
    )
    parser.add_argument(
        "--key-prefix",
        default=get_default_key_prefix(),
        help="Optional prefix inside the bucket. Defaults to env or 'images'.",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Optional Gemini model. Defaults to env IMAGE_TO_IMAGE_MODEL / GEMINI_IMAGE_MODEL or 'gemini-2.5-flash-image'.",
    )
    return parser.parse_args()


def download_image(url: str) -> Image.Image:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return Image.open(BytesIO(resp.content)).convert("RGB")


def build_object_key(prefix: str) -> str:
    clean_prefix = prefix.strip("/ ")
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    filename = f"{uuid.uuid4().hex}.png"
    parts = [segment for segment in (clean_prefix, date_path, filename) if segment]
    return "/".join(parts)


def upload_to_s3(image: Image.Image, bucket: str, key_prefix: str) -> str:
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)

    # 获取 AWS 凭证和区域
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    
    # 创建 boto3 Session，如果环境变量中有凭证则显式传入
    if aws_access_key_id and aws_secret_access_key:
        session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
    else:
        # 如果没有显式凭证，尝试使用默认凭证链（环境变量、配置文件、IAM 角色等）
        session = boto3.session.Session(region_name=region)
    
    s3_client = session.client("s3", region_name=region)

    object_key = build_object_key(key_prefix)
    s3_client.upload_fileobj(
        buffer,
        bucket,
        object_key,
        ExtraArgs={"ACL": "public-read", "ContentType": "image/png"},
    )

    if region == "us-east-1":
        return f"https://{bucket}.s3.amazonaws.com/{object_key}"
    return f"https://{bucket}.s3.{region}.amazonaws.com/{object_key}"


def main():
    args = parse_args()
    result = generate_images_to_s3(
        prompt=args.prompt,
        image_urls=args.image_urls,
        bucket=args.bucket,
        key_prefix=args.key_prefix,
        model=args.model,
    )

    for text in result["texts"]:
        print(text)

    if not result["urls"]:
        raise RuntimeError("Gemini response did not contain an image to upload.")

    print("\nAccessible image URLs:")
    for url in result["urls"]:
        print(url)


def part_to_pil_image(part) -> Image.Image:
    """Convert Gemini inline data to a Pillow image."""
    data = part.inline_data.data
    if isinstance(data, str):
        data = base64.b64decode(data)
    return Image.open(BytesIO(data)).convert("RGB")


def get_default_bucket() -> str:
    return os.environ.get("S3_BUCKET") or os.environ.get("S3_BUCKET_NAME") or "amzn-s3-fc-bucket"


def get_default_key_prefix() -> str:
    return os.environ.get("S3_KEY_PREFIX", "images")


def get_default_model() -> str:
    return (
        os.environ.get("IMAGE_TO_IMAGE_MODEL")
        or os.environ.get("GEMINI_IMAGE_MODEL")
        or "gemini-2.5-flash-image"
    )


def generate_images_to_s3(
    prompt: str,
    image_urls: List[str],
    bucket: str | None = None,
    key_prefix: str | None = None,
    model: str | None = None,
) -> Dict[str, List[str]]:
    """
    Generate images using Gemini and upload results to S3.

    Returns dictionary with textual responses and S3 URLs.
    """

    load_env_file()
    final_bucket = bucket or get_default_bucket()
    if not final_bucket:
        raise ValueError("Missing S3 bucket configuration.")

    final_prefix = key_prefix or get_default_key_prefix()

    final_model = model or get_default_model()

    client = genai.Client()
    if not image_urls:
        raise ValueError("At least one image URL is required.")

    contents = [prompt]
    for url in image_urls:
        image = download_image(url)
        contents.append(pil_image_to_part(image))

    response = client.models.generate_images(
        model=final_model,
        contents=contents,
    )

    texts: List[str] = []
    urls: List[str] = []
    for part in response.parts:
        if part.text:
            texts.append(part.text)
        elif part.inline_data:
            pil_image = part_to_pil_image(part)
            url = upload_to_s3(pil_image, final_bucket, final_prefix)
            urls.append(url)

    return {"texts": texts, "urls": urls}


def pil_image_to_part(image: Image.Image) -> types.Part:
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)
    return types.Part.from_bytes(
        data=buffer.read(),
        mime_type="image/png",
    )


if __name__ == "__main__":
    main()
