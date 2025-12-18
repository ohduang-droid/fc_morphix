import argparse
import json
import os
import tempfile
import time
import uuid
from datetime import datetime
from io import BytesIO
from typing import Any

import boto3
import requests
from PIL import Image
from google import genai
from google.genai import types

# =============================================================================
# Veo 配置模板（按需修改）
#
# 参数参考：
# - prompt / negative_prompt: 文本提示与反向提示
# - image / last_frame: 起始图与插值终帧（Image 对象）
# - reference_images: 最多 3 张风格参考图（VideoGenerationReferenceImage，仅 Veo 3.1）
# - video: 参考视频素材（仅限 Veo 3.1）
# - aspect_ratio: "16:9" 或 "9:16"
# - resolution: "720p" 或 "1080p"（部分模式限制）
# - duration_seconds: "4"、"6"、"8"（扩展、参考图常要求 8 秒）
# - person_generation: "allow_all"、"allow_adult"、"dont_allow" 等
# 这里保持默认空配置，方便统一修改。
# =============================================================================
DEFAULT_VEO_CONFIG = types.GenerateVideosConfig(
    # aspect_ratio="16:9",
    # resolution="720p",
    # duration_seconds=8,
    # person_generation="allow_all",
)

# ==============================
# 默认配置
# ==============================

DEFAULT_IMAGE_PROMPTS = [
    "Three minimalist fridge magnets on a modern fridge in a real kitchen"
]

DEFAULT_SEGMENT_PROMPTS = [
    """
    Cinematic close-up in a real modern kitchen.

    A smartphone floods with payment notifications.
    A hand pauses and does not tap.
    On-screen text appears briefly: Paid. Unread.

    In the same continuous shot, coffee steam rises.
    The protagonist lifts their eyes from the phone and looks toward the fridge.

    The camera subtly reframes to a fridge magnet labeled “Coffee With Lenny”.
    A finger gently touches the magnet.

    On-screen text fades in and out:
    Your channel. In their kitchen.
    A touchpoint you own.
    """,
    """
    Continue seamlessly from the previous clip.
    Same kitchen, same fridge, same lighting.

    The phone instantly opens to a clean Fridge Edition player.
    A waveform animates, an episode title appears.
    Audio playback begins immediately.

    A minimal subtitle appears once:
    From fridge to audio. Zero algorithm.
    """,
    """
    Continue seamlessly from the previous clip.
    Same kitchen environment.

    A dense rhythmic micro-montage of everyday moments,
    each lasting under one second,
    all unified by the same visual symbol:
    hands or eyes near the fridge while audio continues playing.

    Tea kettle boiling, tea bag drops, fingers brush the magnet.
    Water bottle refilling, audio waveform continues.
    Kids snack prep, packaging tears, sound keeps playing.
    Lunchbox packing, lid clicks shut, a subtle nod.
    Groceries put away, milk into fridge, hand taps magnet.
    Dishwasher unload nearby, plate held, a pause to listen.
    Vitamins taken, thumb swipes up to open full issue.
    Late-night fridge stare, cool light, quiet listening.

    A single subtitle appears once during the montage:
    Moments happen here.
    """,
    """
    Continue seamlessly from the previous clip.
    Maintain calm, intimate tone.

    From the audio moment, a thumb swipes up:
    Open full issue.

    Three minimal data cards flash quickly:
    Full issue opens up.
    Churn down.
    Revenue per subscriber up.

    A brief glimpse of a creator dashboard:
    Household touches.
    Plays.
    Full opens.

    Return to a calm close-up of the fridge magnet.

    Final on-screen text:
    Start your Fridge Channel.
    """
]

FINAL_HOLD_PROMPT = """
Continue seamlessly.
Hold on the fridge magnet in a calm close-up.
Minimal motion.
Soft ambient light.
No new elements introduced.
End gently.
"""


# ==============================
# 工具函数
# ==============================

def load_env_file(path: str = ".env"):
    """Load environment variables from a .env file supporting `export KEY=value` syntax."""
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


def parse_json_array(value: str | None, fallback: list[str]) -> list[str]:
    if not value:
        return fallback
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    except json.JSONDecodeError:
        pass
    return fallback


def parse_args():
    load_env_file()
    parser = argparse.ArgumentParser(
        description="Generate multi-segment Veo videos and upload each to S3."
    )
    parser.add_argument(
        "--segment-prompts",
        default=json.dumps(DEFAULT_SEGMENT_PROMPTS),
        help="JSON array of prompts, one per segment.",
    )
    parser.add_argument(
        "--image-prompts",
        default=json.dumps(DEFAULT_IMAGE_PROMPTS),
        help="JSON array of prompts for generating base images.",
    )
    parser.add_argument(
        "--image-urls",
        default="[]",
        help="JSON array of existing image URLs (first one used).",
    )
    parser.add_argument(
        "--bucket",
        default=get_default_bucket(),
        help="Destination S3 bucket. Defaults to env or 'amzn-s3-fc-bucket'.",
    )
    parser.add_argument(
        "--key-prefix",
        default=get_default_key_prefix(),
        help="Prefix inside the bucket. Defaults to env or 'videos'.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=int(os.environ.get("VEO_POLL_INTERVAL", "8")),
        help="Polling interval (seconds) while waiting for Veo operations. Default 8s.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=int(os.environ.get("VEO_MAX_RETRIES", "2")),
        help="Max retries per segment (before optional downgrade). Default 2.",
    )
    return parser.parse_args()


def download_image(url: str) -> Image.Image:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return Image.open(BytesIO(resp.content)).convert("RGB")


def generate_image_from_prompt(client: genai.Client, prompt: str) -> Any:
    response = client.models.generate_content(
        model="gemini-2.5-flash-image",
        contents=prompt,
        config={"response_modalities": ["IMAGE"]},
    )
    if not response.parts:
        raise RuntimeError("Gemini did not return an image for the provided prompt.")
    return response.parts[0].as_image()


def pil_to_genai_image(image: Image.Image) -> types.Image:
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return types.Image(image_bytes=buffer.getvalue(), mime_type="image/png")


def build_object_key(prefix: str, extension: str) -> str:
    clean_prefix = prefix.strip("/ ")
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    filename = f"{uuid.uuid4().hex}.{extension}"
    parts = [segment for segment in (clean_prefix, date_path, filename) if segment]
    return "/".join(parts)


def upload_file_to_s3(file_path: str, bucket: str, key_prefix: str) -> str:
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
    object_key = build_object_key(key_prefix, "mp4")

    s3_client.upload_file(
        Filename=file_path,
        Bucket=bucket,
        Key=object_key,
        ExtraArgs={"ACL": "public-read", "ContentType": "video/mp4"},
    )

    if region == "us-east-1":
        return f"https://{bucket}.s3.amazonaws.com/{object_key}"
    return f"https://{bucket}.s3.{region}.amazonaws.com/{object_key}"


def wait_for_operation(client: genai.Client, operation, poll_interval: int):
    while not operation.done:
        print("⏳ Waiting for video generation...")
        time.sleep(poll_interval)
        operation = client.operations.get(operation)
    return operation


def save_and_upload_video(client: genai.Client, video, bucket: str, key_prefix: str) -> str:
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_file:
        temp_path = tmp_file.name

    try:
        client.files.download(file=video)
        video.save(temp_path)
        print("⬆️ Uploading video to S3...")
        return upload_file_to_s3(temp_path, bucket, key_prefix)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def generate_initial_segment(client: genai.Client, prompt: str, image, poll_interval: int):
    print("🎬 Generating initial segment...")
    operation = client.models.generate_videos(
        model="veo-3.1-generate-preview",
        prompt=prompt,
        image=image,
        config=DEFAULT_VEO_CONFIG,
    )
    operation = wait_for_operation(client, operation, poll_interval)

    if not operation.response or not operation.response.generated_videos:
        raise RuntimeError("Failed to generate the initial video segment.")

    return operation.response.generated_videos[0].video


def generate_extend_video(
    client: genai.Client,
    base_video,
    prompt: str,
    poll_interval: int,
    max_retries: int,
    allow_downgrade: bool = False,
):
    current_prompt = prompt

    for attempt in range(1, max_retries + 2):
        print(f"▶️ Extending video | attempt {attempt}")

        operation = client.models.generate_videos(
            model="veo-3.1-generate-preview",
            video=base_video,
            prompt=current_prompt,
            config=DEFAULT_VEO_CONFIG,
        )
        operation = wait_for_operation(client, operation, poll_interval)

        if operation.response and operation.response.generated_videos:
            return operation.response.generated_videos[0].video

        print("⚠️ Veo returned no video (response is None).")

        if allow_downgrade:
            print("🔽 Downgrading prompt to final hold.")
            current_prompt = FINAL_HOLD_PROMPT

        time.sleep(5)

    raise RuntimeError("❌ Failed to generate extended video after retries.")


# ==============================
# 公开方法
# ==============================

def get_default_bucket() -> str:
    return os.environ.get("S3_BUCKET") or os.environ.get("S3_BUCKET_NAME") or "amzn-s3-fc-bucket"


def get_default_key_prefix() -> str:
    return os.environ.get("S3_VIDEO_PREFIX") or os.environ.get("S3_KEY_PREFIX") or "videos"


def generate_multisegment_videos_to_s3(
    segment_prompts: list[str] | None = None,
    image_prompts: list[str] | None = None,
    image_urls: list[str] | None = None,
    bucket: str | None = None,
    key_prefix: str | None = None,
    poll_interval: int | None = None,
    max_retries: int | None = None,
) -> str:
    """
    Generate a sequence of Veo segments and upload only the final video to S3.

    Returns the S3 URL of the last segment.
    """

    load_env_file()

    prompts = segment_prompts or DEFAULT_SEGMENT_PROMPTS
    ref_prompts = image_prompts or DEFAULT_IMAGE_PROMPTS
    ref_urls = image_urls or []

    if not prompts:
        raise ValueError("At least one segment prompt is required.")
    if not ref_urls and not ref_prompts:
        raise ValueError("Provide at least one image prompt or image URL.")

    final_bucket = bucket or get_default_bucket()
    if not final_bucket:
        raise ValueError("Missing S3 bucket configuration.")
    final_prefix = key_prefix or get_default_key_prefix()

    poll = poll_interval or int(os.environ.get("VEO_POLL_INTERVAL", "8"))
    retries = max_retries or int(os.environ.get("VEO_MAX_RETRIES", "2"))

    client = genai.Client()

    if ref_urls:
        print("🖼️ Downloading provided reference image...")
        base_image = download_image(ref_urls[0])
    else:
        print("🎨 Generating reference image from prompt...")
        base_image = generate_image_from_prompt(client, ref_prompts[0])

    genai_image = pil_to_genai_image(base_image)

    current_video = generate_initial_segment(
        client=client,
        prompt=prompts[0],
        image=genai_image,
        poll_interval=poll,
    )

    for idx in range(1, len(prompts)):
        is_last = idx == len(prompts) - 1
        current_video = generate_extend_video(
            client=client,
            base_video=current_video,
            prompt=prompts[idx],
            poll_interval=poll,
            max_retries=retries,
            allow_downgrade=is_last,
        )
    # Upload only the final video
    final_url = save_and_upload_video(client, current_video, final_bucket, final_prefix)
    return final_url


# ==============================
# 主流程
# ==============================

def main():
    args = parse_args()

    segment_prompts = parse_json_array(args.segment_prompts, DEFAULT_SEGMENT_PROMPTS)
    image_prompts = parse_json_array(args.image_prompts, DEFAULT_IMAGE_PROMPTS)
    image_urls = parse_json_array(args.image_urls, [])

    final_url = generate_multisegment_videos_to_s3(
        segment_prompts=segment_prompts,
        image_prompts=image_prompts,
        image_urls=image_urls,
        bucket=args.bucket,
        key_prefix=args.key_prefix,
        poll_interval=args.poll_interval,
        max_retries=args.max_retries,
    )

    print("\n🎉 All segments uploaded to S3:")
    print(final_url)


if __name__ == "__main__":
    main()
