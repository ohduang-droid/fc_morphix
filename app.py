from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from imagetoimage2 import generate_images_to_s3
from imagetovideowithextending import generate_multisegment_videos_to_s3


class ImageToImageRequest(BaseModel):
    prompt: str
    image_url: str
    bucket: Optional[str] = None
    key_prefix: Optional[str] = None
    model: Optional[str] = None


class ImageToImageResponse(BaseModel):
    urls: List[str]
    texts: List[str]


class ImageToVideoRequest(BaseModel):
    segment_prompts: Optional[List[str]] = None
    image_prompts: Optional[List[str]] = None
    image_urls: Optional[List[str]] = None
    bucket: Optional[str] = None
    key_prefix: Optional[str] = None
    poll_interval: Optional[int] = None
    max_retries: Optional[int] = None


class ImageToVideoResponse(BaseModel):
    url: str


app = FastAPI(title="FC Morphix API", version="1.0.0")


@app.post("/image-to-image", response_model=ImageToImageResponse)
async def image_to_image(payload: ImageToImageRequest):
    """
    Generate images using Gemini with an input prompt + reference image URL,
    upload them to S3, and return the accessible URLs.
    """
    try:
        result = generate_images_to_s3(
            prompt=payload.prompt,
            image_url=payload.image_url,
            bucket=payload.bucket,
            key_prefix=payload.key_prefix,
            model=payload.model,
        )
        return ImageToImageResponse(urls=result["urls"], texts=result["texts"])
    except Exception as exc:  # pragma: no cover - FastAPI handles formatting
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/image-to-video", response_model=ImageToVideoResponse)
async def image_to_video(payload: ImageToVideoRequest):
    """
    Generate multi-segment Veo videos using prompts + optional reference images,
    upload each segment to S3, and return their URLs.
    """
    try:
        final_url = generate_multisegment_videos_to_s3(
            segment_prompts=payload.segment_prompts,
            image_prompts=payload.image_prompts,
            image_urls=payload.image_urls,
            bucket=payload.bucket,
            key_prefix=payload.key_prefix,
            poll_interval=payload.poll_interval,
            max_retries=payload.max_retries,
        )
        return ImageToVideoResponse(url=final_url)
    except Exception as exc:  # pragma: no cover - FastAPI handles formatting
        raise HTTPException(status_code=500, detail=str(exc)) from exc
