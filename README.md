## FC Morphix API

This project exposes the existing `imagetoimage2.py` and `imagetovideowithextending.py`
workflows through a FastAPI service so you can trigger Gemini/Veo generations and
receive public S3 URLs for the results.

### Requirements

- Python 3.11+
- Google AI Studio key (`GOOGLE_API_KEY`) or Vertex credentials
- AWS credentials with `s3:PutObject` permissions on your bucket

### Environment configuration

Create a `.env` file in the project root (the scripts auto‑load it) :

```
export GOOGLE_API_KEY=your_google_api_key
export AWS_ACCESS_KEY_ID=your_aws_key
export AWS_SECRET_ACCESS_KEY=your_aws_secret
export AWS_DEFAULT_REGION=sa-east-1
export S3_BUCKET=amzn-s3-fc-bucket
export S3_KEY_PREFIX=images
export S3_VIDEO_PREFIX=videos
```

You can also override buckets/prefixes via API payloads or CLI flags.

### Install & run locally

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload
```

The API will be available at `http://127.0.0.1:8000`. In production, the service is
exposed behind `https://media.datail.ai/`.

### API endpoints (production base URL: `https://media.datail.ai/`)

#### POST `/image-to-image`

Request body:

```json
{
  "prompt": "生成一个冰箱贴",
  "image_url": "https://example.com/reference.png",
  "bucket": "optional-bucket",
  "key_prefix": "optional-prefix"
}
```

- `prompt` *(string, required)* – Gemini 指令
- `image_url` *(string, required)* – 参考图地址
- `bucket` *(string, optional)* – 覆盖默认 S3 bucket
- `key_prefix` *(string, optional)* – 对象前缀，默认 `images`

Response body:

```json
{
  "urls": [
    "https://amzn-s3-fc-bucket.s3.sa-east-1.amazonaws.com/images/2025/02/01/abc123.png"
  ],
  "texts": [
    "这是一个写有 Coffee With Lenny 的冰箱贴。"
  ]
}
```

- `urls` – 每个生成图片在 S3 的公网地址
- `texts` – 模型输出的文字描述（如果存在）

#### POST `/image-to-video`

Request body:

```json
{
  "segment_prompts": [
    "Segment prompt 1",
    "Segment prompt 2"
  ],
  "image_prompts": [
    "备用图 prompt"
  ],
  "image_urls": [
    "https://example.com/start.png"
  ],
  "bucket": "optional-bucket",
  "key_prefix": "videos",
  "poll_interval": 8,
  "max_retries": 2
}
```

- `segment_prompts` *(array, optional)* – 每段 Veo prompt
- `image_prompts` *(array, optional)* – 需要自动生成参考图时的 prompt
- `image_urls` *(array, optional)* – 已有的参考图 URL；如果提供则忽略 `image_prompts`
- `bucket` *(string, optional)* – 覆盖默认 S3 bucket
- `key_prefix` *(string, optional)* – 对象前缀，默认 `videos`
- `poll_interval` *(int, optional)* – 轮询间隔（秒），默认 8
- `max_retries` *(int, optional)* – 每段最大重试次数，默认 2

Response body:

```json
{
  "url": "https://amzn-s3-fc-bucket.s3.sa-east-1.amazonaws.com/videos/2025/02/01/final-segment.mp4"
}
```

仅返回最终一段视频的公网地址，前面生成的中间段落不会保留在 S3。

If `segment_prompts`, `image_prompts`, or `image_urls` are omitted the defaults from
the scripts are used.

### CLI usage

Both scripts still work via CLI:

```
python imagetoimage2.py "生成一个冰箱贴" "https://example.com/base.png"
python imagetovideowithextending.py
```

Use `--help` on each script for the available flags.

### Docker

```
docker build -t fc-morphix .
docker run --env-file .env -p 8000:8000 fc-morphix
```

The container entrypoint runs `uvicorn app:app --host 0.0.0.0 --port 8000`.
