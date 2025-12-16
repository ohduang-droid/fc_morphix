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

The API will be available at `http://127.0.0.1:8000`.

### API endpoints

- `POST /image-to-image`
  ```json
  {
    "prompt": "生成一个冰箱贴",
    "image_url": "https://example.com/reference.png",
    "bucket": "optional-bucket",
    "key_prefix": "optional-prefix"
  }
  ```
  Response: `{ "urls": [...], "texts": [...] }`

- `POST /image-to-video`
  ```json
  {
    "segment_prompts": ["prompt 1", "prompt 2"],
    "image_urls": ["https://example.com/start.png"],
    "bucket": "optional-bucket",
    "key_prefix": "videos",
    "poll_interval": 8,
    "max_retries": 2
  }
  ```
  Response: `{ "urls": ["https://...mp4", ...] }`

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
