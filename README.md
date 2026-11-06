# KB Reader

FastAPI app that renders the Seven74AI knowledge base: every Obsidian note plus its linked media (videos, transcripts, books), unified and deep-linkable via `#/<slug>`.

## Run

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt

# required env vars
export MINIO_ACCESS_KEY=...
export MINIO_SECRET_KEY=...
export STREAM_MEDIA_HOST=...      # public media host (default media.sevend.io)
export STREAM_INTERNAL_HOST=...   # internal object-store host fragment (optional)

# optional: restrict to one tag (summit view)
export TAG_FILTER=sommet-tdah-2026

venv/bin/python -m uvicorn app:app --host 127.0.0.1 --port 8000
```

## API

- `GET /api/catalog` — full note list with metadata (title, source, tags, speaker, day, has_video, …)
- `GET /api/note/<slug>` — note markdown
- `GET /api/transcript/<slug>` — transcript
- `GET /api/play/<slug>` — playable media URL
- `GET /img/speaker/<name>.jpg` — speaker photo

Speaker photos live in `speakers/`; static JS libraries in `static/`.
