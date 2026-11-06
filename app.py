#!/usr/bin/env python3
"""KB Reader — every Obsidian note + every MinIO video, unified. Deep-linkable via #/slug."""
import os
import io
import json
import re
import glob
import zipfile
import posixpath
import mimetypes
import xml.etree.ElementTree as ET
from urllib.parse import quote
import html as _html
from datetime import timedelta

import yaml
import markdown
from minio import Minio
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse, Response, FileResponse, StreamingResponse

VAULT = os.environ.get("OBSIDIAN_VAULT_PATH", "/root/Documents/Obsidian Vault")
KB_DIR = os.path.join(VAULT, "Knowledge base")

# MinIO: listing happens server-side against localhost; presigning signs for
# the PUBLIC host the browser reaches through the tunnel (Access-gated).
MINIO_LIST_HOST = "localhost:9000"
MINIO_PUBLIC_HOST = os.environ.get("STREAM_MEDIA_HOST", "media.sevend.io")
MINIO_ACCESS = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET = os.environ["MINIO_SECRET_KEY"]
BUCKET = "knowledge-base"
REGION = "us-east-1"
PRESIGN_TTL = timedelta(hours=2)

# Tigris (S3-compatible) presigning — active when AWS_ENDPOINT_URL_S3 is set.
TIGRIS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL_S3", "")
TIGRIS_ACCESS = os.environ.get("AWS_ACCESS_KEY_ID", "")
TIGRIS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
TIGRIS_BUCKET = os.environ.get("BUCKET_NAME", BUCKET)
VIDEO_PRESIGN_TTL = timedelta(hours=24)   # long talks must not 403 mid-playback
FILE_PRESIGN_TTL = timedelta(hours=1)     # transcripts/slides are single fetches

VIDEO_EXTS = (".mp4", ".webm", ".mkv", ".mov")
MEDIA_PREFIXES = ("reels/", "videos/", "threads/")  # reels first = preferred on slug conflict (full-quality originals)
TAG_FILTER = os.environ.get("TAG_FILTER", "").strip()  # when set, only notes with this tag (+ their videos) are served
SUMMIT_PROGRAM = [  # program order for the Sommet TDAH 2026 edition (used when TAG_FILTER set)
    "tdah-adulte-2026-j1-jonathan-moussa",
    "tdah-adulte-2026-j1-christelle-gavory",
    "tdah-adulte-2026-j1-caroline-fabbri",
    "tdah-adulte-2026-j1-virginie-diss",
    "tdah-adulte-2026-j2-jonathan-moussa",
    "tdah-adulte-2026-j2-sandra-bore",
    "tdah-adulte-2026-j2-nathalie-alsteen",
    "tdah-adulte-2026-j2-gaelle-baldassari",
    "tdah-adulte-2026-j2-nicole-schmidt-de-la-brelie",
    "tdah-adulte-2026-j2-guillemette-destors",
    "tdah-adulte-2026-j3-isabelle-et-pablo-jeanpert",
    "tdah-adulte-2026-j3-candice-levy",
    "tdah-adulte-2026-j3-sophie-balestra",
    "tdah-adulte-2026-j3-magali-scemama",
    "tdah-adulte-2026-j3-manon-luneau",
    "tdah-adulte-2026-j3-cyrielle-soulbieu",
    "tdah-adulte-2026-j4-caroline-fabbri",
    "tdah-adulte-2026-j4-laura-marie",
    "tdah-adulte-2026-j4-elodie-crepel",
    "tdah-adulte-2026-j4-celine-domecq",
    "tdah-adulte-2026-j4-virginie-diss",
    "tdah-adulte-2026-j4-melanie-jean-naudin",
    "tdah-adulte-2026-j5-isabelle-pailleau",
    "tdah-adulte-2026-j5-charlotte-chaumet",
    "tdah-adulte-2026-j5-catherine-testa-metiers",
    "tdah-adulte-2026-j5-catherine-testa-argent",
    "tdah-adulte-2026-j5-arthur-jasinski",
    "tdah-adulte-2026-j5-sara-joban",
]


def _day_of(slug: str) -> str:
    for d, pfx in (("Jour 1", "-j1-"), ("Jour 2", "-j2-"), ("Jour 3", "-j3-"),
                   ("Jour 4", "-j4-"), ("Jour 5", "-j5-")):
        if pfx in slug:
            return d
    return ""


def _program_pos(slug: str) -> int:
    try:
        return SUMMIT_PROGRAM.index(slug)
    except ValueError:
        return len(SUMMIT_PROGRAM)


SPEAKER_IMG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "speakers")

SPEAKERS = {
    "jonathan-moussa": {"name": "Dr Jonathan Moussa", "role": "Médecin spécialiste du TDAH", "img": "jonathan-moussa.jpg"},
    "christelle-gavory": {"name": "Christelle Gavory", "role": "Psychologue spécialisée en neuropsychologie", "img": "christelle-gavory.jpg"},
    "caroline-fabbri": {"name": "Caroline Fabbri", "role": "Psychoéducatrice et formatrice professionnelle", "img": "caroline-fabbri.jpg"},
    "virginie-diss": {"name": "Virginie Diss", "role": "Coach et conférencière spécialiste en parentalité neuroatypique TDAH - TOP - DYS", "img": "virginie-diss.jpg"},
    "sandra-bore": {"name": "Sandra Boré", "role": "Effervescience", "img": "sandra-bore.jpg"},
    "nathalie-alsteen": {"name": "Nathalie Alsteen", "role": "Auteure et conférencière, créatrice de la Méthode des 5 Graines©", "img": "nathalie-alsteen.jpg"},
    "gaelle-baldassari": {"name": "Gaëlle Baldassari", "role": "Autrice, créatrice de Kiffe ton Cycle", "img": "gaelle-baldassari.jpg"},
    "nicole-schmidt-de-la-brelie": {"name": "Dr Nicole Schmidt de la Brélie", "role": "Pédiatre spécialiste des troubles neuroatypiques", "img": "nicole-schmidt-de-la-brelie.jpg"},
    "guillemette-destors": {"name": "Guillemette Destors", "role": "Coach métaphorique et formatrice", "img": "guillemette-destors.jpg"},
    "candice-levy": {"name": "Candice Levy", "role": "Naturopathe, experte en santé intégrative, fondatrice de la Naturopédiatrie", "img": "candice-levy.jpg"},
    "cyrielle-soulbieu": {"name": "Cyrielle Soulbieu", "role": "Accompagnante parentale TDAH", "img": "cyrielle-soulbieu.jpg"},
    "isabelle-et-pablo-jeanpert": {"name": "Isabelle et Pablo Jeanpert", "role": "Experts en neuroatypisme, adolescence et orientation", "img": "isabelle-et-pablo-jeanpert.jpg"},
    "magali-scemama": {"name": "Magali Scemama", "role": "Coach spécialisée TDA/H", "img": "magali-scemama.jpg"},
    "manon-luneau": {"name": "Manon Luneau", "role": "Créatrice des Balades de l'Émerveillement®, formatrice « Nature et Santé »", "img": "manon-luneau.jpg"},
    "sophie-balestra": {"name": "Sophie Balestra", "role": "Nutritionniste et micronutritionniste, autrice", "img": "sophie-balestra.jpg"},
    "laura-marie": {"name": "Laura Marie", "role": "Orthophoniste, autrice, formatrice", "img": "laura-marie.jpg"},
    "elodie-crepel": {"name": "Élodie Crépel", "role": "Formatrice, conférencière, autrice, médiatrice, maître en psychanalyse spécialisée dans les atypies", "img": "elodie-crepel.jpg"},
    "celine-domecq": {"name": "Céline Domecq", "role": "Thérapeute, coach et conférencière, spécialiste des relations amoureuses et familiales", "img": "celine-domecq.jpg"},
    "melanie-jean-naudin": {"name": "Mélanie Jean-Naudin", "role": "Autrice, pair-aidante familiale en santé mentale et TND", "img": "melanie-jean-naudin.jpg"},
    "isabelle-pailleau": {"name": "Isabelle Pailleau", "role": "Psychologue du travail et des méthodes d'apprentissage, thérapeute en systémie familiale", "img": "isabelle-pailleau.jpg"},
    "charlotte-chaumet": {"name": "Charlotte Chaumet", "role": "Autrice, conférencière et créatrice de contenus", "img": "charlotte-chaumet.jpg"},
    "catherine-testa": {"name": "Catherine Testa", "role": "Autrice de « TDAH et alors ? », fondatrice de tdah.io", "img": "catherine-testa.jpg"},
    "arthur-jasinski": {"name": "Arthur Jasinski", "role": "Fondateur de L'Éveil du TDAH, auteur, coach en organisation", "img": "arthur-jasinski.jpg"},
    "sara-joban": {"name": "Sara Joban", "role": "Fondatrice de l'Université du Kiff Pédagogique", "img": "sara-joban.jpg"},
}

DAY_INFO = {
    "Jour 1": {"date": "Lundi 2 novembre", "axe": "Ouverture du Sommet", "theme": "Poser les bases : ce qu'est le TDAH, et ce qu'il n'est pas", "badge": "4 conférences incontournables · 2025"},
    "Jour 2": {"date": "Mardi 3 novembre", "axe": "Axe 1", "theme": "Comprendre son cerveau, poser un cadre et la démarche médicale", "badge": "6 conférences inédites"},
    "Jour 3": {"date": "Mercredi 4 novembre", "axe": "Axe 2", "theme": "Santé globale, régulation corporelle et hygiène de vie", "badge": "6 conférences inédites"},
    "Jour 4": {"date": "Jeudi 5 novembre", "axe": "Axe 3", "theme": "Émotions, rapport à soi, couple et dynamique familiale", "badge": "6 conférences inédites"},
    "Jour 5": {"date": "Vendredi 6 novembre", "axe": "Axe 4", "theme": "Vie professionnelle, projets et gestion du quotidien", "badge": "6 conférences inédites"},
}


def _speaker_key(slug: str) -> str:
    for pfx in ("j1-", "j2-", "j3-", "j4-", "j5-"):
        if pfx in slug:
            key = slug.split(pfx, 1)[1].rstrip("_")  # strip URL-slug trailing _
            # Catherine Testa gave two talks (metiers / argent) — one speaker.
            return key.split("-metiers")[0].split("-argent")[0]
    return ""


app = FastAPI(title="KB Reader")


@app.middleware("http")
async def no_cache(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


def _note_meta(path: str):
    """Return {title, source, date, tags, confidence} for a note (always a dict)."""
    text = open(path, encoding="utf-8").read()
    meta = {}
    body = text
    if text.startswith("---"):
        end = text.find("\n---", 3)
        if end != -1:
            try:
                meta = yaml.safe_load(text[3:end].strip()) or {}
            except Exception:
                meta = {}
            body = text[end + 4:]
    m = re.search(r"^#\s+(.+)$", body, re.M)
    title = m.group(1).strip() if m else os.path.splitext(os.path.basename(path))[0]
    dm = re.search(r"Durée[^\d]*(\d+)\s*min(?:\s*(\d+)\s*s)?", body)
    duration = ""
    if dm:
        duration = dm.group(1) + " min" + (" " + dm.group(2) + " s" if dm.group(2) else "")
    return {
        "title": title,
        "source": meta.get("source", ""),
        "date": str(meta.get("date", "")),
        "tags": meta.get("tags", []) or [],
        "confidence": meta.get("confidence", ""),
        "duration": duration,
    }


def _notes_by_slug():
    out = {}
    for path in glob.glob(os.path.join(KB_DIR, "**", "*.md"), recursive=True):
        slug = os.path.splitext(os.path.basename(path))[0]
        out[slug] = _note_meta(path)
    return out


def _note_path(slug: str) -> str | None:
    """Resolve a note's .md path, searching subfolders (videos/, etc.)."""
    for path in glob.glob(os.path.join(KB_DIR, "**", f"{slug}.md"), recursive=True):
        return path
    return None


def _humanize(slug: str) -> str:
    return slug.replace("-", " ").replace("_", " ").strip().title()


def _folder_rank(name: str) -> int:
    for i, p in enumerate(MEDIA_PREFIXES):
        if name.startswith(p):
            return i
    return len(MEDIA_PREFIXES)


def _videos_by_slug():
    """{slug: MinIO object} — deduped, reels preferred over videos/ on conflict."""
    client = Minio(MINIO_LIST_HOST, MINIO_ACCESS, MINIO_SECRET, secure=False, region=REGION)
    by_slug = {}
    for prefix in MEDIA_PREFIXES:
        for o in client.list_objects(BUCKET, prefix=prefix, recursive=True):
            name = o.object_name
            if not name.lower().endswith(VIDEO_EXTS):
                continue
            slug = os.path.splitext(name.rsplit("/", 1)[-1])[0]
            cur = by_slug.get(slug)
            if cur is None or _folder_rank(name) < _folder_rank(cur.object_name):
                by_slug[slug] = o
    return by_slug


def catalog():
    """All notes + all videos merged into one entry list."""
    notes = _notes_by_slug()
    vids = _videos_by_slug()
    if TAG_FILTER:
        tagged = {s for s, n in notes.items() if TAG_FILTER in (n["tags"] or [])}
        notes = {s: n for s, n in notes.items() if s in tagged}
        vids = {s: v for s, v in vids.items() if s in tagged}
    entries = []
    for slug in sorted(set(notes) | set(vids)):
        note = notes.get(slug)
        vid = vids.get(slug)
        spk = SPEAKERS.get(_speaker_key(slug), {})
        entries.append({
            "slug": slug,
            "title": note["title"] if note else _humanize(slug),
            "source": note["source"] if note else "",
            "date": note["date"] if note else "",
            "tags": note["tags"] if note else [],
            "confidence": note["confidence"] if note else "",
            "has_note": note is not None,
            "has_video": vid is not None,
            "key": vid.object_name if vid else "",
            "size": vid.size if vid else 0,
            "day": _day_of(slug),
            "duration": note["duration"] if note else "",
            "speaker": spk.get("name", ""),
            "role": spk.get("role", ""),
            "img": spk.get("img", ""),
        })
    if TAG_FILTER:
        return sorted(entries, key=lambda v: (_program_pos(v["slug"]), v["slug"]))
    # dated first (desc), then undated by slug
    return sorted(entries, key=lambda v: (bool(v["date"]), v["date"], v["slug"]), reverse=True)


def presign(key: str, ttl: timedelta | None = None) -> str:
    """Presign a GET URL for an object key.

    Uses Tigris (boto3, S3-compatible) when AWS_ENDPOINT_URL_S3 is set;
    otherwise falls back to the MinIO SDK. TTL defaults to PRESIGN_TTL.
    """
    ttl = ttl or PRESIGN_TTL
    if TIGRIS_ENDPOINT:
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            region_name="auto",
            endpoint_url=TIGRIS_ENDPOINT,
            aws_access_key_id=TIGRIS_ACCESS,
            aws_secret_access_key=TIGRIS_SECRET,
            config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}),
        )
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": TIGRIS_BUCKET, "Key": key},
            ExpiresIn=int(ttl.total_seconds()),
        )
    # region set explicitly so the SDK skips the GetBucketLocation network
    # call (which would hit media.sevend.io through the Access gate → 403).
    client = Minio(MINIO_PUBLIC_HOST, MINIO_ACCESS, MINIO_SECRET, secure=True, region=REGION)
    return client.presigned_get_object(BUCKET, key, expires=ttl)


def _object_key_from_url(url: str):
    """MinIO object key from a source_files URL, or None if it's an external URL."""
    u = url
    if u.startswith("minio://"):
        u = u[len("minio://"):]
    elif "://" in u:
        rest = u.split("://", 1)[1]
        host, _, path = rest.partition("/")
        internal_frag = os.environ.get("STREAM_INTERNAL_HOST", "")
        if not any(h in host for h in (internal_frag, "sevend.io", "localhost", "127.0.0.1") if h):
            return None  # external link, leave as-is
        u = path
    if u.startswith("knowledge-base/"):
        u = u[len("knowledge-base/"):]
    return u or None


def _file_label(subkey: str, url: str) -> str:
    ext = os.path.splitext(url.split("?")[0].split("#")[0])[1].lower().lstrip(".")
    if subkey == "source":
        return {"epub": "Livre (ePub)", "pdf": "Livre (PDF)", "mobi": "Livre (MOBI)"}.get(ext, "Livre")
    if subkey == "text":
        return "Texte brut"
    if subkey == "transcript":
        return "Transcription"
    if subkey == "audio":
        return "Audio"
    return subkey


def _note_source_files(slug: str) -> dict:
    path = _note_path(slug)
    if not path:
        return {}
    text = open(path, encoding="utf-8").read()
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    if end == -1:
        return {}
    try:
        meta = yaml.safe_load(text[3:end].strip()) or {}
    except Exception:
        return {}
    return meta.get("source_files", {}) or {}


@app.get("/api/catalog")
def api_catalog():
    return JSONResponse(catalog())


@app.get("/img/speaker/{name}")
def speaker_img(name: str):
    if "/" in name or ".." in name:
        raise HTTPException(status_code=400, detail="bad name")
    path = os.path.join(SPEAKER_IMG_DIR, name)
    if os.path.exists(path):
        return FileResponse(path, media_type="image/jpeg")
    raise HTTPException(status_code=404, detail="image not found")


@app.get("/api/play/{slug}")
def api_play(slug: str):
    vids = _videos_by_slug()
    if slug in vids:
        o = vids[slug]
        return JSONResponse({"url": presign(o.object_name, VIDEO_PRESIGN_TTL), "title": o.object_name})
    raise HTTPException(status_code=404, detail="video not found")


@app.get("/media/{key:path}")
def media(key: str):
    """302-redirect to a presigned URL for a raw object key.

    Video gets a 24h TTL (long talks must not 403 mid-playback); everything
    else (transcripts, slides, books) gets 1h.
    """
    ext = os.path.splitext(key)[1].lower()  # ".mp4"
    ttl = VIDEO_PRESIGN_TTL if ext in VIDEO_EXTS else FILE_PRESIGN_TTL
    return RedirectResponse(presign(key, ttl), status_code=302)


@app.get("/api/download/{slug}")
def api_download(slug: str):
    vids = _videos_by_slug()
    if slug not in vids:
        raise HTTPException(status_code=404, detail="video not found")
    key = vids[slug].object_name
    client = Minio(MINIO_LIST_HOST, MINIO_ACCESS, MINIO_SECRET, secure=False, region=REGION)
    try:
        total = client.stat_object(BUCKET, key).size
        resp = client.get_object(BUCKET, key)
    except Exception:
        raise HTTPException(status_code=404, detail="video not found")

    def _iter():
        try:
            for chunk in resp.stream(amt=1024 * 1024):
                yield chunk
        finally:
            try:
                resp.close()
                resp.release_conn()
            except Exception:
                pass

    filename = slug + os.path.splitext(key)[1]
    ct = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return StreamingResponse(
        _iter(),
        media_type=ct,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(total),
        },
    )


def _note_title(slug: str) -> str:
    path = _note_path(slug)
    if not path:
        return slug
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                m = re.match(r"^#\s+(.+?)\s*$", line)
                if m:
                    return m.group(1).strip()
    except Exception:
        pass
    return slug


_KNOWN_SLUGS = None


def _known_slugs():
    global _KNOWN_SLUGS
    if _KNOWN_SLUGS is None:
        _KNOWN_SLUGS = {os.path.basename(p)[:-3] for p in glob.glob(os.path.join(KB_DIR, "**", "*.md"), recursive=True)}
    return _KNOWN_SLUGS


def _render_wikilinks(text: str) -> str:
    known = _known_slugs()
    def rep(m):
        target = m.group(1).strip()
        if target not in known:
            return m.group(0)  # unknown target (URL/missing note) — leave literal
        alias = m.group(2).strip() if m.group(2) else _note_title(target)
        return f'<a href="#/{quote(target)}">{_html.escape(alias)}</a>'
    return re.sub(r"\[\[([^\]|]+)(?:\|([^\]]+))?\]\]", rep, text)


@app.get("/api/note/{slug}")
def api_note(slug: str):
    path = _note_path(slug)
    if not path:
        raise HTTPException(status_code=404, detail="note not found")
    text = open(path, encoding="utf-8").read()
    if text.startswith("---"):
        end = text.find("\n---", 3)
        body = text[end + 4:] if end != -1 else text
    else:
        body = text
    body = body.strip()
    body = re.sub(r"^#\s+[^\n]*\n?", "", body, count=1)  # strip duplicate H1 title
    body = re.sub(r"\n## Transcript(?:ion)?\s*\n.*?(?=\n## |\Z)", "", body, flags=re.S)  # transcript lives in its own tab
    html = markdown.markdown(body, extensions=["tables", "fenced_code", "sane_lists"])
    html = _render_wikilinks(html)
    return JSONResponse({"html": html})


@app.get("/api/files/{slug}")
def api_files(slug: str):
    sf = _note_source_files(slug)
    out = []
    for subkey, val in (sf or {}).items():
        if subkey == "video":
            continue  # the player already handles it
        if subkey == "slides":
            items = val if isinstance(val, list) else [val]
            urls = []
            for it in items:
                key = _object_key_from_url(it)
                urls.append(presign(key, FILE_PRESIGN_TTL) if key else it)
            out.append({"type": "slides", "label": "Slides", "urls": urls})
        elif isinstance(val, str):
            key = _object_key_from_url(val)
            url = presign(key, FILE_PRESIGN_TTL) if key else val
            ext = os.path.splitext((key or val).split("?")[0].split("#")[0])[1].lower().lstrip(".")
            out.append({"type": subkey, "label": _file_label(subkey, val), "url": url, "ext": ext})
    return JSONResponse({"files": out})


def _source_file_key(slug: str, subkey: str) -> str:
    sf = _note_source_files(slug)
    val = (sf or {}).get(subkey)
    if val is None:
        raise HTTPException(status_code=404, detail="source file not found")
    if isinstance(val, list):
        val = val[0]
    key = _object_key_from_url(val)
    if not key:
        raise HTTPException(status_code=400, detail="external source — cannot proxy")
    return key


def _read_object_bytes(key: str) -> bytes:
    client = Minio(MINIO_LIST_HOST, MINIO_ACCESS, MINIO_SECRET, secure=False, region=REGION)
    try:
        resp = client.get_object(BUCKET, key)
        return resp.read()
    except Exception:
        raise HTTPException(status_code=404, detail="object not found")
    finally:
        try:
            resp.close()
            resp.release_conn()
        except Exception:
            pass


_EPUB_CACHE = {}


def _epub_bytes(slug: str, subkey: str) -> bytes:
    k = (slug, subkey)
    if k not in _EPUB_CACHE:
        _EPUB_CACHE[k] = _read_object_bytes(_source_file_key(slug, subkey))
    return _EPUB_CACHE[k]


def _epub_chapters(slug: str, subkey: str, data: bytes) -> dict:
    """Parse EPUB → {'title', 'chapters': [{'title', 'html'}]}. Images rewritten to /api/epub-res."""
    zf = zipfile.ZipFile(io.BytesIO(data))
    try:
        container = ET.fromstring(zf.read("META-INF/container.xml"))
    except Exception:
        raise HTTPException(status_code=415, detail="invalid epub (no container.xml)")
    rootfile = container.find(".//{urn:oasis:names:tc:opendocument:xmlns:container}rootfile")
    if rootfile is None or not rootfile.get("full-path"):
        raise HTTPException(status_code=415, detail="invalid epub (no rootfile)")
    opf_path = rootfile.get("full-path")
    opf_dir = posixpath.dirname(opf_path)
    try:
        opf_raw = zf.read(opf_path).decode("utf-8", "replace")
        opf = ET.fromstring(opf_raw)
    except Exception:
        raise HTTPException(status_code=415, detail="invalid epub (bad opf)")
    ns = "http://www.idpf.org/2007/opf"
    manifest = {it.get("id"): it.get("href", "") for it in opf.iter(f"{{{ns}}}item")}
    spine = [ir.get("idref") for ir in opf.iter(f"{{{ns}}}itemref")]
    m = re.search(r"<dc:title[^>]*>(.*?)</dc:title>", opf_raw, re.I | re.S)
    title = re.sub(r"<[^>]+>", "", m.group(1)).strip() if m else ""

    def clean(data_html, base):
        data_html = re.sub(r"<script\b[^>]*>.*?</script>", "", data_html, flags=re.I | re.S)
        bm = re.search(r"<body[^>]*>(.*)</body>", data_html, re.I | re.S)
        if bm:
            data_html = bm.group(1)
        def img(m):
            src = m.group(1)
            if src.startswith(("http:", "https:", "data:", "#")):
                return m.group(0)
            p = posixpath.normpath(posixpath.join(base, src.split("#")[0].split("?")[0]))
            try:
                zf.getinfo(p)
            except Exception:
                return m.group(0)
            return f'src="/api/epub-res/{quote(slug)}/{quote(subkey)}/{quote(p)}"'
        return re.sub(r'src="([^"]+)"', img, data_html, flags=re.I)

    chapters = []
    for idref in spine:
        href = manifest.get(idref, "")
        if not href:
            continue
        p = posixpath.normpath(posixpath.join(opf_dir, href.split("#")[0]))
        try:
            html = zf.read(p).decode("utf-8", "replace")
        except Exception:
            continue
        tm = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
        ctitle = re.sub(r"<[^>]+>", "", tm.group(1)).strip() if tm else ""
        html = clean(html, posixpath.dirname(p))
        if not ctitle:
            hm = re.search(r"<h[1-6][^>]*>(.*?)</h[1-6]>", html, re.I | re.S)
            ctitle = re.sub(r"<[^>]+>", "", hm.group(1)).strip() if hm else href
        chapters.append({"title": ctitle, "html": html})
    return {"title": title or "ePub", "chapters": chapters}


@app.get("/api/epub/{slug}/{subkey}")
def api_epub(slug: str, subkey: str):
    return JSONResponse(_epub_chapters(slug, subkey, _epub_bytes(slug, subkey)))


@app.get("/api/epub-res/{slug}/{subkey}/{path:path}")
def api_epub_res(slug: str, subkey: str, path: str):
    zf = zipfile.ZipFile(io.BytesIO(_epub_bytes(slug, subkey)))
    try:
        raw = zf.read(path)
    except Exception:
        raise HTTPException(status_code=404, detail="resource not found")
    mime = mimetypes.guess_type(path)[0] or "application/octet-stream"
    return Response(content=raw, media_type=mime)


@app.get("/api/raw/{slug}/{subkey}")
def api_raw(slug: str, subkey: str, request: Request):
    subkey = re.sub(r"\.(epub|pdf|txt|md|json|vtt|mobi|mp3|m4a|wav)$", "", subkey)
    key = _source_file_key(slug, subkey)
    ext = os.path.splitext(key)[1].lower().lstrip(".")
    ct = {
        "txt": "text/plain; charset=utf-8",
        "md": "text/plain; charset=utf-8",
        "json": "application/json",
        "vtt": "text/vtt",
        "pdf": "application/pdf",
        "epub": "application/epub+zip",
        "mobi": "application/x-mobipocket-ebook",
    }.get(ext, "application/octet-stream")
    client = Minio(MINIO_LIST_HOST, MINIO_ACCESS, MINIO_SECRET, secure=False, region=REGION)
    try:
        total = client.stat_object(BUCKET, key).size
    except Exception:
        raise HTTPException(status_code=404, detail="object not found")
    rng = request.headers.get("range")
    if rng:
        m = re.match(r"bytes=(\d*)-(\d*)", rng)
        if m:
            start_s, end_s = m.group(1), m.group(2)
            if not start_s and end_s:
                # suffix range: last N bytes (zip central directory reads)
                start = max(0, total - int(end_s))
                end = total - 1
            else:
                start = int(start_s) if start_s else 0
                end = int(end_s) if end_s else total - 1
            end = min(end, total - 1)
            if start > end or start >= total:
                return Response(status_code=416, headers={"Content-Range": f"bytes */{total}"})
            length = end - start + 1
            resp = client.get_object(BUCKET, key, offset=start, length=length)
            try:
                data = resp.read()
            finally:
                try:
                    resp.close()
                    resp.release_conn()
                except Exception:
                    pass
            return Response(
                content=data,
                media_type=ct,
                status_code=206,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{total}",
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(length),
                },
            )
    data = _read_object_bytes(key)
    return Response(content=data, media_type=ct, headers={"Accept-Ranges": "bytes"})


@app.get("/api/transcript/{slug}")
def api_transcript(slug: str):
    """Parsed transcription annex + chapter table for a note (drives synced transcript UI)."""
    path = _note_path(slug)
    if not path:
        raise HTTPException(404, "note not found")
    text = open(path, encoding="utf-8").read()
    if text.startswith("---"):
        end = text.find("\n---", 3)
        if end != -1:
            text = text[end + 4:]
    segments = []
    cur = None
    tm = re.search(r"^## Transcript(?:ion)?\s*\n(.*?)(?=\n## |\Z)", text, re.S | re.M)
    if tm:
        for line in tm.group(1).splitlines():
            s = line.strip()
            if not s:
                continue
            m = re.match(r"^\*\((\d{1,2}):(\d{2})(?::(\d{2}))?\)\*\s*\*\*(.+?)\*\*\s*:\s*(.*)$", s)
            if m:
                if m.group(3):
                    t = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
                else:
                    t = int(m.group(1)) * 60 + int(m.group(2))
                cur = {"time": t, "speaker": m.group(4).strip(), "text": m.group(5).strip()}
                segments.append(cur)
            elif cur is not None:
                cur["text"] += " " + s
    chapters = []
    cm = re.search(r"^## (?:Chapitres|Chapters)\s*\n(.*?)(?=\n## |\Z)", text, re.S | re.M)
    if cm:
        for line in cm.group(1).splitlines():
            s = line.strip()
            m = re.match(r"^\|\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*\|([^|]+)\|", s)
            if m and "---" not in s and "Timestamp" not in s:
                t = (int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))) if m.group(3) else (int(m.group(1)) * 60 + int(m.group(2)))
                chapters.append({"time": t, "title": m.group(4).strip()})
    return JSONResponse({"segments": segments, "chapters": chapters})


@app.get("/")
def index():
    page = HTML_PAGE.replace("__SUMMIT__", "true" if TAG_FILTER else "false")
    page = page.replace("__DAY_INFO__", json.dumps(DAY_INFO, ensure_ascii=False))
    conf_count = len([e for e in catalog() if e["has_note"]])
    page = page.replace("__CONF_COUNT__", str(conf_count))
    return HTMLResponse(page)


HTML_PAGE = r"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<title>KB</title>
<style>
  :root { --bg:#0e1116; --panel:#161b22; --fg:#e6edf3; --dim:#8b949e; --acc:#58a6ff;
          --border:#21262d; --border2:#30363d; --hdr:50px;
          --row-border:#1c2128; --seg-cur:#1c2837; --on-acc:#0e1116; --code-bg:#0d1117; }
  html.light { --bg:#ffffff; --panel:#f6f8fa; --fg:#1f2328; --dim:#59636e; --acc:#0969da;
               --border:#d0d7de; --border2:#b6bfc9; --row-border:#eaeef2; --seg-cur:#ddf4ff;
               --on-acc:#ffffff; --code-bg:#f6f8fa; }
  * { box-sizing:border-box; }
  html, body { height:100%; }
  body { margin:0; font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
         background:var(--bg); color:var(--fg); }
  header { height:var(--hdr); padding:0 12px; border-bottom:1px solid var(--border);
           display:flex; align-items:center; gap:8px; }
  #back { display:none; background:none; border:none; color:var(--fg); font-size:24px;
          cursor:pointer; padding:2px 8px; line-height:1; }
  #listbtn { display:none; margin-left:auto; }
  #scrim { display:none; }
  h1 { font-size:15px; margin:0; font-weight:600; white-space:nowrap; }
  #count { color:var(--dim); font-size:11px; white-space:nowrap; }
  #search { flex:1; min-width:0; background:var(--panel); border:1px solid var(--border2);
            color:var(--fg); border-radius:6px; padding:7px 10px; font-size:16px; }
  #search:focus { outline:none; border-color:var(--acc); }
  select, button { background:var(--panel); border:1px solid var(--border2); color:var(--fg);
                   border-radius:6px; padding:7px 10px; font-size:13px; cursor:pointer; }
  button:hover, select:hover { border-color:var(--acc); }
  #nonote-label { display:flex; align-items:center; gap:4px; font-size:11px; color:var(--dim);
                  cursor:pointer; white-space:nowrap; }

  main { display:grid; grid-template-columns:340px 1fr; height:calc(100% - var(--hdr)); overflow:hidden; }
  #sidebar { display:flex; flex-direction:column; border-right:1px solid var(--border); overflow:hidden; min-height:0; }
  #tags { padding:10px 12px; border-bottom:1px solid var(--border); display:flex; flex-wrap:wrap; gap:6px; flex-shrink:0; }
  .chip { font-size:11px; color:var(--dim); border:1px solid var(--border2); border-radius:12px;
          padding:3px 9px; cursor:pointer; }
  .chip:hover { color:var(--fg); border-color:var(--acc); }
  .chip.on { color:var(--on-acc); background:var(--acc); border-color:var(--acc); font-weight:600; }
  #list { flex:1; overflow-y:auto; }
  .item { padding:12px 14px; cursor:pointer; border-bottom:1px solid var(--row-border); display:flex; gap:11px; align-items:flex-start; }
  .item:hover { background:var(--panel); }
  .item.active { background:var(--panel); border-left:3px solid var(--acc); }
  .avatar { width:44px; height:44px; border-radius:50%; object-fit:cover; flex-shrink:0; background:var(--panel); }
  .it-body { flex:1; min-width:0; }
  .tag { font-size:10px; font-weight:600; border-radius:4px; padding:1px 5px; margin-left:6px; }
  .tag-new { color:#2ea043; border:1px solid #2ea043; }
  .tag-old { color:#d29922; border:1px solid #3a2f1a; }
  .t { font-size:14px; font-weight:500; line-height:1.3; }
  .m { font-size:12px; color:var(--dim); margin-top:4px; }
  .badge { font-size:10px; color:#d29922; border:1px solid #3a2f1a; border-radius:4px;
           padding:1px 5px; margin-left:6px; }
  .badge.b2 { color:var(--dim); border-color:var(--border2); }

  #detail { display:flex; flex-direction:column; overflow:hidden; padding:18px; min-height:0; }
  video { flex-shrink:0; width:100%; max-height:48vh; background:#000; border-radius:8px; }
  #sentinel { height:1px; flex-shrink:0; }
  #novideo { flex-shrink:0; color:var(--dim); padding:36px 20px; text-align:center;
             border:1px dashed var(--border2); border-radius:8px; display:none; }
  #title { font-size:18px; font-weight:600; margin:14px 0 4px; flex-shrink:0; }
  #meta { color:var(--dim); font-size:13px; line-height:1.5; flex-shrink:0; }
  #meta a { color:var(--acc); text-decoration:none; }
  #meta .conf { font-size:11px; border:1px solid var(--border2); border-radius:4px;
                padding:1px 5px; margin-left:6px; color:var(--dim); }
  #files { flex-shrink:0; display:flex; flex-wrap:wrap; gap:8px; margin-top:10px; align-items:center; }
  #files audio { width:100%; }
  #files a.filelink { font-size:13px; color:var(--acc); text-decoration:none; border:1px solid var(--border2);
                      border-radius:6px; padding:6px 12px; }
  #files a.filelink:hover { border-color:var(--acc); }
  #files img { max-height:180px; border-radius:6px; margin-right:8px; }
  #note { flex:1 1 auto; min-height:0; overflow:auto; margin-top:14px;
          font-size:14px; line-height:1.6; }
  #note h1 { font-size:19px; margin:14px 0 6px; }
  #note h2 { font-size:16px; margin:12px 0 5px; }
  #note h3, #note h4 { font-size:14px; margin:10px 0 4px; }
  #note p { margin:8px 0; }
  #note ul, #note ol { padding-left:22px; margin:8px 0; }
  #note a { color:var(--acc); }
  #note blockquote { border-left:3px solid var(--border2); margin:10px 0; padding:2px 14px; color:var(--dim); }
  #note img { max-width:100%; border-radius:6px; }
  #note pre { background:var(--code-bg); padding:12px; border-radius:6px; overflow-x:auto; }
  #note code { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:13px; }
  #note details { margin:8px 0; }
  #note summary { cursor:pointer; color:var(--acc); }
  #note table { border-collapse:collapse; margin:10px 0; width:100%; font-size:13px; }
  #note th, #note td { border:1px solid var(--border2); padding:6px 9px; text-align:left; vertical-align:top; }
  #note th { background:var(--panel); font-weight:600; }
  #note .ts-wrap { display:flex; justify-content:space-between; align-items:center; gap:8px; }
  #note .jump { padding:3px 10px; font-size:12px; white-space:nowrap; flex-shrink:0; }
  #note blockquote.lang-note { background:var(--panel); border-left:3px solid var(--acc); border-radius:0 6px 6px 0; padding:8px 14px; color:var(--dim); font-size:13px; }
  #nav { flex-shrink:0; padding-top:14px; display:flex; gap:10px; align-items:center; }
  #copy.feedback { border-color:#2ea043; color:#2ea043; }
  #mini { position:fixed; bottom:16px; right:16px; width:220px; z-index:100; display:none;
          border-radius:8px; overflow:hidden; box-shadow:0 6px 20px rgba(0,0,0,.55); background:#000; }
  #mini.show { display:block; }
  #mini video { width:100%; display:block; max-height:30vh; max-height:30dvh; border-radius:0; }
  body.mini #detail { padding-bottom: calc(30dvh + 76px + env(safe-area-inset-bottom, 0px)); }
  #mini-close { position:absolute; top:6px; right:6px; width:26px; height:26px; background:rgba(0,0,0,.65);
                color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; line-height:1; padding:0; z-index:2; }
  #mini-speed { position:absolute; top:6px; left:6px; background:rgba(0,0,0,.65); color:#fff; border:none;
                border-radius:6px; cursor:pointer; font-size:13px; line-height:1; padding:6px 8px; z-index:2; }
  #viewer { position:fixed; inset:0; z-index:300; display:none; flex-direction:column; background:var(--bg); }
  #viewer.open { display:flex; }
  #viewer-head { height:50px; flex-shrink:0; display:flex; align-items:center; gap:10px;
                 padding:0 12px; border-bottom:1px solid var(--border); }
  #viewer-title { font-size:14px; font-weight:600; flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  #viewer-close { background:var(--panel); border:1px solid var(--border2); color:var(--fg);
                  border-radius:6px; padding:7px 10px; font-size:13px; cursor:pointer; }
  #viewer-body { flex:1; overflow:auto; }
  #viewer-body pre { margin:0; padding:18px; font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
                     font-size:13px; line-height:1.55; white-space:pre-wrap; word-break:break-word; }
  #viewer-body iframe { width:100%; height:100%; border:none; display:block; }
  #viewer-body a { color:var(--acc); }
  #epub-nav { display:flex; align-items:center; gap:10px; padding:10px 18px; border-top:1px solid var(--border);
              position:sticky; bottom:0; background:var(--bg); z-index:1; }
  #epub-nav button { padding:6px 12px; }
  #epub-pos { flex:1; text-align:center; color:var(--dim); font-size:12px; }
  #epub-chapter { padding:18px; }
  #epub-chapter h2 { margin:0 0 12px; font-size:17px; }
  .epub-content { font-size:15px; line-height:1.7; }
  .epub-content img { max-width:100%; height:auto; }
  .epub-content p { margin:10px 0; }
  .empty { color:var(--dim); padding:30px; text-align:center; }
  .kbd { font-size:10px; color:var(--dim); }

  @media (max-width:760px) {
    main { grid-template-columns:1fr; }
    #back { display:none; }
    body.viewing #back { display:block; }
    #listbtn { display:none; }
    body.viewing #listbtn { display:block; }
    #detail { display:none; padding:12px 12px calc(60px + env(safe-area-inset-bottom, 0px)); overflow-y:auto; }
    body.viewing #sidebar { display:none; }
    body.viewing #detail { display:flex; }
    body.viewing.drawer #scrim { display:block; position:fixed; top:var(--hdr); left:0; right:0; bottom:0;
                                 background:rgba(0,0,0,.5); z-index:199; }
    body.viewing.drawer #sidebar { display:flex; position:fixed; top:var(--hdr); left:0; bottom:0;
                                   width:85vw; max-width:340px; z-index:200; background:var(--bg);
                                   border-right:1px solid var(--border); }
    video { max-height:40vh; }
    #note { flex:none; overflow:visible; }
    #nav { position:fixed; left:0; right:0; bottom:0; z-index:90;
           padding:8px 12px calc(8px + env(safe-area-inset-bottom, 0px));
           background:var(--bg); border-top:1px solid var(--border); }
    #mini { bottom: calc(68px + env(safe-area-inset-bottom, 0px)); }
    .kbd { display:none; }
    #count { display:none; }
    #note table, #note thead, #note tbody, #note tr, #note th, #note td { display:block; }
    #note thead tr { display:none; }
    #note tr { border:1px solid var(--border2); border-radius:8px; padding:10px; margin:10px 0; }
    #note td { border:none; padding:3px 0; }
    #note td:first-child { color:var(--acc); font-weight:600; font-size:12px; }
    #note td:nth-child(2) { font-weight:600; }
  }

  /* ——— summit edition (TAG_FILTER set) ——— */
  #hero { display:none; padding:14px 18px; border-bottom:1px solid var(--border); }
  #hero.on { display:block; }
  #hero h2 { font-size:20px; margin:0 0 5px; }
  #hero p { margin:0; color:var(--dim); font-size:13px; }
  .day-head { padding:12px 14px 10px; border-bottom:1px solid var(--border);
              position:sticky; top:0; background:var(--bg); z-index:1; }
  .dh-meta { font-size:11px; font-weight:600; color:var(--dim); text-transform:uppercase; letter-spacing:.06em; margin-bottom:3px; }
  .dh-theme { font-size:20px; font-weight:700; color:var(--fg); line-height:1.25; }
  .dh-badge { display:inline-block; font-size:11px; color:var(--dim); border:1px solid var(--border2); border-radius:12px; padding:2px 9px; margin-top:6px; }
  .item .spk { font-size:12px; color:var(--acc); font-weight:600; margin-bottom:2px; }
  .item .spk-role { font-size:11px; color:var(--dim); margin-bottom:2px; }
  #transcript-btn.on { color:var(--on-acc); background:var(--acc); border-color:var(--acc); font-weight:600; }
  #chapters { flex-shrink:0; display:none; flex-wrap:wrap; gap:6px; margin-top:10px; }
  #chapters .chap { font-size:12px; padding:4px 10px; }
  #transcript { display:none; position:fixed; left:0; right:0; top:0; bottom:0; z-index:150;
                background:var(--bg); flex-direction:column; }
  #transcript.on { display:flex; }
  .trans-body { flex:1; overflow-y:auto; min-height:0; }
  .trans-head { flex-shrink:0; display:flex; justify-content:space-between; align-items:center;
                padding:10px 16px calc(10px + env(safe-area-inset-bottom, 0px));
                background:var(--panel); border-top:1px solid var(--border); }
  .trans-head button { padding:7px 14px; }
  #share { position:fixed; inset:0; z-index:250; display:none; align-items:center; justify-content:center; background:rgba(0,0,0,.5); }
  #share.open { display:flex; }
  #share-box { width:min(92vw, 380px); background:var(--bg); border:1px solid var(--border); border-radius:10px; padding:16px; }
  #share-head { display:flex; justify-content:space-between; align-items:center; margin-bottom:12px; }
  #share-head button { padding:4px 10px; }
  #share-toggle { display:flex; align-items:center; gap:8px; font-size:14px; margin-bottom:10px; cursor:pointer; }
  #share-time { width:100%; margin:6px 0 14px; accent-color:var(--acc); }
  #share-actions { display:flex; justify-content:flex-end; }
  #share-copy { padding:8px 16px; }
  #share-copy.feedback { border-color:#2ea043; color:#2ea043; }
  .seg { padding:7px 14px; border-bottom:1px solid var(--row-border); cursor:pointer; font-size:13.5px; line-height:1.5; }
  .seg:hover { background:var(--bg); }
  .seg.cur { background:var(--seg-cur); border-left:3px solid var(--acc); }
  .seg .t { color:var(--dim); font-size:11px; margin-right:8px; font-variant-numeric:tabular-nums; }
  .seg .s { font-weight:600; margin-right:6px; }
  .seg .s.s0 { color:#58a6ff; }
  .seg .s.s1 { color:#3fb950; }
  .seg .s.su { color:var(--dim); }
</style>
<script>document.documentElement.classList.toggle('light', localStorage.getItem('theme') === 'light');</script>
</head>
<body>
<header>
  <button id="back" aria-label="Retour">‹</button>
  <h1>KB</h1>
  <span id="count"></span>
  <input id="search" placeholder="Rechercher titre, source, tag…" autocomplete="off">
  <select id="sort">
    <option value="program">Programme</option>
    <option value="date">Récents</option>
    <option value="az">A → Z</option>
    <option value="size">Plus lourds</option>
  </select>
  <label id="nonote-label"><input type="checkbox" id="nonote"> sans note</label>
  <button id="theme" title="Mode clair/sombre">☀️</button>
</header>
<main>
  <div id="sidebar">
    <div id="hero"><h2>Sommet TDAH Adulte 2026</h2><p>__CONF_COUNT__ conférences · 2–6 novembre 2026 · Terr'à Grandir</p></div>
    <div id="tags"></div>
    <div id="list"><div class="empty">Chargement…</div></div>
  </div>
  <div id="detail">
    <video id="player" controls playsinline></video>
    <audio id="player-audio"></audio>
    <div id="sentinel"></div>
    <div id="novideo">Pas de vidéo liée à cette note.</div>
    <div id="title"></div>
    <div id="meta"></div>
    <div id="files"></div>
    <div id="chapters"></div>
    <div id="note"></div>
    <div id="transcript"></div>
    <div id="nav">
      <button id="transcript-btn">📝 Transcription</button>
      <button id="copy" aria-label="Partager"><svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true" style="display:block"><path d="M18 16.08c-.76 0-1.44.3-1.96.77L8.91 12.7c.05-.23.09-.46.09-.7s-.04-.47-.09-.7l7.05-4.11c.54.5 1.25.81 2.04.81 1.66 0 3-1.34 3-3s-1.34-3-3-3-3 1.34-3 3c0 .24.04.47.09.7L8.04 9.81C7.5 9.31 6.79 9 6 9c-1.66 0-3 1.34-3 3s1.34 3 3 3c.79 0 1.5-.31 2.04-.81l7.12 4.16c-.05.21-.08.43-.08.65 0 1.61 1.31 2.92 2.92 2.92 1.61 0 2.92-1.31 2.92-2.92s-1.31-2.92-2.92-2.92z"/></svg></button>
      <button id="download" aria-label="Télécharger la vidéo">⬇️</button>
      <button id="playpause" aria-label="Lecture/Pause">▶</button>
      <button id="speed">1×</button>
      <button id="listbtn" aria-label="Liste">☰</button>
      <span class="kbd">← → naviguer · / chercher</span>
    </div>
  </div>
</main>
<div id="scrim"></div>
<div id="mini"><button id="mini-speed" aria-label="Vitesse">1×</button><button id="mini-close" aria-label="Fermer">✕</button></div>
<div id="viewer">
  <div id="viewer-head">
    <span id="viewer-title"></span>
    <button id="viewer-close" aria-label="Fermer">✕</button>
  </div>
  <div id="viewer-body"></div>
</div>
<div id="share">
  <div id="share-box">
    <div id="share-head"><strong>Partager ce lien</strong><button id="share-close" aria-label="Fermer">✕</button></div>
    <label id="share-toggle"><input type="checkbox" id="share-on"> Commencer à <span id="share-time-label">0:00</span></label>
    <input type="range" id="share-time" min="0" max="0" step="1" value="0">
    <div id="share-actions"><button id="share-copy">Copier le lien</button></div>
  </div>
</div>
<script>
const SUMMIT = __SUMMIT__;
const DAY_INFO = __DAY_INFO__;
let entries = [], filtered = [], cur = -1, activeTag = null, searchQ = '', sortKey = SUMMIT ? 'program' : 'date', onlyNoNote = false;
let transcript = null, curSeg = -1;

function humanSize(b) {
  if (!b) return '';
  const u = ['B','KB','MB','GB']; let i = 0, n = b;
  while (n >= 1024 && i < 3) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : n) + ' ' + u[i];
}
function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function slugFromHash() {
  const h = location.hash;
  if (!h.startsWith('#/')) return null;
  return decodeURIComponent(h.slice(2).split('?')[0]);
}
function timeFromHash() {
  const m = location.hash.match(/[?&]t=(\d+)/);
  return m ? parseInt(m[1], 10) : null;
}
function fmt(t) {
  const h = Math.floor(t / 3600), m = Math.floor(t / 60) % 60, s = Math.floor(t) % 60;
  return (h ? h + ':' : '') + String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
}
function resumeKey(slug) { return 'resume_' + slug; }
function saveResume() {
  if (cur < 0) return;
  if (playerEl.currentTime > 5) localStorage.setItem(resumeKey(filtered[cur].slug), String(playerEl.currentTime));
}
function loadResume(slug) {
  const v = parseFloat(localStorage.getItem(resumeKey(slug)));
  return isFinite(v) && v > 0 ? v : 0;
}
async function fetchTranscript(slug) {
  const r = await fetch('/api/transcript/' + encodeURIComponent(slug));
  transcript = r.ok ? await r.json() : { segments: [], chapters: [] };
  renderTranscript();
  renderChapters();
}
function renderTranscript() {
  const el = document.getElementById('transcript');
  const head = '<div class="trans-head"><strong>Transcription</strong><button id="trans-close" title="Fermer">✕</button></div>';
  if (!transcript || !transcript.segments.length) { el.innerHTML = '<div class="trans-body"><div class="empty">Pas de transcription.</div></div>' + head; el.querySelector('#trans-close').onclick = toggleTranscript; return; }
  const known = [...new Set(transcript.segments.map(s => s.speaker).filter(s => s !== 'Unknown'))];
  const spkColor = {};
  known.forEach((s, i) => { spkColor[s] = 's' + (i % 2); });
  const segs = transcript.segments.map((s, i) => {
    const cls = s.speaker === 'Unknown' ? 'su' : (spkColor[s.speaker] || 's0');
    return `<div class="seg" data-i="${i}" data-t="${s.time}"><span class="t">${fmt(s.time)}</span><span class="s ${cls}">${esc(s.speaker)}</span>${esc(s.text)}</div>`;
  }).join('');
  el.innerHTML = '<div class="trans-body">' + segs + '</div>' + head;
  el.querySelector('#trans-close').onclick = toggleTranscript;
  el.querySelectorAll('.seg').forEach(seg => {
    seg.onclick = () => { playerEl.currentTime = parseFloat(seg.dataset.t); playerEl.play().catch(() => {}); };
  });
  curSeg = -1;
}
function renderChapters() {
  const el = document.getElementById('chapters');
  if (!transcript || !transcript.chapters.length) { el.innerHTML = ''; el.style.display = 'none'; return; }
  el.style.display = 'flex';
  el.innerHTML = transcript.chapters.map(c => `<button class="chap" data-t="${c.time}">${fmt(c.time)} · ${esc(c.title)}</button>`).join('');
  el.querySelectorAll('.chap').forEach(b => b.onclick = () => { playerEl.currentTime = parseFloat(b.dataset.t); playerEl.play().catch(() => {}); });
}
function makeChapterJumpButtons() {
  const note = document.getElementById('note');
  note.querySelectorAll('table tbody tr').forEach(tr => {
    const firstTd = tr.querySelector('td');
    if (!firstTd) return;
    const txt = firstTd.textContent.trim();
    const m = txt.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
    if (!m) return;
    const t = m[3] ? (+m[1]) * 3600 + (+m[2]) * 60 + (+m[3]) : (+m[1]) * 60 + (+m[2]);
    firstTd.innerHTML = `<div class="ts-wrap"><span>${txt}</span><button class="jump" title="Aller à ${txt}">▶ Aller à</button></div>`;
    firstTd.querySelector('.jump').onclick = () => { playerEl.currentTime = t; playerEl.play().catch(() => {}); };
  });
}
function toggleTranscript() {
  if (cur < 0) return;
  const el = document.getElementById('transcript');
  if (!transcript) fetchTranscript(filtered[cur].slug);
  const on = el.classList.toggle('on');
  document.getElementById('transcript-btn').classList.toggle('on', on);
}
function isMobile() { return window.matchMedia('(max-width: 760px)').matches; }

const miniBox = document.getElementById('mini');
const detailEl = document.getElementById('detail');
const playerEl = document.getElementById('player');
const audioEl = document.getElementById('player-audio');
const sentinelEl = document.getElementById('sentinel');
let miniMode = false;

// —— background audio handoff (screen locked / app backgrounded) + Media Session ——
function setupMediaSession(v) {
  if (!('mediaSession' in navigator)) return;
  try {
    navigator.mediaSession.metadata = new MediaMetadata({
      title: v.title,
      artist: v.speaker || '',
      album: 'Sommet TDAH Adulte 2026',
      artwork: v.img ? [{ src: '/img/speaker/' + v.img, sizes: '256x256', type: 'image/jpeg' }] : [],
    });
  } catch (e) {}
  const h = {
    play: () => { playerEl.play().catch(() => {}); },
    pause: () => { playerEl.pause(); audioEl.pause(); },
    seekbackward: () => seekActive(activeTime() - 15),
    seekforward: () => seekActive(activeTime() + 15),
    seekto: (d) => { if (d.seekTime != null) seekActive(d.seekTime); },
  };
  Object.keys(h).forEach(a => { try { navigator.mediaSession.setActionHandler(a, h[a]); } catch (e) {} });
}
function activeTime() { return audioEl.paused ? playerEl.currentTime : audioEl.currentTime; }
function seekActive(t) {
  t = Math.max(0, t);
  if (audioEl.paused) playerEl.currentTime = t; else audioEl.currentTime = t;
}
function handoffToAudio() {
  if (!playerEl.src || !audioEl.paused) return;
  const t = playerEl.currentTime;
  try {
    audioEl.src = playerEl.src;
    audioEl.playbackRate = playerEl.playbackRate;
    audioEl.currentTime = t;
    audioEl.play().then(() => { playerEl.pause(); }).catch(() => {});
  } catch (e) {}
}
function handoffToVideo() {
  if (!audioEl.src || audioEl.paused) return;
  const t = audioEl.currentTime;
  audioEl.pause();
  try { playerEl.currentTime = t; playerEl.play().catch(() => {}); } catch (e) {}
}
let primedSrc = '';
playerEl.addEventListener('play', () => {
  if ('mediaSession' in navigator) navigator.mediaSession.playbackState = 'playing';
  if (playerEl.src && primedSrc !== playerEl.src) {
    primedSrc = playerEl.src;
    audioEl.src = playerEl.src;
    audioEl.muted = true;
    const pp = audioEl.play();
    if (pp) pp.then(() => { audioEl.pause(); audioEl.muted = false; }).catch(() => { audioEl.muted = false; });
  }
});
playerEl.addEventListener('pause', () => { if ('mediaSession' in navigator) navigator.mediaSession.playbackState = 'paused'; });
document.addEventListener('visibilitychange', () => {
  if (!isMobile()) return;
  if (document.hidden) { if (!playerEl.paused) handoffToAudio(); }
  else handoffToVideo();
});
window.addEventListener('pagehide', () => { if (isMobile() && !playerEl.paused) handoffToAudio(); });

function enterMini() {
  if (miniMode || !playerEl.src) return;
  miniBox.appendChild(playerEl);
  miniBox.classList.add('show');
  document.body.classList.add('mini');
  miniMode = true;
}
function exitMini(scroll) {
  if (!miniMode) return;
  detailEl.insertBefore(playerEl, sentinelEl);
  miniBox.classList.remove('show');
  document.body.classList.remove('mini');
  miniMode = false;
  if (scroll) detailEl.scrollTo({ top: 0, behavior: 'smooth' });
}
new IntersectionObserver((entries) => {
  entries.forEach(e => {
    if (e.isIntersecting) { if (miniMode) exitMini(false); }
    else { if (!miniMode && playerEl.src) enterMini(); }
  });
}, { threshold: 0, rootMargin: '-50px 0px 0px 0px' }).observe(sentinelEl);
document.getElementById('mini-close').onclick = () => exitMini(true);

const SPEEDS = [1, 1.25, 1.5, 2, 2.5, 3, 4];
let speedIdx = 0;
function setSpeed(i) {
  speedIdx = i;
  playerEl.playbackRate = SPEEDS[i]; audioEl.playbackRate = SPEEDS[i];
  document.querySelectorAll('#speed, #mini-speed').forEach(b => b.textContent = SPEEDS[i] + '×');
}
document.getElementById('speed').onclick = () => setSpeed((speedIdx + 1) % SPEEDS.length);
document.getElementById('mini-speed').onclick = () => setSpeed((speedIdx + 1) % SPEEDS.length);

function openViewer(slug, sub, ext, url) {
  const viewer = document.getElementById('viewer');
  const body = document.getElementById('viewer-body');
  body.classList.remove('epub');
  document.getElementById('viewer-title').textContent = sub;
  if (['txt', 'md', 'json', 'vtt'].includes(ext)) {
    body.innerHTML = '<div class="empty">Chargement…</div>';
    fetch('/api/raw/' + encodeURIComponent(slug) + '/' + encodeURIComponent(sub))
      .then(r => { if (!r.ok) throw new Error(r.status); return r.text(); })
      .then(t => { body.innerHTML = `<pre>${esc(t)}</pre>`; })
      .catch(() => { body.innerHTML = `<div class="empty">Impossible de charger ce fichier.<br><br><a href="${esc(url)}" target="_blank">Ouvrir dans un onglet</a></div>`; });
  } else if (ext === 'pdf') {
    body.innerHTML = `<iframe src="/api/raw/${encodeURIComponent(slug)}/${encodeURIComponent(sub)}"></iframe>`;
  } else if (ext === 'epub') {
    viewer.classList.add('open');
    openEpub(slug, sub, url);
    return;
  } else {
    body.innerHTML = `<div class="empty">Ce format (${esc(ext) || 'inconnu'}) ne peut pas être affiché ici.<br><br><a href="${esc(url)}" target="_blank">Ouvrir dans un onglet</a></div>`;
  }
  viewer.classList.add('open');
}

function openEpub(slug, sub, url) {
  const body = document.getElementById('viewer-body');
  document.getElementById('viewer-title').textContent = 'ePub';
  body.innerHTML = '<div class="empty">Chargement…</div>';
  fetch('/api/epub/' + encodeURIComponent(slug) + '/' + encodeURIComponent(sub))
    .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
    .then(book => renderEpub(book))
    .catch(() => {
      body.innerHTML = `<div class="empty">Impossible de charger l'ePub.<br><br><a href="${esc(url)}" target="_blank">Ouvrir dans un onglet</a></div>`;
    });
}
let epubIdx = 0;
function renderEpub(book) {
  const body = document.getElementById('viewer-body');
  document.getElementById('viewer-title').textContent = book.title || 'ePub';
  if (!book.chapters || !book.chapters.length) { body.innerHTML = '<div class="empty">Aucun chapitre.</div>'; return; }
  epubIdx = 0;
  function draw() {
    const c = book.chapters[epubIdx];
    body.innerHTML = `
      <div id="epub-chapter"><h2>${esc(c.title)}</h2><div class="epub-content">${c.html}</div></div>
      <div id="epub-nav">
        <button id="epub-prev"${epubIdx === 0 ? ' disabled' : ''}>←</button>
        <span id="epub-pos">${epubIdx + 1} / ${book.chapters.length}</span>
        <button id="epub-next"${epubIdx === book.chapters.length - 1 ? ' disabled' : ''}>→</button>
      </div>`;
    document.getElementById('epub-prev').onclick = () => { if (epubIdx > 0) { epubIdx--; draw(); body.scrollTop = 0; } };
    document.getElementById('epub-next').onclick = () => { if (epubIdx < book.chapters.length - 1) { epubIdx++; draw(); body.scrollTop = 0; } };
  }
  draw();
}
document.getElementById('viewer-close').onclick = () => {
  document.getElementById('viewer').classList.remove('open');
  const b = document.getElementById('viewer-body');
  b.classList.remove('epub');
  b.innerHTML = '';
};

async function load() {
  const r = await fetch('/api/catalog');
  entries = await r.json();
  const initialSlug = slugFromHash() || localStorage.getItem('last_slug');
  buildChips();
  apply();
  if (initialSlug) selectBySlug(initialSlug);
}

function buildChips() {
  const c = {};
  entries.forEach(v => (v.tags||[]).forEach(t => c[t] = (c[t]||0)+1));
  const top = Object.entries(c).sort((a,b)=>b[1]-a[1]).slice(0,16).map(e=>e[0]);
  const el = document.getElementById('tags');
  el.innerHTML = top.map(t => `<span class="chip" data-tag="${esc(t)}">#${esc(t)}</span>`).join('');
  el.querySelectorAll('.chip').forEach(ch => ch.onclick = () => {
    activeTag = activeTag === ch.dataset.tag ? null : ch.dataset.tag;
    el.querySelectorAll('.chip').forEach(x => x.classList.toggle('on', x.dataset.tag === activeTag));
    apply();
  });
}

function apply() {
  filtered = entries.filter(v => {
    if (onlyNoNote && v.has_note) return false;
    if (activeTag && !(v.tags||[]).includes(activeTag)) return false;
    if (searchQ) {
      const hay = (v.title + ' ' + (v.source||'') + ' ' + (v.tags||[]).join(' ')).toLowerCase();
      if (!hay.includes(searchQ)) return false;
    }
    return true;
  });
  if (sortKey === 'az') filtered.sort((a,b) => a.title.localeCompare(b.title, 'fr'));
  else if (sortKey === 'size') filtered.sort((a,b) => b.size - a.size);
  else if (sortKey === 'date') filtered.sort((a,b) => (b.date||'').localeCompare(a.date||''));
  // 'program' keeps the server's program order
  document.getElementById('count').textContent = filtered.length + ' / ' + entries.length;
  render();
  if (cur >= filtered.length) cur = -1;
  if (cur === -1 && filtered.length && !isMobile()) select(0);
}

function render() {
  const list = document.getElementById('list');
  list.innerHTML = '';
  if (!filtered.length) { list.innerHTML = '<div class="empty">Aucun résultat.</div>'; return; }
  let lastDay = null;
  filtered.forEach((v, i) => {
    if (SUMMIT && v.day && v.day !== lastDay) {
      lastDay = v.day;
      const info = DAY_INFO[v.day] || {};
      const dh = document.createElement('div');
      dh.className = 'day-head';
      dh.innerHTML = `<div class="dh-meta">${esc(v.day)} · ${esc(info.date || '')}${info.axe ? ' · ' + esc(info.axe) : ''}</div>
        <div class="dh-theme">${esc(info.theme || '')}</div>
        <div class="dh-badge">${esc(info.badge || '')}</div>`;
      list.appendChild(dh);
    }
    const el = document.createElement('div');
    el.className = 'item' + (i === cur ? ' active' : '');
    const dur = v.duration ? esc(v.duration) : humanSize(v.size);
    let avatar = '', spk = '', metaStr = '';
    if (SUMMIT) {
      const isNew = v.day !== 'Jour 1';
      const tag = `<span class="tag${isNew ? ' tag-new' : ' tag-old'}">${isNew ? 'Inédite' : '2025'}</span>`;
      if (v.speaker) spk += `<div class="spk">${esc(v.speaker)}</div>`;
      if (v.role) spk += `<div class="spk-role">${esc(v.role)}</div>`;
      if (v.img) avatar = `<img class="avatar" src="/img/speaker/${esc(v.img)}" alt="">`;
      metaStr = dur + ' · ' + tag;
    } else {
      metaStr = (v.date ? esc(v.date) + ' · ' : '') + dur;
    }
    let badge = '';
    if (!v.has_note) badge += '<span class="badge">sans note</span>';
    if (!v.has_video) badge += '<span class="badge b2">sans vidéo</span>';
    el.innerHTML = avatar + `<div class="it-body">${spk}<div class="t">${esc(v.title)}${badge}</div>
      <div class="m">${metaStr}</div></div>`;
    el.onclick = () => select(i);
    list.appendChild(el);
  });
}

async function select(i) {
  if (i < 0 || i >= filtered.length) return;
  if (i === cur) return;
  cur = i;
  const v = filtered[i];
  const shareTime = timeFromHash();
  transcript = null; curSeg = -1;
  document.getElementById('transcript').classList.remove('on');
  document.getElementById('transcript-btn').classList.remove('on');
  document.getElementById('chapters').style.display = 'none';
  history.replaceState(null, '', '#/' + encodeURIComponent(v.slug));
  if (isMobile()) document.body.classList.add('viewing');
  document.body.classList.remove('drawer');
  if (miniMode) exitMini(false);
  detailEl.scrollTo({ top: 0 });
  document.querySelectorAll('.item').forEach((x, j) => x.classList.toggle('active', j === i));
  document.getElementById('title').textContent = v.title;
  const meta = document.getElementById('meta');
  let src = v.source;
  if (src && src.startsWith('http')) src = `<a href="${esc(src)}" target="_blank">${esc(src)}</a>`;
  else src = esc(src);
  const tags = (v.tags||[]).map(t => `#${esc(t)}`).join(' ');
  const conf = v.confidence ? `<span class="conf">${esc(v.confidence)}</span>` : '';
  meta.innerHTML = [src, tags, conf].filter(x => x.trim()).join(' · ');

  const p = document.getElementById('player');
  const novideo = document.getElementById('novideo');
  if (v.has_video) {
    p.style.display = 'block'; novideo.style.display = 'none';
    document.getElementById('playpause').style.display = '';
    document.getElementById('speed').style.display = '';
    document.getElementById('download').style.display = '';
    document.getElementById('transcript-btn').style.display = '';
    const r = await fetch('/api/play/' + encodeURIComponent(v.slug));
    if (r.ok) { const { url } = await r.json(); p.src = url; audioEl.src = url; p.playbackRate = SPEEDS[speedIdx]; audioEl.playbackRate = SPEEDS[speedIdx]; setupMediaSession(v); const rt = shareTime != null ? shareTime : loadResume(v.slug); if (rt > 0) p.addEventListener('loadedmetadata', () => { try { p.currentTime = rt; } catch(e){} }, { once:true }); p.play().catch(() => {}); }
  } else {
    p.pause(); p.removeAttribute('src'); audioEl.pause(); audioEl.removeAttribute('src'); p.style.display = 'none'; novideo.style.display = 'block';
    document.getElementById('playpause').style.display = 'none';
    document.getElementById('speed').style.display = 'none';
    document.getElementById('download').style.display = 'none';
    document.getElementById('transcript-btn').style.display = 'none';
  }

  const noteEl = document.getElementById('note');
  if (v.has_note) {
    noteEl.innerHTML = '<div class="empty">Chargement…</div>';
    const nr = await fetch('/api/note/' + encodeURIComponent(v.slug));
    noteEl.innerHTML = nr.ok ? (await nr.json()).html : '<div class="empty">Note introuvable.</div>';
    noteEl.querySelector('blockquote')?.classList.add('lang-note');
    makeChapterJumpButtons();
  } else {
    noteEl.innerHTML = '<div class="empty">Aucune note liée à cette vidéo.</div>';
  }

  const filesEl = document.getElementById('files');
  filesEl.innerHTML = '';
  if (v.has_note) {
    const fr = await fetch('/api/files/' + encodeURIComponent(v.slug));
    if (fr.ok) {
      const { files } = await fr.json();
      filesEl.innerHTML = files.map(f => {
        if (f.type === 'audio') return `<audio controls src="${esc(f.url)}"></audio>`;
        if (f.type === 'slides') return f.urls.map(u => `<img src="${esc(u)}" loading="lazy">`).join('');
        return `<button class="filelink" data-sub="${esc(f.type)}" data-ext="${esc(f.ext || '')}" data-url="${esc(f.url)}">📄 ${esc(f.label)}</button>`;
      }).join('');
      filesEl.querySelectorAll('button.filelink').forEach(b => {
        b.onclick = () => openViewer(v.slug, b.dataset.sub, b.dataset.ext, b.dataset.url);
      });
    }
  }

  fetchTranscript(v.slug);
  document.querySelector('.item.active')?.scrollIntoView({block:'nearest'});
}

function selectBySlug(slug) {
  let i = filtered.findIndex(v => v.slug === slug);
  if (i === -1) {
    searchQ = ''; activeTag = null; onlyNoNote = false;
    document.getElementById('search').value = '';
    document.getElementById('nonote').checked = false;
    document.querySelectorAll('.chip').forEach(x => x.classList.remove('on'));
    apply();
    i = filtered.findIndex(v => v.slug === slug);
  }
  if (i >= 0) select(i);
}

function step(d) { select(cur + d); }

document.getElementById('search').addEventListener('input', e => { searchQ = e.target.value.trim(); apply(); });
document.getElementById('sort').addEventListener('change', e => { sortKey = e.target.value; apply(); });
document.getElementById('nonote').addEventListener('change', e => { onlyNoNote = e.target.checked; apply(); });
document.getElementById('back').onclick = () => { document.body.classList.remove('viewing', 'drawer'); playerEl.pause(); audioEl.pause(); };
document.getElementById('listbtn').onclick = () => document.body.classList.toggle('drawer');
document.getElementById('scrim').onclick = () => document.body.classList.remove('drawer');
document.getElementById('playpause').onclick = () => {
  if (playerEl.paused) playerEl.play().catch(() => {}); else playerEl.pause();
};
function updatePlayPause() { document.getElementById('playpause').textContent = playerEl.paused ? '▶' : '⏸'; }
playerEl.addEventListener('play', updatePlayPause);
playerEl.addEventListener('pause', updatePlayPause);
document.getElementById('copy').onclick = openShare;
document.getElementById('download').onclick = () => {
  const v = filtered[cur];
  if (v && v.has_video) window.location.href = '/api/download/' + encodeURIComponent(v.slug);
};
async function copyText(text) {
  let ok = false;
  if (navigator.clipboard && window.isSecureContext) {
    try { await navigator.clipboard.writeText(text); ok = true; } catch(e) {}
  }
  if (!ok) {
    try {
      const ta = document.createElement('textarea');
      ta.value = text; ta.setAttribute('readonly', '');
      ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select(); ta.setSelectionRange(0, text.length);
      ok = document.execCommand('copy');
      document.body.removeChild(ta);
    } catch(e) {}
  }
  return ok;
}
function openShare() {
  if (cur < 0) return;
  const v = filtered[cur];
  let dur = isFinite(playerEl.duration) ? playerEl.duration : 0;
  if (!dur && v.duration) {
    const m = v.duration.match(/(\d+)\s*min(?:\s*(\d+)\s*s)?/);
    if (m) dur = (+m[1]) * 60 + (m[2] ? +m[2] : 0);
  }
  const t = playerEl.currentTime;
  const slider = document.getElementById('share-time');
  slider.max = Math.max(0, Math.floor(dur));
  slider.value = Math.floor(t);
  document.getElementById('share-on').checked = t > 0;
  document.getElementById('share-time-label').textContent = fmt(t);
  document.getElementById('share').classList.add('open');
}
document.getElementById('share-time').addEventListener('input', () => {
  document.getElementById('share-time-label').textContent = fmt(parseFloat(document.getElementById('share-time').value));
});
document.getElementById('share-close').onclick = () => document.getElementById('share').classList.remove('open');
document.getElementById('share').addEventListener('click', e => { if (e.target.id === 'share') document.getElementById('share').classList.remove('open'); });
document.getElementById('share-copy').onclick = async function() {
  const v = filtered[cur];
  const base = location.href.split('#')[0];
  let link = base + '#/' + encodeURIComponent(v.slug);
  if (document.getElementById('share-on').checked) {
    link += '?t=' + Math.floor(parseFloat(document.getElementById('share-time').value));
  }
  const ok = await copyText(link);
  this.textContent = ok ? '✅ Copié' : '⚠️ Erreur';
  this.classList.toggle('feedback', ok);
  setTimeout(() => { this.textContent = 'Copier le lien'; this.classList.remove('feedback'); document.getElementById('share').classList.remove('open'); }, 1200);
};
document.addEventListener('keydown', e => {
  if (e.key === 'ArrowLeft') step(-1);
  else if (e.key === 'ArrowRight') step(1);
  else if (e.key === 'Escape') {
    const v = document.getElementById('viewer');
    if (v.classList.contains('open')) {
      v.classList.remove('open');
      const b = document.getElementById('viewer-body');
      b.classList.remove('epub');
      b.innerHTML = '';
    }
    else if (isMobile()) document.body.classList.remove('viewing');
  }
  else if (e.key === '/' && document.activeElement !== document.getElementById('search')) {
    e.preventDefault(); document.getElementById('search').focus();
  }
});
document.getElementById('theme').textContent = document.documentElement.classList.contains('light') ? '🌙' : '☀️';
document.getElementById('theme').onclick = () => {
  const isLight = document.documentElement.classList.toggle('light');
  document.getElementById('theme').textContent = isLight ? '🌙' : '☀️';
  localStorage.setItem('theme', isLight ? 'light' : 'dark');
};
if (SUMMIT) {
  document.getElementById('hero').classList.add('on');
  document.querySelector('header h1').textContent = 'Sommet TDAH 2026';
  document.title = 'Sommet TDAH Adulte 2026';
  document.getElementById('sort').value = 'program';
  document.getElementById('search').style.display = 'none';
  document.getElementById('tags').style.display = 'none';
  document.getElementById('nonote-label').style.display = 'none';
} else {
  // Non-summit homepage: no program order — default to most recent (date)
  // and drop the meaningless "Programme" option so the dropdown matches sortKey.
  const programOpt = document.querySelector('#sort option[value="program"]');
  if (programOpt) programOpt.style.display = 'none';
  document.getElementById('sort').value = 'date';
}
document.getElementById('transcript-btn').onclick = toggleTranscript;
playerEl.addEventListener('timeupdate', () => {
  if (transcript && document.getElementById('transcript').classList.contains('on') && transcript.segments.length) {
    const t = playerEl.currentTime;
    let i = -1;
    for (let j = 0; j < transcript.segments.length; j++) { if (transcript.segments[j].time <= t) i = j; else break; }
    if (i !== curSeg) {
      curSeg = i;
      document.querySelectorAll('.seg').forEach((s, j) => s.classList.toggle('cur', j === i));
      const c = document.querySelector('.seg.cur');
      if (c) c.scrollIntoView({ block: 'center', behavior: 'smooth' });
    }
  }
});
let _lastSave = 0;
playerEl.addEventListener('timeupdate', () => {
  const now = Date.now();
  if (now - _lastSave > 5000) { _lastSave = now; saveResume(); }
});
window.addEventListener('beforeunload', () => { saveResume(); if (cur >= 0) localStorage.setItem('last_slug', filtered[cur].slug); });
window.addEventListener('hashchange', () => {
  const slug = slugFromHash();
  if (slug) selectBySlug(slug);
});
load();
</script>
</body>
</html>
"""
