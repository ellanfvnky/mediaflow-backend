"""
MediaFlow – FastAPI Backend v3
"""

import asyncio
import json
import os
import shutil
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

try:
    import static_ffmpeg
    static_ffmpeg.add_paths()
    print("✅ static-ffmpeg loaded")
except Exception as e:
    print(f"⚠️ static-ffmpeg: {e}")

import yt_dlp
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

app = FastAPI(title="MediaFlow API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BASE_DIR      = Path(__file__).parent
DOWNLOAD_DIR  = BASE_DIR / "downloads"
BIN_DIR       = BASE_DIR / "bin"
DATA_FILE     = BASE_DIR / "data.json"
SETTINGS_FILE = BASE_DIR / "settings.json"
COOKIES_FILE  = BASE_DIR / "cookies.txt"
DOWNLOAD_DIR.mkdir(exist_ok=True)

def find_ffmpeg():
    for name in ["ffmpeg.exe", "ffmpeg"]:
        p = BIN_DIR / name
        if p.exists():
            return str(p.parent)
    if shutil.which("ffmpeg"):
        return "system"
    return None

FFMPEG_DIR = find_ffmpeg()

def get_cookies_path():
    env_path = os.environ.get("COOKIES_PATH")
    if env_path and Path(env_path).exists():
        return env_path
    if COOKIES_FILE.exists():
        return str(COOKIES_FILE)
    return None

def _load_json(path, default):
    try:
        return json.loads(path.read_text()) if path.exists() else default
    except:
        return default

def _save_json(path, data):
    path.write_text(json.dumps(data, indent=2, default=str))

def load_history():
    return _load_json(DATA_FILE, {}).get("history", [])

def save_history(history):
    _save_json(DATA_FILE, {"history": history})

def load_settings():
    defaults = {"quality":"1080p","format":"mp4","save_dir":str(DOWNLOAD_DIR),
                "max_concurrent":3,"speed_limit":None,"use_ffmpeg":True,
                "notifications":True,"theme":"Liquid Glass","language":"English"}
    return {**defaults, **_load_json(SETTINGS_FILE, {})}

def save_settings(data):
    _save_json(SETTINGS_FILE, data)

# Format pakai fallback bestvideo+bestaudio/best agar selalu ada
QUALITY_MAP = {
    "4K":    "bestvideo[height<=2160]+bestaudio/best[height<=2160]/bestvideo+bestaudio/best",
    "1440p": "bestvideo[height<=1440]+bestaudio/best[height<=1440]/bestvideo+bestaudio/best",
    "1080p": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/bestvideo+bestaudio/best",
    "720p":  "bestvideo[height<=720]+bestaudio/best[height<=720]/bestvideo+bestaudio/best",
    "480p":  "bestvideo[height<=480]+bestaudio/best[height<=480]/bestvideo+bestaudio/best",
    "360p":  "bestvideo[height<=360]+bestaudio/best[height<=360]/bestvideo+bestaudio/best",
}

download_progress: Dict[str, dict] = {}
active_flags: Dict[str, bool] = {}

def _base_opts():
    opts = {"quiet": True, "no_warnings": True}
    cp = get_cookies_path()
    if cp:
        opts["cookiefile"] = cp
    return opts

def _make_hook(download_id):
    def hook(d):
        if not active_flags.get(download_id, True):
            raise yt_dlp.utils.DownloadError("Cancelled by user")
        if d["status"] == "downloading":
            dl  = d.get("downloaded_bytes") or 0
            tot = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            download_progress[download_id].update({
                "status": "downloading",
                "progress": round(dl / tot * 100, 1) if tot else 0,
                "downloaded_bytes": dl,
                "total_bytes": tot,
                "speed": d.get("speed") or 0,
                "eta": d.get("eta") or 0,
            })
        elif d["status"] == "finished":
            download_progress[download_id]["status"] = "converting"
    return hook

def _build_opts(download_id, fmt, quality, out_dir):
    pps = []
    if fmt == "mp3":
        fmt_str = "bestaudio/best"
        pps.append({"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "320"})
    else:
        fmt_str = QUALITY_MAP.get(quality, QUALITY_MAP["1080p"])
        pps.append({"key": "FFmpegVideoConvertor", "preferedformat": "mp4"})

    opts = {
        **_base_opts(),
        "format": fmt_str,
        "outtmpl": str(Path(out_dir) / "%(title).80s.%(ext)s"),
        "progress_hooks": [_make_hook(download_id)],
        "postprocessors": pps,
        "merge_output_format": "mp4",
        "format_sort": ["res", "ext:mp4:m4a"],
    }
    if FFMPEG_DIR and FFMPEG_DIR != "system":
        opts["ffmpeg_location"] = FFMPEG_DIR
    return opts

def _fetch_info_sync(url):
    with yt_dlp.YoutubeDL(_base_opts()) as ydl:
        info    = ydl.extract_info(url, download=False)
        heights = sorted(
            {f.get("height") for f in info.get("formats", [])
             if f.get("height") and f.get("vcodec") != "none"},
            reverse=True,
        )
        return {
            "title":               info.get("title", "Unknown"),
            "duration":            info.get("duration", 0),
            "thumbnail":           info.get("thumbnail", ""),
            "uploader":            info.get("uploader", ""),
            "platform":            info.get("extractor_key", ""),
            "available_qualities": [f"{h}p" for h in heights if h][:6] or ["720p"],
            "url":                 url,
        }

def _run_download(download_id, url, fmt, quality, out_dir):
    active_flags[download_id] = True
    try:
        with yt_dlp.YoutubeDL(_build_opts(download_id, fmt, quality, out_dir)) as ydl:
            info     = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            if fmt == "mp3":
                filename = str(Path(filename).with_suffix(".mp3"))
            fpath     = Path(filename)
            file_size = fpath.stat().st_size if fpath.exists() else 0
            download_progress[download_id].update({
                "status": "done", "progress": 100,
                "filename": filename, "file_size": file_size,
                "completed_at": datetime.now().isoformat(),
            })
            history = load_history()
            history.insert(0, dict(download_progress[download_id]))
            save_history(history[:200])
    except Exception as exc:
        download_progress[download_id].update({
            "status": "cancelled" if "Cancelled" in str(exc) else "error",
            "error":  str(exc),
        })
    finally:
        active_flags.pop(download_id, None)

class InfoReq(BaseModel):
    url: str

class DownloadReq(BaseModel):
    url:        str
    format:     str = "mp4"
    quality:    str = "1080p"
    output_dir: Optional[str] = None

@app.get("/api/health")
async def health():
    return {
        "ok":      True,
        "ffmpeg":  FFMPEG_DIR is not None or bool(shutil.which("ffmpeg")),
        "cookies": get_cookies_path() is not None,
    }

@app.post("/api/info")
async def get_info(req: InfoReq):
    try:
        return await asyncio.get_event_loop().run_in_executor(None, _fetch_info_sync, req.url)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/download")
async def start_download(req: DownloadReq):
    download_id = uuid.uuid4().hex[:8]
    out_dir     = req.output_dir or load_settings().get("save_dir", str(DOWNLOAD_DIR))
    meta = {}
    try:
        meta = await asyncio.get_event_loop().run_in_executor(None, _fetch_info_sync, req.url)
    except:
        pass
    download_progress[download_id] = {
        "id": download_id, "status": "queued", "progress": 0,
        "speed": 0, "eta": 0, "downloaded_bytes": 0, "total_bytes": 0,
        "title": meta.get("title", ""), "thumbnail": meta.get("thumbnail", ""),
        "platform": meta.get("platform", ""), "format": req.format,
        "quality": req.quality, "url": req.url,
        "started_at": datetime.now().isoformat(),
    }
    threading.Thread(
        target=_run_download,
        args=(download_id, req.url, req.format, req.quality, out_dir),
        daemon=True,
    ).start()
    return {"download_id": download_id}

@app.delete("/api/download/{download_id}")
async def cancel_download(download_id: str):
    active_flags[download_id] = False
    if download_id in download_progress:
        download_progress[download_id]["status"] = "cancelled"
    return {"ok": True}

@app.get("/api/downloads")
async def list_downloads():
    return list(download_progress.values())

@app.get("/api/history")
async def get_history():
    return load_history()

@app.get("/api/files")
async def list_files():
    files = []
    for f in sorted(DOWNLOAD_DIR.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
        if f.is_file() and not f.name.startswith("."):
            files.append({
                "name": f.name, "path": str(f), "size": f.stat().st_size,
                "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                "type": "audio" if f.suffix.lower() in {".mp3",".m4a",".ogg",".flac",".wav"} else "video",
            })
    return files

@app.delete("/api/files/{filename}")
async def delete_file(filename: str):
    fpath = DOWNLOAD_DIR / filename
    if fpath.exists() and fpath.parent.resolve() == DOWNLOAD_DIR.resolve():
        fpath.unlink()
        return {"ok": True}
    raise HTTPException(status_code=404, detail="File not found")

@app.get("/api/files/download/{filename}")
async def download_file(filename: str):
    fpath = DOWNLOAD_DIR / filename
    if fpath.exists() and fpath.parent.resolve() == DOWNLOAD_DIR.resolve():
        return FileResponse(path=str(fpath), filename=filename, media_type="application/octet-stream")
    raise HTTPException(status_code=404, detail="File not found")

@app.get("/api/settings")
async def get_settings_route():
    return load_settings()

@app.put("/api/settings")
async def update_settings_route(data: dict):
    s = load_settings()
    s.update(data)
    save_settings(s)
    return s

@app.websocket("/ws/{download_id}")
async def ws_progress(ws: WebSocket, download_id: str):
    await ws.accept()
    try:
        while True:
            prog = download_progress.get(download_id)
            if prog:
                await ws.send_json(prog)
                if prog.get("status") in ("done", "error", "cancelled"):
                    await asyncio.sleep(0.3)
                    break
            await asyncio.sleep(0.35)
    except (WebSocketDisconnect, Exception):
        pass

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    print(f"🍑 MediaFlow API → http://0.0.0.0:{port}")
    print(f"🍪 Cookies       → {get_cookies_path() or 'none'}")
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)