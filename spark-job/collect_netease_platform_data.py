import argparse
import concurrent.futures
import json
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

try:
    import pymysql
except ModuleNotFoundError:
    pymysql = None


MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "music_system")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
NETEASE_TOPLIST_API = os.getenv("NETEASE_TOPLIST_API", "https://music.163.com/api/toplist/detail")
NETEASE_PLAYLIST_DETAIL_API = os.getenv(
    "NETEASE_PLAYLIST_DETAIL_API",
    "https://music.163.com/api/v6/playlist/detail",
)
NETEASE_PLAYLIST_LIST_API = os.getenv("NETEASE_PLAYLIST_LIST_API", "https://music.163.com/api/playlist/list")
NETEASE_LYRIC_API = os.getenv("NETEASE_LYRIC_API", "https://music.163.com/api/song/lyric")
NETEASE_ARTIST_TOP_API = os.getenv("NETEASE_ARTIST_TOP_API", "https://music.163.com/api/artist/top")
NETEASE_ARTIST_TOP_SONG_API = os.getenv("NETEASE_ARTIST_TOP_SONG_API", "https://music.163.com/api/artist/top/song")
NETEASE_SSL_VERIFY = os.getenv("NETEASE_SSL_VERIFY", "false").lower() == "true"
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
AI_API_KEY = os.getenv("AI_API_KEY", "")
AI_MODEL = os.getenv("AI_MODEL", "qwen-plus")
AI_TAG_CONCURRENCY = max(1, int(os.getenv("AI_TAG_CONCURRENCY", "6")))
AI_TIMEOUT_SEC = int(os.getenv("AI_TIMEOUT_SEC", "25"))
PRIORITY_RANK_NAMES = ["飙升榜", "新歌榜", "原创榜", "热歌榜"]


def mysql_conn():
    if pymysql is None:
        raise RuntimeError("pymysql is required, please install dependencies from spark-job/requirements.txt")
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True,
    )


def write_task_log(conn, status, message, start_time, end_time):
    duration_ms = int((end_time - start_time) * 1000)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sys_task_log(task_name, task_type, status, retry_count, start_time, end_time, duration_ms, message, trace_id)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                "netease_platform_collector",
                "collect",
                status,
                0,
                datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S"),
                datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S"),
                duration_ms,
                message,
                f"trace-{int(end_time * 1000)}",
            ),
        )


def ensure_required_tables(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS artists (
              artist_id VARCHAR(32) PRIMARY KEY,
              artist_name VARCHAR(128) NOT NULL,
              artist_url VARCHAR(512),
              region VARCHAR(64),
              style VARCHAR(128),
              created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              INDEX idx_artist_name(artist_name),
              INDEX idx_artist_updated_at(updated_at)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS songs (
              song_id VARCHAR(32) PRIMARY KEY,
              song_name VARCHAR(256) NOT NULL,
              duration_sec INT,
              album_name VARCHAR(256),
              lyric_text LONGTEXT,
              song_url VARCHAR(512),
              source_platform VARCHAR(32) NOT NULL DEFAULT 'netease',
              publish_time DATETIME,
              created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              INDEX idx_song_name(song_name),
              INDEX idx_album_name(album_name),
              INDEX idx_song_updated_at(updated_at)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ranks (
              rank_id VARCHAR(32) PRIMARY KEY,
              rank_name VARCHAR(128) NOT NULL,
              rank_order INT NOT NULL DEFAULT 9999,
              rank_url VARCHAR(512),
              rank_category VARCHAR(64),
              update_cycle VARCHAR(32),
              created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS playlists (
              playlist_id VARCHAR(32) PRIMARY KEY,
              playlist_name VARCHAR(256) NOT NULL,
              playlist_url VARCHAR(512),
              creator_user_id VARCHAR(32),
              play_count BIGINT,
              collect_count BIGINT,
              track_count INT,
              description TEXT,
              created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              INDEX idx_playlist_name(playlist_name),
              INDEX idx_play_count(play_count),
              INDEX idx_playlist_updated_at(updated_at)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS artist_songs (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              artist_id VARCHAR(32) NOT NULL,
              song_id VARCHAR(32) NOT NULL,
              role_type VARCHAR(32),
              crawl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY uk_artist_song(artist_id, song_id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS playlist_songs (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              playlist_id VARCHAR(32) NOT NULL,
              song_id VARCHAR(32) NOT NULL,
              position INT,
              added_at DATETIME,
              crawl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY uk_playlist_song(playlist_id, song_id),
              INDEX idx_playlist_position(playlist_id, position),
              INDEX idx_playlist_song_crawl_time(crawl_time)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rank_songs (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              rank_id VARCHAR(32) NOT NULL,
              song_id VARCHAR(32) NOT NULL,
              rank_position INT NOT NULL,
              score DOUBLE,
              crawl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY uk_rank_song_time(rank_id, song_id, crawl_time),
              INDEX idx_rank_time_pos(rank_id, crawl_time, rank_position)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS song_tags (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              song_id VARCHAR(32) NOT NULL,
              tag_name VARCHAR(64) NOT NULL,
              score DECIMAL(6,4),
              source_model VARCHAR(64) NOT NULL,
              source_version VARCHAR(32),
              updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              UNIQUE KEY uk_song_tag(song_id, tag_name, source_model),
              INDEX idx_song_tag_updated_at(updated_at)
            )
            """
        )
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name='songs'
            """,
            (MYSQL_DB,),
        )
        song_columns = {row[0] for row in cursor.fetchall()}
        if "lyric_text" not in song_columns:
            cursor.execute("ALTER TABLE songs ADD COLUMN lyric_text LONGTEXT")
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name='ranks'
            """,
            (MYSQL_DB,),
        )
        rank_columns = {row[0] for row in cursor.fetchall()}
        if "rank_order" not in rank_columns:
            cursor.execute("ALTER TABLE ranks ADD COLUMN rank_order INT NOT NULL DEFAULT 9999")
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name='dim_song'
            """,
            (MYSQL_DB,),
        )
        legacy_song_columns = {row[0] for row in cursor.fetchall()}
        if "lyric_text" not in legacy_song_columns:
            cursor.execute("ALTER TABLE dim_song ADD COLUMN lyric_text LONGTEXT")
        required = {
            "songs",
            "artists",
            "playlists",
            "artist_songs",
            "playlist_songs",
            "ranks",
            "rank_songs",
            "song_tags",
            "sys_task_log",
            "dim_song",
            "dim_artist",
        }
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            """,
            (MYSQL_DB,),
        )
        existing = {row[0] for row in cursor.fetchall()}
    missing = sorted(required - existing)
    if missing:
        raise RuntimeError(f"missing tables: {', '.join(missing)}")


def http_get_json(url):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://music.163.com/",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = resp.read().decode("utf-8")
    except urllib.error.URLError:
        if NETEASE_SSL_VERIFY:
            raise
        insecure_context = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=20, context=insecure_context) as resp:
            payload = resp.read().decode("utf-8")
    return json.loads(payload)


def fetch_toplists(limit):
    body = http_get_json(NETEASE_TOPLIST_API)
    all_lists = body.get("list", [])
    result = []
    for idx, item in enumerate(all_lists):
        rank_id = item.get("id")
        rank_name = item.get("name")
        if not rank_id or not rank_name:
            continue
        result.append(
            {
                "rank_id": str(rank_id),
                "rank_name": str(rank_name)[:128],
                "rank_url": f"https://music.163.com/#/discover/toplist?id={rank_id}",
                "rank_category": "官方榜" if item.get("ToplistType") else "特色榜",
                "update_cycle": (item.get("updateFrequency") or "")[:32] or None,
                "_source_index": idx,
            }
        )
    priority_map = {name: idx for idx, name in enumerate(PRIORITY_RANK_NAMES)}
    result.sort(
        key=lambda rec: (
            priority_map.get(rec["rank_name"], 1000 + rec["_source_index"]),
            rec["_source_index"],
        )
    )
    ordered = []
    for idx, rec in enumerate(result[:limit], start=1):
        rec["rank_order"] = idx
        rec.pop("_source_index", None)
        ordered.append(rec)
    return ordered


def fetch_rank_tracks(rank_id, track_limit):
    query = urllib.parse.urlencode({"id": rank_id, "n": max(track_limit, 50), "s": 0})
    url = f"{NETEASE_PLAYLIST_DETAIL_API}?{query}"
    body = http_get_json(url)
    playlist = body.get("playlist") or {}
    tracks = playlist.get("tracks") or []
    if not tracks:
        raise RuntimeError(f"rank {rank_id} has no tracks")
    return tracks[:track_limit]


def fetch_playlist_cards(limit):
    query = urllib.parse.urlencode(
        {"cat": "全部", "order": "hot", "offset": 0, "limit": max(limit, 20), "total": "true"}
    )
    url = f"{NETEASE_PLAYLIST_LIST_API}?{query}"
    body = http_get_json(url)
    playlists = body.get("playlists") or []
    result = []
    for item in playlists[:limit]:
        playlist_id = item.get("id")
        if not playlist_id:
            continue
        result.append(
            {
                "playlist_id": str(playlist_id),
                "playlist_name": ((item.get("name") or "").strip() or "未命名歌单")[:256],
                "playlist_url": f"https://music.163.com/#/playlist?id={playlist_id}",
                "creator_user_id": str((item.get("creator") or {}).get("userId") or ""),
                "play_count": int(item.get("playCount") or 0),
                "collect_count": int(item.get("subscribedCount") or 0),
                "track_count": int(item.get("trackCount") or 0),
                "description": (item.get("description") or "")[:3000] or None,
            }
        )
    return result


def fetch_playlist_tracks(playlist_id, track_limit):
    query = urllib.parse.urlencode({"id": playlist_id, "n": max(track_limit, 100), "s": 0})
    url = f"{NETEASE_PLAYLIST_DETAIL_API}?{query}"
    body = http_get_json(url)
    playlist = body.get("playlist") or {}
    tracks = playlist.get("tracks") or []
    return tracks[:track_limit], playlist


def fetch_top_artists(limit):
    query = urllib.parse.urlencode({"offset": 0, "limit": max(limit, 20)})
    url = f"{NETEASE_ARTIST_TOP_API}?{query}"
    body = http_get_json(url)
    artists = body.get("artists") or []
    result = []
    for item in artists[:limit]:
        artist_id = item.get("id")
        artist_name = (item.get("name") or "").strip()
        if not artist_id or not artist_name:
            continue
        result.append(
            {
                "artist_id": str(artist_id),
                "artist_name": artist_name[:128],
                "artist_url": f"https://music.163.com/#/artist?id={artist_id}",
            }
        )
    return result


def fetch_artist_tracks(artist_id, track_limit):
    query = urllib.parse.urlencode({"id": artist_id})
    url = f"{NETEASE_ARTIST_TOP_SONG_API}?{query}"
    body = http_get_json(url)
    tracks = body.get("songs") or []
    return tracks[:track_limit]


def fetch_song_lyric(song_id):
    query = urllib.parse.urlencode({"id": song_id, "lv": -1, "kv": -1, "tv": -1})
    url = f"{NETEASE_LYRIC_API}?{query}"
    try:
        body = http_get_json(url)
    except Exception:
        return None
    lrc = body.get("lrc") or {}
    lyric = lrc.get("lyric")
    if not lyric:
        return None
    return lyric.strip()


def load_song_ids_with_lyrics(conn, song_ids):
    if not song_ids:
        return set()
    placeholders = ",".join(["%s"] * len(song_ids))
    sql = f"""
        SELECT song_id
        FROM songs
        WHERE song_id IN ({placeholders})
          AND lyric_text IS NOT NULL
          AND lyric_text <> ''
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, tuple(song_ids))
        rows = cursor.fetchall()
    return {str(row[0]) for row in rows if row and row[0]}


def normalize_track(track, default_artist=None):
    song_id = str(track.get("id"))
    song_name = (track.get("name") or "").strip()[:256] or "unknown"
    duration_ms = track.get("dt")
    publish_time_ms = track.get("publishTime")
    album = track.get("al") or {}
    album_name = (album.get("name") or "").strip()[:256] or None
    artists = []
    if default_artist:
        artists.append(default_artist)
    else:
        artist_items = track.get("ar") or []
        for item in artist_items:
            artist_id = item.get("id")
            artist_name = (item.get("name") or "").strip()
            if not artist_id or not artist_name:
                continue
            artists.append(
                {
                    "artist_id": str(artist_id),
                    "artist_name": artist_name[:128],
                    "artist_url": f"https://music.163.com/#/artist?id={artist_id}",
                }
            )
    if not artists:
        artists.append(
            {
                "artist_id": "0",
                "artist_name": "未知歌手",
                "artist_url": None,
            }
        )
    return {
        "song": {
            "song_id": song_id,
            "song_name": song_name,
            "duration_sec": int(duration_ms / 1000) if isinstance(duration_ms, int) else None,
            "album_name": album_name,
            "lyric_text": None,
            "song_url": f"https://music.163.com/#/song?id={song_id}",
            "source_platform": "netease",
            "publish_time": datetime.fromtimestamp(publish_time_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(publish_time_ms, int) and publish_time_ms > 0
            else None,
        },
        "artists": artists,
        "score": track.get("pop"),
    }


def upsert_rank(conn, rec):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ranks(rank_id, rank_name, rank_order, rank_url, rank_category, update_cycle)
            VALUES(%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              rank_name=VALUES(rank_name),
              rank_order=VALUES(rank_order),
              rank_url=VALUES(rank_url),
              rank_category=VALUES(rank_category),
              update_cycle=VALUES(update_cycle)
            """,
            (
                rec["rank_id"],
                rec["rank_name"],
                rec["rank_order"],
                rec["rank_url"],
                rec["rank_category"],
                rec["update_cycle"],
            ),
        )


def upsert_playlist(conn, rec):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO playlists(playlist_id, playlist_name, playlist_url, creator_user_id, play_count, collect_count, track_count, description)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              playlist_name=VALUES(playlist_name),
              playlist_url=VALUES(playlist_url),
              creator_user_id=VALUES(creator_user_id),
              play_count=VALUES(play_count),
              collect_count=VALUES(collect_count),
              track_count=VALUES(track_count),
              description=VALUES(description)
            """,
            (
                rec["playlist_id"],
                rec["playlist_name"],
                rec["playlist_url"],
                rec["creator_user_id"] or None,
                rec["play_count"],
                rec["collect_count"],
                rec["track_count"],
                rec["description"],
            ),
        )


def upsert_song(conn, rec):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO songs(song_id, song_name, duration_sec, album_name, lyric_text, song_url, source_platform, publish_time)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              song_name=VALUES(song_name),
              duration_sec=VALUES(duration_sec),
              album_name=VALUES(album_name),
              lyric_text=COALESCE(VALUES(lyric_text), lyric_text),
              song_url=VALUES(song_url),
              source_platform=VALUES(source_platform),
              publish_time=VALUES(publish_time)
            """,
            (
                rec["song_id"],
                rec["song_name"],
                rec["duration_sec"],
                rec["album_name"],
                rec["lyric_text"],
                rec["song_url"],
                rec["source_platform"],
                rec["publish_time"],
            ),
        )


def upsert_artist(conn, rec):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO artists(artist_id, artist_name, artist_url, region, style)
            VALUES(%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              artist_name=VALUES(artist_name),
              artist_url=VALUES(artist_url)
            """,
            (rec["artist_id"], rec["artist_name"], rec["artist_url"], None, None),
        )


def upsert_artist_song(conn, artist_id, song_id, crawl_time):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO artist_songs(artist_id, song_id, role_type, crawl_time)
            VALUES(%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              role_type=VALUES(role_type),
              crawl_time=VALUES(crawl_time)
            """,
            (artist_id, song_id, "主唱", crawl_time),
        )


def upsert_rank_song(conn, rank_id, song_id, rank_position, score, crawl_time):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO rank_songs(rank_id, song_id, rank_position, score, crawl_time)
            VALUES(%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              rank_position=VALUES(rank_position),
              score=VALUES(score)
            """,
            (rank_id, song_id, rank_position, score, crawl_time),
        )


def upsert_playlist_song(conn, playlist_id, song_id, position, crawl_time):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO playlist_songs(playlist_id, song_id, position, added_at, crawl_time)
            VALUES(%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              position=VALUES(position),
              added_at=VALUES(added_at),
              crawl_time=VALUES(crawl_time)
            """,
            (playlist_id, song_id, position, crawl_time, crawl_time),
        )


def upsert_legacy_artist(conn, artist):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO dim_artist(artist_id, artist_name, region, style)
            VALUES(%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              artist_name=VALUES(artist_name)
            """,
            (artist["artist_id"], artist["artist_name"], None, None),
        )


def upsert_legacy_song(conn, song, artist_id):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO dim_song(song_id, song_name, artist_id, source_platform, platform_song_id, album_name, lyric_text, duration_sec, tags, publish_time)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              song_name=VALUES(song_name),
              artist_id=VALUES(artist_id),
              source_platform=VALUES(source_platform),
              platform_song_id=VALUES(platform_song_id),
              album_name=VALUES(album_name),
              lyric_text=COALESCE(VALUES(lyric_text), lyric_text),
              duration_sec=VALUES(duration_sec),
              publish_time=VALUES(publish_time)
            """,
            (
                song["song_id"],
                song["song_name"],
                artist_id,
                song["source_platform"],
                song["song_id"],
                song["album_name"],
                song["lyric_text"],
                song["duration_sec"],
                None,
                song["publish_time"],
            ),
        )


def parse_ai_tag_content(content):
    if not content:
        return []
    text = content.strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text.replace("json", "", 1).strip()
    tags = []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            tags = [str(item).strip() for item in parsed]
    except json.JSONDecodeError:
        raw = text.replace("，", ",").replace("、", ",").replace("|", ",").replace("/", ",")
        tags = [part.strip() for part in raw.split(",")]
    deduped = []
    for tag in tags:
        cleaned = tag.strip().strip("[]\"'`")
        if not cleaned:
            continue
        if len(cleaned) > 16:
            cleaned = cleaned[:16]
        if cleaned not in deduped:
            deduped.append(cleaned)
    return deduped[:8]


def fallback_tags(song_name, lyric):
    text = f"{song_name or ''} {lyric or ''}".lower()
    tags = []
    if "爱" in text:
        tags.append("爱情")
    if "夜" in text or "月" in text:
        tags.append("夜晚")
    if "梦" in text:
        tags.append("梦想")
    if "孤" in text or "伤" in text:
        tags.append("伤感")
    if "风" in text or "海" in text:
        tags.append("治愈")
    if not tags:
        tags = ["流行", "华语"]
    return list(dict.fromkeys(tags))[:8]


def request_ai_tags(song_name, lyric):
    if not AI_API_KEY:
        return []
    lyric_text = (lyric or "").strip()
    if len(lyric_text) > 4000:
        lyric_text = lyric_text[:4000]
    payload = {
        "model": AI_MODEL,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": "你是音乐标签生成助手，只输出JSON数组，最多8个中文标签，不要解释。"},
            {"role": "user", "content": f"歌曲名：{song_name}\n歌词：{lyric_text}\n请输出标签JSON数组。"},
        ],
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{AI_BASE_URL.rstrip('/')}/chat/completions",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {AI_API_KEY}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=AI_TIMEOUT_SEC) as resp:
            payload = resp.read().decode("utf-8")
        data = json.loads(payload)
        content = (
            (((data.get("choices") or [{}])[0]).get("message") or {}).get("content")
            if isinstance(data, dict)
            else None
        )
        return parse_ai_tag_content(content)
    except Exception:
        return []


def build_song_tags(song):
    song_name = song.get("song_name") or ""
    lyric = song.get("lyric_text") or ""
    tags = request_ai_tags(song_name, lyric)
    if tags:
        return tags
    return fallback_tags(song_name, lyric)


def build_song_tags_concurrently(songs):
    if not songs:
        return {}
    workers = min(len(songs), AI_TAG_CONCURRENCY)
    result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(build_song_tags, song): song["song_id"] for song in songs}
        for future in concurrent.futures.as_completed(future_map):
            song_id = future_map[future]
            try:
                tags = future.result()
            except Exception:
                tags = []
            result[song_id] = tags
    return result


def upsert_song_tags(conn, song_id, tags):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM song_tags WHERE song_id = %s AND source_model = %s", (song_id, AI_MODEL))
        for idx, tag in enumerate(tags, start=1):
            score = max(0.01, 1.0 - (idx - 1) * 0.12)
            cursor.execute(
                """
                INSERT INTO song_tags(song_id, tag_name, score, source_model, source_version, updated_at)
                VALUES(%s,%s,%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE
                  score=VALUES(score),
                  source_version=VALUES(source_version),
                  updated_at=VALUES(updated_at)
                """,
                (song_id, tag, score, AI_MODEL, "v1"),
            )


def persist_new_songs(conn, new_songs, song_artist_map):
    if not new_songs:
        return 0
    tag_map = build_song_tags_concurrently(new_songs)
    for song in new_songs:
        song_id = song["song_id"]
        upsert_song(conn, song)
        upsert_song_tags(conn, song_id, tag_map.get(song_id) or fallback_tags(song["song_name"], song.get("lyric_text")))
        upsert_legacy_song(conn, song, song_artist_map[song_id])
    return len(new_songs)


def collect(top_limit, track_limit, playlist_limit, artist_limit, dry_run, refresh_all_lyrics):
    start_time = time.time()
    conn = None
    summary = {
        "ranks": 0,
        "playlists": 0,
        "top_artists": 0,
        "songs": 0,
        "artists": 0,
        "rank_song_rows": 0,
        "playlist_song_rows": 0,
        "artist_song_rows": 0,
    }
    crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    song_seen = set()
    artist_seen = set()
    try:
        toplists = fetch_toplists(top_limit)
        playlist_cards = fetch_playlist_cards(playlist_limit)
        top_artists = fetch_top_artists(artist_limit)
        summary["ranks"] = len(toplists)
        summary["playlists"] = len(playlist_cards)
        summary["top_artists"] = len(top_artists)
        if not dry_run:
            conn = mysql_conn()
            ensure_required_tables(conn)
        for rank in toplists:
            rank_id = rank["rank_id"]
            tracks = fetch_rank_tracks(rank_id, track_limit)
            rank_song_ids = [str(track.get("id")) for track in tracks if track.get("id")]
            new_songs = []
            new_song_artist_map = {}
            existing_lyric_song_ids = set()
            if not dry_run and not refresh_all_lyrics:
                existing_lyric_song_ids = load_song_ids_with_lyrics(conn, rank_song_ids)
            if not dry_run:
                upsert_rank(conn, rank)
            for idx, track in enumerate(tracks, start=1):
                if not track.get("id"):
                    continue
                normalized = normalize_track(track)
                song = normalized["song"]
                if song["song_id"] not in song_seen:
                    song_seen.add(song["song_id"])
                    summary["songs"] += 1
                    if refresh_all_lyrics or song["song_id"] not in existing_lyric_song_ids:
                        song["lyric_text"] = fetch_song_lyric(song["song_id"])
                    new_songs.append(song)
                artists = normalized["artists"]
                if song["song_id"] in song_seen and song["song_id"] not in new_song_artist_map:
                    new_song_artist_map[song["song_id"]] = artists[0]["artist_id"]
                for artist in artists:
                    if artist["artist_id"] not in artist_seen:
                        artist_seen.add(artist["artist_id"])
                        summary["artists"] += 1
                        if not dry_run:
                            upsert_artist(conn, artist)
                            upsert_legacy_artist(conn, artist)
                    if not dry_run:
                        upsert_artist_song(conn, artist["artist_id"], song["song_id"], crawl_time)
                if not dry_run:
                    upsert_rank_song(conn, rank_id, song["song_id"], idx, normalized["score"], crawl_time)
                summary["rank_song_rows"] += 1
            if not dry_run and new_songs:
                persist_new_songs(conn, new_songs, new_song_artist_map)
        for card in playlist_cards:
            playlist_id = card["playlist_id"]
            tracks, detail = fetch_playlist_tracks(playlist_id, track_limit)
            card["track_count"] = int(detail.get("trackCount") or card["track_count"] or len(tracks))
            card["play_count"] = int(detail.get("playCount") or card["play_count"] or 0)
            card["collect_count"] = int(detail.get("subscribedCount") or card["collect_count"] or 0)
            card["description"] = (detail.get("description") or card["description"] or "")[:3000] or None
            rank_song_ids = [str(track.get("id")) for track in tracks if track.get("id")]
            existing_lyric_song_ids = set()
            if not dry_run and not refresh_all_lyrics:
                existing_lyric_song_ids = load_song_ids_with_lyrics(conn, rank_song_ids)
            new_songs = []
            new_song_artist_map = {}
            if not dry_run:
                upsert_playlist(conn, card)
            for idx, track in enumerate(tracks, start=1):
                if not track.get("id"):
                    continue
                normalized = normalize_track(track)
                song = normalized["song"]
                if song["song_id"] not in song_seen:
                    song_seen.add(song["song_id"])
                    summary["songs"] += 1
                    if refresh_all_lyrics or song["song_id"] not in existing_lyric_song_ids:
                        song["lyric_text"] = fetch_song_lyric(song["song_id"])
                    new_songs.append(song)
                artists = normalized["artists"]
                if song["song_id"] not in new_song_artist_map:
                    new_song_artist_map[song["song_id"]] = artists[0]["artist_id"]
                for artist in artists:
                    if artist["artist_id"] not in artist_seen:
                        artist_seen.add(artist["artist_id"])
                        summary["artists"] += 1
                        if not dry_run:
                            upsert_artist(conn, artist)
                            upsert_legacy_artist(conn, artist)
                    if not dry_run:
                        upsert_artist_song(conn, artist["artist_id"], song["song_id"], crawl_time)
                if not dry_run:
                    upsert_playlist_song(conn, playlist_id, song["song_id"], idx, crawl_time)
                summary["playlist_song_rows"] += 1
            if not dry_run and new_songs:
                persist_new_songs(conn, new_songs, new_song_artist_map)
        for artist in top_artists:
            artist_id = artist["artist_id"]
            tracks = fetch_artist_tracks(artist_id, track_limit)
            track_song_ids = [str(track.get("id")) for track in tracks if track.get("id")]
            existing_lyric_song_ids = set()
            if not dry_run and not refresh_all_lyrics:
                existing_lyric_song_ids = load_song_ids_with_lyrics(conn, track_song_ids)
            new_songs = []
            new_song_artist_map = {}
            if artist_id not in artist_seen:
                artist_seen.add(artist_id)
                summary["artists"] += 1
            if not dry_run:
                upsert_artist(conn, artist)
                upsert_legacy_artist(conn, artist)
            for track in tracks:
                if not track.get("id"):
                    continue
                normalized = normalize_track(track, default_artist=artist)
                song = normalized["song"]
                if song["song_id"] not in song_seen:
                    song_seen.add(song["song_id"])
                    summary["songs"] += 1
                    if refresh_all_lyrics or song["song_id"] not in existing_lyric_song_ids:
                        song["lyric_text"] = fetch_song_lyric(song["song_id"])
                    new_songs.append(song)
                new_song_artist_map[song["song_id"]] = artist_id
                if not dry_run:
                    upsert_artist_song(conn, artist_id, song["song_id"], crawl_time)
                summary["artist_song_rows"] += 1
            if not dry_run and new_songs:
                persist_new_songs(conn, new_songs, new_song_artist_map)
        end_time = time.time()
        message = (
            f"netease collected ranks={summary['ranks']}, playlists={summary['playlists']}, "
            f"top_artists={summary['top_artists']}, songs={summary['songs']}, artists={summary['artists']}, "
            f"rank_song_rows={summary['rank_song_rows']}, playlist_song_rows={summary['playlist_song_rows']}, "
            f"artist_song_rows={summary['artist_song_rows']}"
        )
        if not dry_run and conn:
            write_task_log(conn, "success", message, start_time, end_time)
        print(message)
        return summary
    except Exception as exc:
        end_time = time.time()
        if not dry_run and conn:
            write_task_log(conn, "failed", f"netease collect failed: {str(exc)[:900]}", start_time, end_time)
        raise
    finally:
        if conn:
            conn.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-limit", type=int, default=10)
    parser.add_argument("--playlist-limit", type=int, default=20)
    parser.add_argument("--artist-limit", type=int, default=20)
    parser.add_argument("--track-limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--refresh-all-lyrics", action="store_true")
    return parser.parse_args()


def run():
    args = parse_args()
    collect(
        args.top_limit,
        args.track_limit,
        args.playlist_limit,
        args.artist_limit,
        args.dry_run,
        args.refresh_all_lyrics,
    )


if __name__ == "__main__":
    run()
