import json
import os
import time
import urllib.request
from datetime import datetime

import pymysql


MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "music_system")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
ITUNES_RSS_URL = os.getenv(
    "ITUNES_RSS_URL",
    "https://rss.applemarketingtools.com/api/v2/us/music/most-played/100/songs.json",
)


def mysql_conn():
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
                "itunes_top_songs_collector",
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


def ensure_schema(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'dim_song'
            """
            ,
            (MYSQL_DB,),
        )
        existing = {row[0] for row in cursor.fetchall()}
        if "source_platform" not in existing:
            cursor.execute("ALTER TABLE dim_song ADD COLUMN source_platform VARCHAR(32) DEFAULT 'manual'")
        if "platform_song_id" not in existing:
            cursor.execute("ALTER TABLE dim_song ADD COLUMN platform_song_id VARCHAR(128)")
        if "album_name" not in existing:
            cursor.execute("ALTER TABLE dim_song ADD COLUMN album_name VARCHAR(256)")


def fetch_itunes_data():
    with urllib.request.urlopen(ITUNES_RSS_URL, timeout=20) as response:
        payload = response.read().decode("utf-8")
    body = json.loads(payload)
    feed = body.get("feed", {})
    return feed.get("results", [])


def normalize_record(item):
    track_id = item.get("id")
    artist_name = item.get("artistName", "").strip() or "Unknown Artist"
    song_name = item.get("name", "").strip() or "Unknown Song"
    release_date = item.get("releaseDate")
    artwork = item.get("artworkUrl100")
    genres = item.get("genres", [])
    tags = ",".join([g.get("name", "") for g in genres if g.get("name")])

    album_name = None
    if item.get("albumName"):
        album_name = item.get("albumName")

    artist_url = item.get("artistUrl", "")
    artist_key = artist_url.rsplit("/", 1)[-1] if artist_url else artist_name
    artist_key = "".join(ch for ch in artist_key if ch.isalnum() or ch in ["_", "-"])
    if not artist_key:
        artist_key = str(abs(hash(artist_name)))

    return {
        "artist_id": f"ITUNES_ARTIST_{artist_key[:40]}",
        "artist_name": artist_name[:128],
        "song_id": f"ITUNES_SONG_{track_id}",
        "platform_song_id": str(track_id),
        "song_name": song_name[:256],
        "publish_time": f"{release_date} 00:00:00" if release_date else None,
        "duration_sec": None,
        "tags": tags[:512] if tags else None,
        "album_name": album_name[:256] if album_name else None,
        "region": "US",
        "style": tags.split(",")[0][:128] if tags else None,
        "source_platform": "itunes",
        "artwork_url": artwork,
    }


def upsert_artist(conn, rec):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO dim_artist(artist_id, artist_name, region, style)
            VALUES(%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              artist_name=VALUES(artist_name),
              region=VALUES(region),
              style=VALUES(style)
            """,
            (rec["artist_id"], rec["artist_name"], rec["region"], rec["style"]),
        )


def upsert_song(conn, rec):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO dim_song(song_id, song_name, artist_id, source_platform, platform_song_id, album_name, duration_sec, tags, publish_time)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              song_name=VALUES(song_name),
              artist_id=VALUES(artist_id),
              source_platform=VALUES(source_platform),
              platform_song_id=VALUES(platform_song_id),
              album_name=VALUES(album_name),
              duration_sec=VALUES(duration_sec),
              tags=VALUES(tags),
              publish_time=VALUES(publish_time)
            """,
            (
                rec["song_id"],
                rec["song_name"],
                rec["artist_id"],
                rec["source_platform"],
                rec["platform_song_id"],
                rec["album_name"],
                rec["duration_sec"],
                rec["tags"],
                rec["publish_time"],
            ),
        )


def run():
    start_time = time.time()
    conn = mysql_conn()
    try:
        ensure_schema(conn)
        rows = fetch_itunes_data()
        inserted = 0
        for item in rows:
            if not item.get("id"):
                continue
            rec = normalize_record(item)
            upsert_artist(conn, rec)
            upsert_song(conn, rec)
            inserted += 1
        end_time = time.time()
        write_task_log(conn, "success", f"itunes collected {inserted} songs", start_time, end_time)
        print(f"itunes collected {inserted} songs")
    except Exception as exc:
        end_time = time.time()
        write_task_log(conn, "failed", f"itunes collect failed: {str(exc)[:900]}", start_time, end_time)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
