import json
import os
import random
import threading
import time
from collections import defaultdict
from datetime import datetime

import pymysql
from kafka import KafkaConsumer, KafkaProducer


MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "music_system")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "music_play_event")

SONG_IDS = ["S001", "S002", "S003", "S004", "S005"]


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


def wait_dependencies():
    while True:
        try:
            conn = mysql_conn()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            conn.close()
            break
        except Exception:
            time.sleep(3)

    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            producer.send(KAFKA_TOPIC, {"ping": 1}).get(timeout=10)
            producer.close()
            break
        except Exception:
            time.sleep(3)


def create_task_log(conn, status, message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sys_task_log(task_name, task_type, status, retry_count, start_time, end_time, duration_ms, message, trace_id)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                "streaming_rank_job",
                "stream",
                status,
                0,
                now,
                now,
                0,
                message,
                f"trace-{int(time.time() * 1000)}",
            ),
        )


def produce_events():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    while True:
        event = {
            "user_id": f"U{random.randint(1, 100):03d}",
            "song_id": random.choice(SONG_IDS),
            "play_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "play_count": 1,
            "like_count": random.randint(0, 1),
            "comment_count": random.randint(0, 1),
            "source_platform": "demo",
        }
        producer.send(KAFKA_TOPIC, event)
        producer.flush()
        time.sleep(1)


def persist_event(conn, event):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO fact_play_event(user_id, song_id, play_time, play_count, like_count, comment_count, source_platform)
            VALUES(%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                event["user_id"],
                event["song_id"],
                event["play_time"],
                event["play_count"],
                event["like_count"],
                event["comment_count"],
                event["source_platform"],
            ),
        )


def write_snapshot(conn, rank_rows):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM fact_rank_snapshot WHERE rank_type = 'minute'")
        for i, row in enumerate(rank_rows, start=1):
            cursor.execute(
                """
                INSERT INTO fact_rank_snapshot(rank_time, rank_type, item_id, item_type, score, rank_position, version)
                VALUES(%s,'minute',%s,'song',%s,%s,'mvp-stream')
                """,
                (now, row["song_id"], row["score"], i),
            )


def run_stream():
    conn = mysql_conn()
    create_task_log(conn, "running", "streaming job started")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="music-mvp-stream-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    window_start = time.time()
    agg = defaultdict(lambda: {"play_count": 0, "like_count": 0, "comment_count": 0})

    for msg in consumer:
        event = msg.value
        if "song_id" not in event:
            continue

        persist_event(conn, event)
        song_id = event["song_id"]
        agg[song_id]["play_count"] += int(event.get("play_count", 1))
        agg[song_id]["like_count"] += int(event.get("like_count", 0))
        agg[song_id]["comment_count"] += int(event.get("comment_count", 0))

        if time.time() - window_start >= 15:
            rows = []
            for sid, stats in agg.items():
                score = (
                    stats["play_count"] * 0.5
                    + stats["like_count"] * 0.25
                    + stats["comment_count"] * 0.15
                )
                rows.append({"song_id": sid, "score": round(score, 3)})
            rows.sort(key=lambda x: x["score"], reverse=True)
            write_snapshot(conn, rows[:20])
            create_task_log(conn, "success", f"snapshot updated with {len(rows)} songs")
            agg = defaultdict(lambda: {"play_count": 0, "like_count": 0, "comment_count": 0})
            window_start = time.time()


def main():
    wait_dependencies()
    producer_thread = threading.Thread(target=produce_events, daemon=True)
    producer_thread.start()
    run_stream()


if __name__ == "__main__":
    main()
