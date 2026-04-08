CREATE DATABASE IF NOT EXISTS music_system DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE music_system;
SET NAMES utf8mb4;

CREATE TABLE IF NOT EXISTS dim_artist (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  artist_id VARCHAR(64) NOT NULL UNIQUE,
  artist_name VARCHAR(128) NOT NULL,
  region VARCHAR(64),
  style VARCHAR(128),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_song (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  song_id VARCHAR(64) NOT NULL UNIQUE,
  song_name VARCHAR(256) NOT NULL,
  artist_id VARCHAR(64) NOT NULL,
  duration_sec INT,
  tags VARCHAR(512),
  publish_time DATETIME,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_play_event (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_id VARCHAR(64),
  song_id VARCHAR(64) NOT NULL,
  play_time DATETIME NOT NULL,
  play_count INT NOT NULL DEFAULT 1,
  like_count INT NOT NULL DEFAULT 0,
  comment_count INT NOT NULL DEFAULT 0,
  source_platform VARCHAR(32) DEFAULT 'demo',
  INDEX idx_song_time(song_id, play_time)
);

CREATE TABLE IF NOT EXISTS fact_rank_snapshot (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  rank_time DATETIME NOT NULL,
  rank_type VARCHAR(16) NOT NULL,
  item_id VARCHAR(64) NOT NULL,
  item_type VARCHAR(16) NOT NULL DEFAULT 'song',
  score DOUBLE NOT NULL,
  rank_position INT NOT NULL,
  version VARCHAR(32) NOT NULL DEFAULT 'mvp',
  INDEX idx_rank_time_type(rank_time, rank_type),
  INDEX idx_rank_type_position(rank_type, rank_position)
);

CREATE TABLE IF NOT EXISTS sys_task_log (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  task_name VARCHAR(128) NOT NULL,
  task_type VARCHAR(32) NOT NULL,
  status VARCHAR(16) NOT NULL,
  retry_count INT NOT NULL DEFAULT 0,
  start_time DATETIME NOT NULL,
  end_time DATETIME,
  duration_ms BIGINT,
  message VARCHAR(1024),
  trace_id VARCHAR(64),
  INDEX idx_task_type(task_type),
  INDEX idx_task_status(status),
  INDEX idx_start_time(start_time)
);

INSERT INTO dim_artist (artist_id, artist_name, region, style) VALUES
('A001', '周杰伦', '中国台湾', '流行'),
('A002', '陈奕迅', '中国香港', '流行'),
('A003', '邓紫棋', '中国香港', '流行')
ON DUPLICATE KEY UPDATE artist_name = VALUES(artist_name);

INSERT INTO dim_song (song_id, song_name, artist_id, duration_sec, tags, publish_time) VALUES
('S001', '七里香', 'A001', 299, '华语,流行', '2004-08-03 00:00:00'),
('S002', '十年', 'A002', 210, '华语,抒情', '2003-04-15 00:00:00'),
('S003', '光年之外', 'A003', 235, '华语,流行', '2016-12-30 00:00:00'),
('S004', '夜曲', 'A001', 224, '华语,流行', '2005-11-01 00:00:00'),
('S005', '孤勇者', 'A002', 246, '华语,热歌', '2021-11-08 00:00:00')
ON DUPLICATE KEY UPDATE song_name = VALUES(song_name);
