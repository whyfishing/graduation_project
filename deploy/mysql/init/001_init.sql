CREATE DATABASE IF NOT EXISTS music_system DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE music_system;
SET NAMES utf8mb4;

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
);

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
);

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
);

CREATE TABLE IF NOT EXISTS ranks (
  rank_id VARCHAR(32) PRIMARY KEY,
  rank_name VARCHAR(128) NOT NULL,
  rank_order INT NOT NULL DEFAULT 9999,
  rank_url VARCHAR(512),
  rank_category VARCHAR(64),
  update_cycle VARCHAR(32),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
  user_id VARCHAR(32) PRIMARY KEY,
  nickname VARCHAR(128),
  gender TINYINT,
  province VARCHAR(64),
  city VARCHAR(64),
  signature VARCHAR(512),
  user_url VARCHAR(512),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

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
);

CREATE TABLE IF NOT EXISTS rank_songs (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  rank_id VARCHAR(32) NOT NULL,
  song_id VARCHAR(32) NOT NULL,
  rank_position INT NOT NULL,
  score DOUBLE,
  crawl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_rank_song_time(rank_id, song_id, crawl_time),
  INDEX idx_rank_time_pos(rank_id, crawl_time, rank_position)
);

CREATE TABLE IF NOT EXISTS artist_songs (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  artist_id VARCHAR(32) NOT NULL,
  song_id VARCHAR(32) NOT NULL,
  role_type VARCHAR(32),
  crawl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_artist_song(artist_id, song_id)
);

CREATE TABLE IF NOT EXISTS user_recent_songs (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_id VARCHAR(32) NOT NULL,
  song_id VARCHAR(32) NOT NULL,
  played_at DATETIME,
  play_order INT,
  crawl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_user_song_time(user_id, song_id, crawl_time),
  INDEX idx_user_recent_crawl_time(crawl_time)
);

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
);

CREATE TABLE IF NOT EXISTS user_profiles (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_id VARCHAR(32) NOT NULL,
  profile_json JSON NOT NULL,
  profile_text TEXT,
  version VARCHAR(32) NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_user_profile_version(user_id, version),
  INDEX idx_user_profile_updated_at(updated_at)
);

CREATE TABLE IF NOT EXISTS playlist_profiles (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  playlist_id VARCHAR(32) NOT NULL,
  profile_json JSON NOT NULL,
  profile_text TEXT,
  version VARCHAR(32) NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_playlist_profile_version(playlist_id, version),
  INDEX idx_playlist_profile_updated_at(updated_at)
);

CREATE TABLE IF NOT EXISTS artist_profiles (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  artist_id VARCHAR(32) NOT NULL,
  profile_json JSON NOT NULL,
  profile_text TEXT,
  version VARCHAR(32) NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_artist_profile_version(artist_id, version),
  INDEX idx_artist_profile_updated_at(updated_at)
);

CREATE TABLE IF NOT EXISTS rank_profiles (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  rank_id VARCHAR(32) NOT NULL,
  profile_json JSON NOT NULL,
  profile_text TEXT,
  version VARCHAR(32) NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_rank_profile_version(rank_id, version),
  INDEX idx_rank_profile_updated_at(updated_at)
);

CREATE TABLE IF NOT EXISTS sys_config (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  config_key VARCHAR(128) NOT NULL,
  config_value TEXT NOT NULL,
  value_type VARCHAR(32) NOT NULL DEFAULT 'string',
  is_enabled TINYINT NOT NULL DEFAULT 1,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_config_key(config_key),
  INDEX idx_sys_config_enabled(is_enabled)
);

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
  source_platform VARCHAR(32) DEFAULT 'manual',
  platform_song_id VARCHAR(128),
  album_name VARCHAR(256),
  lyric_text LONGTEXT,
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

INSERT INTO artists (artist_id, artist_name, artist_url, region, style) VALUES
('A001', '周杰伦', 'https://music.163.com/#/artist?id=A001', '中国台湾', '流行'),
('A002', '陈奕迅', 'https://music.163.com/#/artist?id=A002', '中国香港', '流行'),
('A003', '邓紫棋', 'https://music.163.com/#/artist?id=A003', '中国香港', '流行')
ON DUPLICATE KEY UPDATE artist_name = VALUES(artist_name);

INSERT INTO songs (song_id, song_name, duration_sec, album_name, lyric_text, song_url, source_platform, publish_time) VALUES
('S001', '七里香', 299, NULL, NULL, 'https://music.163.com/#/song?id=S001', 'netease', '2004-08-03 00:00:00'),
('S002', '十年', 210, NULL, NULL, 'https://music.163.com/#/song?id=S002', 'netease', '2003-04-15 00:00:00'),
('S003', '光年之外', 235, NULL, NULL, 'https://music.163.com/#/song?id=S003', 'netease', '2016-12-30 00:00:00'),
('S004', '夜曲', 224, NULL, NULL, 'https://music.163.com/#/song?id=S004', 'netease', '2005-11-01 00:00:00'),
('S005', '孤勇者', 246, NULL, NULL, 'https://music.163.com/#/song?id=S005', 'netease', '2021-11-08 00:00:00')
ON DUPLICATE KEY UPDATE song_name = VALUES(song_name);

INSERT INTO artist_songs (artist_id, song_id, role_type, crawl_time) VALUES
('A001', 'S001', '主唱', NOW()),
('A002', 'S002', '主唱', NOW()),
('A003', 'S003', '主唱', NOW()),
('A001', 'S004', '主唱', NOW()),
('A002', 'S005', '主唱', NOW())
ON DUPLICATE KEY UPDATE role_type = VALUES(role_type);

INSERT INTO dim_artist (artist_id, artist_name, region, style) VALUES
('A001', '周杰伦', '中国台湾', '流行'),
('A002', '陈奕迅', '中国香港', '流行'),
('A003', '邓紫棋', '中国香港', '流行')
ON DUPLICATE KEY UPDATE artist_name = VALUES(artist_name);

INSERT INTO dim_song (song_id, song_name, artist_id, source_platform, platform_song_id, album_name, lyric_text, duration_sec, tags, publish_time) VALUES
('S001', '七里香', 'A001', 'manual', 'S001', NULL, NULL, 299, '华语,流行', '2004-08-03 00:00:00'),
('S002', '十年', 'A002', 'manual', 'S002', NULL, NULL, 210, '华语,抒情', '2003-04-15 00:00:00'),
('S003', '光年之外', 'A003', 'manual', 'S003', NULL, NULL, 235, '华语,流行', '2016-12-30 00:00:00'),
('S004', '夜曲', 'A001', 'manual', 'S004', NULL, NULL, 224, '华语,流行', '2005-11-01 00:00:00'),
('S005', '孤勇者', 'A002', 'manual', 'S005', NULL, NULL, 246, '华语,热歌', '2021-11-08 00:00:00')
ON DUPLICATE KEY UPDATE song_name = VALUES(song_name);
