package com.music.api.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DashboardService {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final String aiBaseUrl;
    private final String aiApiKey;
    private final String aiModel;

    public DashboardService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            @Value("${ai.dashscope-base-url:https://dashscope.aliyuncs.com/compatible-mode/v1}") String aiBaseUrl,
            @Value("${ai.dashscope-api-key:}") String aiApiKey,
            @Value("${ai.model:qwen-plus}") String aiModel
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newHttpClient();
        this.aiBaseUrl = aiBaseUrl;
        this.aiApiKey = aiApiKey;
        this.aiModel = aiModel;
    }

    public List<Map<String, Object>> getRankings(String rankType, int limit) {
        String sql = """
                SELECT rs.rank_position, rs.item_id AS song_id, s.song_name, a.artist_name, ROUND(rs.score, 2) AS score, rs.rank_time
                FROM fact_rank_snapshot rs
                LEFT JOIN dim_song s ON rs.item_id = s.song_id
                LEFT JOIN dim_artist a ON s.artist_id = a.artist_id
                WHERE rs.rank_type = ?
                  AND rs.rank_time = (
                    SELECT MAX(rank_time) FROM fact_rank_snapshot WHERE rank_type = ?
                  )
                ORDER BY rs.rank_position ASC
                LIMIT ?
                """;
        return jdbcTemplate.queryForList(sql, rankType, rankType, limit);
    }

    public List<Map<String, Object>> getRankRecommendations(int limit) {
        String sql = """
                SELECT r.rank_id, r.rank_name, r.rank_category, r.update_cycle, MAX(rs.crawl_time) AS latest_crawl_time, COUNT(rs.id) AS song_count
                FROM ranks r
                LEFT JOIN rank_songs rs ON rs.rank_id = r.rank_id
                GROUP BY r.rank_id, r.rank_name, r.rank_category, r.update_cycle
                ORDER BY latest_crawl_time DESC
                LIMIT ?
                """;
        return jdbcTemplate.queryForList(sql, limit);
    }

    public List<Map<String, Object>> getRankSongs(String rankId, int limit) {
        String sql = """
                SELECT rs.rank_position, s.song_id, s.song_name, a.artist_name, rs.score, rs.crawl_time
                FROM rank_songs rs
                JOIN songs s ON rs.song_id = s.song_id
                LEFT JOIN artist_songs ars ON ars.song_id = s.song_id
                LEFT JOIN artists a ON a.artist_id = ars.artist_id
                WHERE rs.rank_id = ?
                  AND rs.crawl_time = (SELECT MAX(crawl_time) FROM rank_songs WHERE rank_id = ?)
                ORDER BY rs.rank_position ASC
                LIMIT ?
                """;
        return jdbcTemplate.queryForList(sql, rankId, rankId, limit);
    }

    public List<Map<String, Object>> getPlaylistRecommendations(int limit) {
        String sql = """
                SELECT playlist_id, playlist_name, play_count, collect_count, track_count, updated_at
                FROM playlists
                ORDER BY updated_at DESC
                LIMIT ?
                """;
        return jdbcTemplate.queryForList(sql, limit);
    }

    public List<Map<String, Object>> getPlaylistSongs(String playlistId, int limit) {
        String sql = """
                SELECT ps.position, s.song_id, s.song_name, a.artist_name, ps.crawl_time
                FROM playlist_songs ps
                JOIN songs s ON s.song_id = ps.song_id
                LEFT JOIN artist_songs ars ON ars.song_id = s.song_id
                LEFT JOIN artists a ON a.artist_id = ars.artist_id
                WHERE ps.playlist_id = ?
                ORDER BY ps.position ASC, ps.id DESC
                LIMIT ?
                """;
        return jdbcTemplate.queryForList(sql, playlistId, limit);
    }

    public List<Map<String, Object>> getArtistRecommendations(int limit) {
        String sql = """
                SELECT a.artist_id, a.artist_name, COUNT(ars.song_id) AS song_count, MAX(ars.crawl_time) AS latest_crawl_time
                FROM artists a
                LEFT JOIN artist_songs ars ON a.artist_id = ars.artist_id
                GROUP BY a.artist_id, a.artist_name
                ORDER BY song_count DESC, latest_crawl_time DESC
                LIMIT ?
                """;
        return jdbcTemplate.queryForList(sql, limit);
    }

    public List<Map<String, Object>> getArtistSongs(String artistId, int limit) {
        String sql = """
                SELECT s.song_id, s.song_name, s.album_name, s.duration_sec, s.publish_time
                FROM artist_songs ars
                JOIN songs s ON s.song_id = ars.song_id
                WHERE ars.artist_id = ?
                ORDER BY ars.crawl_time DESC, s.publish_time DESC
                LIMIT ?
                """;
        return jdbcTemplate.queryForList(sql, artistId, limit);
    }

    public Map<String, Object> getSongAnalysis(String songId) {
        Map<String, Object> song = jdbcTemplate.queryForMap(
                """
                SELECT s.song_id, s.song_name, s.album_name, s.duration_sec, s.lyric_text, s.publish_time, s.song_url,
                       GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name SEPARATOR ' / ') AS artist_names
                FROM songs s
                LEFT JOIN artist_songs ars ON ars.song_id = s.song_id
                LEFT JOIN artists a ON a.artist_id = ars.artist_id
                WHERE s.song_id = ?
                GROUP BY s.song_id, s.song_name, s.album_name, s.duration_sec, s.lyric_text, s.publish_time, s.song_url
                """,
                songId
        );
        List<Map<String, Object>> tags = jdbcTemplate.queryForList(
                """
                SELECT tag_name, score, source_model, updated_at
                FROM song_tags
                WHERE song_id = ?
                ORDER BY score DESC, updated_at DESC
                LIMIT 12
                """,
                songId
        );
        List<Map<String, Object>> similarSongs = jdbcTemplate.queryForList(
                """
                SELECT s.song_id, s.song_name, ROUND(SUM(st.score), 4) AS similarity_score
                FROM song_tags base
                JOIN song_tags st ON st.tag_name = base.tag_name AND st.song_id <> base.song_id
                JOIN songs s ON s.song_id = st.song_id
                WHERE base.song_id = ?
                GROUP BY s.song_id, s.song_name
                ORDER BY similarity_score DESC, s.song_name ASC
                LIMIT 8
                """,
                songId
        );
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("song", song);
        result.put("tags", tags);
        result.put("similarSongs", similarSongs);
        result.put("lyricLength", song.get("lyric_text") == null ? 0 : song.get("lyric_text").toString().length());
        return result;
    }

    public Map<String, Object> generateSongTags(String songId) {
        Map<String, Object> song = jdbcTemplate.queryForMap("SELECT song_id, song_name, lyric_text FROM songs WHERE song_id = ?", songId);
        String lyric = song.get("lyric_text") == null ? "" : song.get("lyric_text").toString().trim();
        List<String> tags = lyric.isBlank() ? List.of() : requestAiTags(song.get("song_name").toString(), lyric);
        if (tags.isEmpty()) {
            tags = fallbackTags(song.get("song_name").toString(), lyric);
        }
        jdbcTemplate.update("DELETE FROM song_tags WHERE song_id = ? AND source_model = ?", songId, aiModel);
        int rank = 0;
        for (String tag : tags) {
            rank++;
            double score = Math.max(0.01, 1.0 - (rank - 1) * 0.12);
            jdbcTemplate.update(
                    """
                    INSERT INTO song_tags(song_id, tag_name, score, source_model, source_version, updated_at)
                    VALUES(?,?,?,?,?,NOW())
                    ON DUPLICATE KEY UPDATE score=VALUES(score), source_version=VALUES(source_version), updated_at=VALUES(updated_at)
                    """,
                    songId,
                    tag,
                    score,
                    aiModel,
                    "v1"
            );
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("songId", songId);
        result.put("songName", song.get("song_name"));
        result.put("generatedTags", tags);
        result.put("tagCount", tags.size());
        result.put("aiEnabled", aiApiKey != null && !aiApiKey.isBlank());
        return result;
    }

    private List<String> requestAiTags(String songName, String lyric) {
        if (aiApiKey == null || aiApiKey.isBlank()) {
            return List.of();
        }
        String text = lyric.length() > 4000 ? lyric.substring(0, 4000) : lyric;
        try {
            Map<String, Object> payload = Map.of(
                    "model", aiModel,
                    "temperature", 0.2,
                    "messages", List.of(
                            Map.of("role", "system", "content", "你是音乐标签生成助手，只输出JSON数组，最多8个中文标签，不要解释。"),
                            Map.of("role", "user", "content", "歌曲名：" + songName + "\n歌词：" + text + "\n请输出标签JSON数组。")
                    )
            );
            String body = objectMapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(aiBaseUrl + "/chat/completions"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + aiApiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                return List.of();
            }
            JsonNode root = objectMapper.readTree(response.body());
            JsonNode contentNode = root.path("choices").path(0).path("message").path("content");
            if (contentNode.isMissingNode() || contentNode.isNull()) {
                return List.of();
            }
            return parseTags(contentNode.asText());
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private List<String> parseTags(String content) {
        String text = content == null ? "" : content.trim();
        if (text.isBlank()) {
            return List.of();
        }
        try {
            String jsonText = text;
            int left = jsonText.indexOf('[');
            int right = jsonText.lastIndexOf(']');
            if (left >= 0 && right > left) {
                jsonText = jsonText.substring(left, right + 1);
            }
            List<String> tags = objectMapper.readValue(jsonText, new TypeReference<List<String>>() {
            });
            return normalizeTags(tags);
        } catch (Exception ignored) {
            String[] items = text.replace("，", ",").split(",");
            return normalizeTags(List.of(items));
        }
    }

    private List<String> normalizeTags(List<String> tags) {
        return tags.stream()
                .map(tag -> tag == null ? "" : tag.trim())
                .filter(tag -> !tag.isBlank())
                .map(tag -> tag.length() > 20 ? tag.substring(0, 20) : tag)
                .distinct()
                .limit(8)
                .collect(Collectors.toList());
    }

    private List<String> fallbackTags(String songName, String lyric) {
        String text = (songName + " " + lyric).toLowerCase();
        List<String> tags = new java.util.ArrayList<>();
        if (text.contains("爱")) {
            tags.add("爱情");
        }
        if (text.contains("夜") || text.contains("月")) {
            tags.add("夜晚");
        }
        if (text.contains("梦")) {
            tags.add("梦想");
        }
        if (text.contains("孤") || text.contains("伤")) {
            tags.add("伤感");
        }
        if (text.contains("风") || text.contains("海")) {
            tags.add("治愈");
        }
        if (tags.isEmpty()) {
            tags = List.of("流行", "华语");
        }
        return tags.stream().distinct().limit(8).collect(Collectors.toList());
    }

    public Map<String, Object> getOverview() {
        Integer songCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM songs", Integer.class);
        Integer artistCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM artists", Integer.class);
        Integer playlistCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM playlists", Integer.class);
        Integer rankCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM ranks", Integer.class);
        Integer eventCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM fact_play_event", Integer.class);
        Integer rankSongCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM rank_songs", Integer.class);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("songCount", songCount == null ? 0 : songCount);
        result.put("artistCount", artistCount == null ? 0 : artistCount);
        result.put("playlistCount", playlistCount == null ? 0 : playlistCount);
        result.put("rankCount", rankCount == null ? 0 : rankCount);
        result.put("playEventCount", eventCount == null ? 0 : eventCount);
        result.put("rankSongCount", rankSongCount == null ? 0 : rankSongCount);
        return result;
    }

    public List<Map<String, Object>> getTaskLogs() {
        String sql = """
                SELECT id, task_name, task_type, status, retry_count, start_time, end_time, duration_ms, message, trace_id
                FROM sys_task_log
                ORDER BY id DESC
                LIMIT 20
                """;
        return jdbcTemplate.queryForList(sql);
    }
}
