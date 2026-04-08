package com.music.api.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class DashboardService {
    private final JdbcTemplate jdbcTemplate;

    public DashboardService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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

    public Map<String, Object> getOverview() {
        Integer songCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM dim_song", Integer.class);
        Integer artistCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM dim_artist", Integer.class);
        Integer eventCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM fact_play_event", Integer.class);
        Integer snapshotCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM fact_rank_snapshot", Integer.class);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("songCount", songCount == null ? 0 : songCount);
        result.put("artistCount", artistCount == null ? 0 : artistCount);
        result.put("playEventCount", eventCount == null ? 0 : eventCount);
        result.put("rankSnapshotCount", snapshotCount == null ? 0 : snapshotCount);
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
