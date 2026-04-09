package com.music.api.controller;

import com.music.api.model.ApiResponse;
import com.music.api.service.DashboardService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@CrossOrigin(origins = "*")
public class DashboardController {
    private final DashboardService dashboardService;

    public DashboardController(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    @GetMapping("/rankings")
    public ApiResponse<List<Map<String, Object>>> rankings(
            @RequestParam(defaultValue = "minute") String rankType,
            @RequestParam(defaultValue = "10") int limit,
            HttpServletRequest request
    ) {
        int boundedLimit = Math.min(Math.max(limit, 1), 50);
        String traceId = buildTraceId(request);
        return ApiResponse.ok(dashboardService.getRankings(rankType, boundedLimit), traceId);
    }

    @GetMapping("/analytics/overview")
    public ApiResponse<Map<String, Object>> overview(HttpServletRequest request) {
        return ApiResponse.ok(dashboardService.getOverview(), buildTraceId(request));
    }

    @GetMapping("/tasks")
    public ApiResponse<List<Map<String, Object>>> tasks(HttpServletRequest request) {
        return ApiResponse.ok(dashboardService.getTaskLogs(), buildTraceId(request));
    }

    @GetMapping("/ranks/recommend")
    public ApiResponse<List<Map<String, Object>>> rankRecommend(
            @RequestParam(defaultValue = "20") int limit,
            HttpServletRequest request
    ) {
        int boundedLimit = Math.min(Math.max(limit, 1), 100);
        return ApiResponse.ok(dashboardService.getRankRecommendations(boundedLimit), buildTraceId(request));
    }

    @GetMapping("/ranks/{rankId}/songs")
    public ApiResponse<List<Map<String, Object>>> rankSongs(
            @PathVariable String rankId,
            @RequestParam(defaultValue = "100") int limit,
            HttpServletRequest request
    ) {
        int boundedLimit = Math.min(Math.max(limit, 1), 200);
        return ApiResponse.ok(dashboardService.getRankSongs(rankId, boundedLimit), buildTraceId(request));
    }

    @GetMapping("/playlists/recommend")
    public ApiResponse<List<Map<String, Object>>> playlistRecommend(
            @RequestParam(defaultValue = "20") int limit,
            HttpServletRequest request
    ) {
        int boundedLimit = Math.min(Math.max(limit, 1), 100);
        return ApiResponse.ok(dashboardService.getPlaylistRecommendations(boundedLimit), buildTraceId(request));
    }

    @GetMapping("/playlists/{playlistId}/songs")
    public ApiResponse<List<Map<String, Object>>> playlistSongs(
            @PathVariable String playlistId,
            @RequestParam(defaultValue = "100") int limit,
            HttpServletRequest request
    ) {
        int boundedLimit = Math.min(Math.max(limit, 1), 200);
        return ApiResponse.ok(dashboardService.getPlaylistSongs(playlistId, boundedLimit), buildTraceId(request));
    }

    @GetMapping("/artists/recommend")
    public ApiResponse<List<Map<String, Object>>> artistRecommend(
            @RequestParam(defaultValue = "20") int limit,
            HttpServletRequest request
    ) {
        int boundedLimit = Math.min(Math.max(limit, 1), 100);
        return ApiResponse.ok(dashboardService.getArtistRecommendations(boundedLimit), buildTraceId(request));
    }

    @GetMapping("/artists/{artistId}/songs")
    public ApiResponse<List<Map<String, Object>>> artistSongs(
            @PathVariable String artistId,
            @RequestParam(defaultValue = "100") int limit,
            HttpServletRequest request
    ) {
        int boundedLimit = Math.min(Math.max(limit, 1), 200);
        return ApiResponse.ok(dashboardService.getArtistSongs(artistId, boundedLimit), buildTraceId(request));
    }

    @GetMapping("/songs/{songId}/analysis")
    public ApiResponse<Map<String, Object>> songAnalysis(
            @PathVariable String songId,
            HttpServletRequest request
    ) {
        return ApiResponse.ok(dashboardService.getSongAnalysis(songId), buildTraceId(request));
    }

    @PostMapping("/songs/{songId}/tags/generate")
    public ApiResponse<Map<String, Object>> generateSongTags(
            @PathVariable String songId,
            HttpServletRequest request
    ) {
        return ApiResponse.ok(dashboardService.generateSongTags(songId), buildTraceId(request));
    }

    private String buildTraceId(HttpServletRequest request) {
        String incoming = request.getHeader("X-Trace-Id");
        return incoming == null || incoming.isBlank() ? UUID.randomUUID().toString() : incoming;
    }
}
