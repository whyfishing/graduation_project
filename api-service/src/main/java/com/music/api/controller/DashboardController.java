package com.music.api.controller;

import com.music.api.model.ApiResponse;
import com.music.api.service.DashboardService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
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

    private String buildTraceId(HttpServletRequest request) {
        String incoming = request.getHeader("X-Trace-Id");
        return incoming == null || incoming.isBlank() ? UUID.randomUUID().toString() : incoming;
    }
}
