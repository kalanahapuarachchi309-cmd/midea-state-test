package lk.rumex.rumex_ott_mediaStat.mediaStatistics.controller;

import io.swagger.v3.oas.annotations.Operation;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.service.MediaWatchSessionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.Map;

@CrossOrigin
@RestController
@RequestMapping("/migration")
public class MediaMigrationController {

    @Autowired
    private MediaWatchSessionService mediaWatchSessionService;

    @Operation(summary = "Consolidate Daily Summaries", description = "One-time migration to merge existing daily rows into single cumulative rows per media.")
    @PostMapping("/consolidate-summaries")
    public ResponseEntity<String> consolidateSummaries() {
        mediaWatchSessionService.consolidateExistingSummaries();
        return ResponseEntity.ok("Summaries consolidated successfully.");
    }

    @Operation(summary = "Rebuild Cumulative Summaries", description = "Rebuilds cumulative summaries only for sessions inside the given date window and updates the affected summary rows.")
    @PostMapping("/rebuild-summaries")
    public ResponseEntity<Map<String, Object>> rebuildSummaries(
            @RequestParam(required = false) LocalDate startDate,
            @RequestParam(required = false) LocalDate endDate,
            @RequestParam(required = false) Long tenantId) {
        MediaWatchSessionService.IncrementalSummaryRebuildResult result =
                mediaWatchSessionService.rebuildCumulativeSummariesIncrementally(startDate, endDate, tenantId);

        return ResponseEntity.ok(Map.of(
                "message", "Summaries rebuilt incrementally.",
                "startDate", result.startDate(),
                "endDate", result.endDate(),
                "tenantId", result.tenantId(),
                "processedSessions", result.processedSessions(),
                "updatedSummaries", result.updatedSummaries()
        ));
    }
}
