package lk.rumex.rumex_ott_mediaStat.config;

import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.SummaryRebuildProgress;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository.SummaryRebuildProgressRepository;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.service.MediaWatchSessionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;

@Component
@RequiredArgsConstructor
@Slf4j
public class SummaryRebuildStartupRunner implements ApplicationRunner {

    private static final String JOB_NAME = "media-daily-summary-bootstrap";
    private static final ZoneId SRI_LANKA_ZONE = ZoneId.of("Asia/Colombo");

    private final MediaWatchSessionService mediaWatchSessionService;
    private final SummaryRebuildProgressRepository progressRepository;

    @Value("${summary.rebuild.bootstrap.enabled:true}")
    private boolean enabled;

    @Value("${summary.rebuild.bootstrap.start-date:2026-01-01}")
    private LocalDate configuredStartDate;

    @Value("${summary.rebuild.bootstrap.target-date:2026-03-18}")
    private LocalDate configuredTargetDate;

    @Value("${summary.rebuild.bootstrap.tenant-id:#{null}}")
    private Long configuredTenantId;

    @Override
    public void run(ApplicationArguments args) {
        if (!enabled) {
            log.info("Summary rebuild bootstrap is disabled.");
            return;
        }

        SummaryRebuildProgress progress = progressRepository.findByJobName(JOB_NAME)
                .orElseGet(this::newProgressRow);

        progress.setStartDate(configuredStartDate);
        progress.setTargetDate(configuredTargetDate);
        progress.setTenantId(configuredTenantId);
        progressRepository.save(progress);

        LocalDate nextDate = progress.getLastCompletedDate() == null
                ? progress.getStartDate()
                : progress.getLastCompletedDate().plusDays(1);

        if (nextDate.isAfter(progress.getTargetDate())) {
            progress.setStatus("COMPLETED");
            progress.setLastMessage("Startup summary rebuild already completed.");
            progress.setLastRunAt(OffsetDateTime.now(SRI_LANKA_ZONE));
            progressRepository.save(progress);
            log.info("Summary rebuild bootstrap already completed through {}", progress.getLastCompletedDate());
            return;
        }

        while (!nextDate.isAfter(progress.getTargetDate())) {
            progress.setStatus("RUNNING");
            progress.setLastRunAt(OffsetDateTime.now(SRI_LANKA_ZONE));
            progress.setLastMessage("Rebuilding summaries for " + nextDate);
            progressRepository.save(progress);

            try {
                MediaWatchSessionService.IncrementalSummaryRebuildResult result =
                        mediaWatchSessionService.rebuildCumulativeSummariesIncrementally(
                                nextDate,
                                nextDate,
                                progress.getTenantId());

                progress.setLastCompletedDate(nextDate);
                progress.setLastProcessedSessions(result.processedSessions());
                progress.setLastUpdatedSummaries(result.updatedSummaries());
                progress.setLastMessage("Completed summary rebuild for " + nextDate);
                progress.setLastRunAt(OffsetDateTime.now(SRI_LANKA_ZONE));
                progressRepository.save(progress);

                log.info("Summary rebuild bootstrap completed for {}: sessions={}, summaries={}",
                        nextDate, result.processedSessions(), result.updatedSummaries());
                nextDate = nextDate.plusDays(1);
            } catch (Exception e) {
                progress.setStatus("FAILED");
                progress.setLastMessage("Failed on " + nextDate + ": " + e.getMessage());
                progress.setLastRunAt(OffsetDateTime.now(SRI_LANKA_ZONE));
                progressRepository.save(progress);
                log.error("Summary rebuild bootstrap failed for {}", nextDate, e);
                return;
            }
        }

        progress.setStatus("COMPLETED");
        progress.setLastMessage("Startup summary rebuild completed through " + progress.getTargetDate());
        progress.setLastRunAt(OffsetDateTime.now(SRI_LANKA_ZONE));
        progressRepository.save(progress);
        log.info("Summary rebuild bootstrap finished through {}", progress.getTargetDate());
    }

    private SummaryRebuildProgress newProgressRow() {
        SummaryRebuildProgress progress = new SummaryRebuildProgress();
        progress.setJobName(JOB_NAME);
        progress.setStartDate(configuredStartDate);
        progress.setTargetDate(configuredTargetDate);
        progress.setTenantId(configuredTenantId);
        progress.setStatus("PENDING");
        progress.setLastMessage("Waiting to start startup summary rebuild.");
        progress.setLastRunAt(OffsetDateTime.now(SRI_LANKA_ZONE));
        return progress;
    }
}
