package lk.rumex.rumex_ott_mediaStat.config;

import lk.rumex.rumex_ott_mediaStat.mediaStatistics.service.MediaDailyCleanupService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MediaDailyCleanupScheduler {

    private final MediaDailyCleanupService cleanupService;

    /**
     * Run cleanup every 5 minutes.
     */
    @Scheduled(cron = "0 */5 * * * *")
    public void runCleanup() {
        log.info("Triggering scheduled MediaDailyWatchSummary cleanup...");
        try {
            cleanupService.cleanupDuplicates();
        } catch (Exception e) {
            log.error("Error occurred during scheduled cleanup", e);
        }
    }
}
