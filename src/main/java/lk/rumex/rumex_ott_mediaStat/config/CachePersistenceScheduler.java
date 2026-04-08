package lk.rumex.rumex_ott_mediaStat.config;

import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.service.UserWatchHistoryService;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.service.MediaWatchSessionService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically persists cached data to the database so that
 * transient updates are eventually stored.
 */
@Component
public class CachePersistenceScheduler {

    private final UserWatchHistoryService historyService;
    private final MediaWatchSessionService sessionService;

    public CachePersistenceScheduler(UserWatchHistoryService historyService,
                                     MediaWatchSessionService sessionService) {
        this.historyService = historyService;
        this.sessionService = sessionService;
    }

    /**
     * Flush buffered writes continuously so cached data is not held in memory for hours.
     */
    @Scheduled(cron = "${app.cache.persistence.cron:0 */2 * * * *}", zone = "Asia/Colombo")
    public void persistCaches() {
        historyService.flushCacheToDb();
        sessionService.flushCacheToDb();
    }
}
