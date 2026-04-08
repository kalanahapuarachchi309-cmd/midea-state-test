package lk.rumex.rumex_ott_mediaStat.config;

import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.service.UserWatchHistoryService;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.service.MediaWatchSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically persists cached data to the database so that
 * transient updates are eventually stored.
 */
@Component
@Slf4j
public class CachePersistenceScheduler {

    private final UserWatchHistoryService historyService;
    private final MediaWatchSessionService sessionService;

    @Value("${app.cache.persistence.enabled:true}")
    private boolean cachePersistenceEnabled;

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
        if (!cachePersistenceEnabled) {
            return;
        }

        safeFlush("user-watch-history", historyService::flushCacheToDb);
        safeFlush("media-watch-session", sessionService::flushCacheToDb);
    }

    private void safeFlush(String cacheName, Runnable flushAction) {
        try {
            flushAction.run();
        } catch (RedisConnectionFailureException ex) {
            log.warn("Skipping {} cache flush because Redis is unreachable: {}", cacheName, ex.getMessage());
        } catch (Exception ex) {
            log.error("Unexpected error while flushing {} cache.", cacheName, ex);
        }
    }
}
