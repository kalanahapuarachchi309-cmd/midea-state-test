package lk.rumex.rumex_ott_mediaStat.config;

import lk.rumex.rumex_ott_mediaStat.mediaStatistics.service.MediaWatchSessionService;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically clears all caches so that data is refreshed from the database
 * ensuring cache and persistence are synchronized.
 */
@Component
public class CacheSyncScheduler {

    private final CacheManager cacheManager;
    private final MediaWatchSessionService mediaWatchSessionService;

    public CacheSyncScheduler(CacheManager cacheManager,
            MediaWatchSessionService mediaWatchSessionService) {
        this.cacheManager = cacheManager;
        this.mediaWatchSessionService = mediaWatchSessionService;
    }

    /**
     * Clear top 10 media keys every day at 5:00 AM (Sri Lanka time).
     */
    @Scheduled(cron = "0 0 5 * * *", zone = "Asia/Colombo")
    public void clearDailyTopStats() {
        mediaWatchSessionService.clearTopMediaCache();
    }

    /**
     * Evict all cache entries daily between 14:00 and 17:00 (Sri Lanka time).
     */
    @Scheduled(cron = "0 */5 14-16 * * *", zone = "Asia/Colombo")
    public void evictAllCaches() {
        for (String name : cacheManager.getCacheNames()) {
            Cache cache = cacheManager.getCache(name);
            if (cache != null) {
                cache.clear();
            }
        }
    }
}
