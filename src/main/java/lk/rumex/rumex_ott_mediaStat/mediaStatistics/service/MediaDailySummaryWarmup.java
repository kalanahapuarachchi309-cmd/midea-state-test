package lk.rumex.rumex_ott_mediaStat.mediaStatistics.service;

import lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository.MediaDailyWatchSummaryRepository;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class MediaDailySummaryWarmup implements ApplicationRunner {
    private final MediaWatchSessionService mediaWatchSessionService;
    private final MediaDailyWatchSummaryRepository dailySummaryRepo;

    public MediaDailySummaryWarmup(MediaWatchSessionService mediaWatchSessionService,
            MediaDailyWatchSummaryRepository dailySummaryRepo) {
        this.mediaWatchSessionService = mediaWatchSessionService;
        this.dailySummaryRepo = dailySummaryRepo;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (dailySummaryRepo.count() == 0) {
            mediaWatchSessionService.rebuildCumulativeSummaries();
        }
    }
}
