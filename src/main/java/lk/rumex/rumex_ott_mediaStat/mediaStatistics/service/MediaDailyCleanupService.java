package lk.rumex.rumex_ott_mediaStat.mediaStatistics.service;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.ott_domain_models.shared.Enum.MaturityRating;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.MediaDailyWatchSummary;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository.MediaDailyWatchSummaryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class MediaDailyCleanupService {

    private final MediaDailyWatchSummaryRepository dailySummaryRepo;

    @Transactional
    public void cleanupDuplicates() {
        log.info("Starting MediaDailyWatchSummary cleanup...");
        List<Object[]> duplicateKeys = dailySummaryRepo.findDuplicateKeys();
        log.info("Found {} groups with duplicate keys", duplicateKeys.size());

        for (Object[] key : duplicateKeys) {
            Long tenantId = (Long) key[0];
            Long mediaId = (Long) key[1];
            MediaType mediaType = (MediaType) key[2];
            MaturityRating maturityRating = (MaturityRating) key[3];

            mergeDuplicates(tenantId, mediaId, mediaType, maturityRating);
        }
        log.info("MediaDailyWatchSummary cleanup completed.");
    }

    private void mergeDuplicates(Long tenantId, Long mediaId, MediaType mediaType, MaturityRating maturityRating) {
        List<MediaDailyWatchSummary> records = dailySummaryRepo.findByTenantIdAndMediaIdAndMediaTypeAndMaturityRatingOrderByIdAsc(
                tenantId, mediaId, mediaType, maturityRating);

        if (records.size() <= 1) {
            return;
        }

        MediaDailyWatchSummary oldest = records.get(0);
        long totalWatchTime = oldest.getTotalWatchTime() != null ? oldest.getTotalWatchTime() : 0;
        LocalDate latestDate = oldest.getWatchedDate();

        for (int i = 1; i < records.size(); i++) {
            MediaDailyWatchSummary latest = records.get(i);

            // Add watch time
            if (latest.getTotalWatchTime() != null) {
                totalWatchTime += latest.getTotalWatchTime();
            }

            // Update latest date if applicable
            if (latest.getWatchedDate() != null) {
                if (latestDate == null || latest.getWatchedDate().isAfter(latestDate)) {
                    latestDate = latest.getWatchedDate();
                }
            }

            // Delete the duplicate
            dailySummaryRepo.delete(latest);
            log.debug("Deleted duplicate record with ID: {}", latest.getId());
        }

        oldest.setTotalWatchTime(totalWatchTime);
        oldest.setWatchedDate(latestDate);
        dailySummaryRepo.save(oldest);
        log.info("Merged {} duplicates into record ID: {} (tenantId: {}, mediaId: {}, mediaType: {}, maturityRating: {})",
                records.size() - 1, oldest.getId(), tenantId, mediaId, mediaType, maturityRating);
    }
}
