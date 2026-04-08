package lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.ott_domain_models.shared.Enum.MaturityRating;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.UserStatus;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.MediaDailyWatchSummary;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface MediaDailyWatchSummaryRepository extends JpaRepository<MediaDailyWatchSummary, Long> {
        Optional<MediaDailyWatchSummary> findByTenantIdAndMediaIdAndMediaType(
                        Long tenantId,
                        Long mediaId,
                        MediaType mediaType
        );

        Optional<MediaDailyWatchSummary> findByTenantIdAndMediaIdAndMediaTypeAndMaturityRating(
                        Long tenantId,
                        Long mediaId,
                        MediaType mediaType,
                        MaturityRating maturityRating
        );

        List<MediaDailyWatchSummary> findByTenantIdOrderByTotalWatchTimeDesc(
                        Long tenantId);

        List<MediaDailyWatchSummary> findByTenantIdAndMaturityRatingOrderByTotalWatchTimeDesc(
                        Long tenantId,
                        MaturityRating userStatus);

        List<MediaDailyWatchSummary> findByTenantIdAndMaturityRatingInOrderByTotalWatchTimeDesc(
                        Long tenantId,
                        Collection<MaturityRating> userStatuses);

        List<MediaDailyWatchSummary> findByTenantIdAndMediaTypeOrderByTotalWatchTimeDesc(
                        Long tenantId,
                        MediaType mediaType);

        List<MediaDailyWatchSummary> findByTenantIdAndMediaTypeAndMaturityRatingOrderByTotalWatchTimeDesc(
                        Long tenantId,
                        MediaType mediaType,
                        MaturityRating userStatus);

        List<MediaDailyWatchSummary> findByTenantIdAndMediaTypeAndMaturityRatingInOrderByTotalWatchTimeDesc(
                        Long tenantId,
                        MediaType mediaType,
                        Collection<MaturityRating> userStatuses);

        @Query("""
                        select coalesce(sum(summary.totalWatchTime), 0)
                        from MediaDailyWatchSummary summary
                        where summary.tenantId = :tenantId
                                and summary.mediaId = :mediaId
                                and summary.mediaType = :mediaType
                        """)
        Long sumTotalWatchTimeByTenantIdAndMediaIdAndMediaType(
                        @Param("tenantId") Long tenantId,
                        @Param("mediaId") Long mediaId,
                        @Param("mediaType") MediaType mediaType);

        @Query("""
                        select coalesce(sum(summary.totalWatchTime), 0)
                        from MediaDailyWatchSummary summary
                        where summary.tenantId = :tenantId
                                and summary.mediaType = :mediaType
                        """)
        Long sumTotalWatchTimeByTenantIdAndMediaType(
                        @Param("tenantId") Long tenantId,
                        @Param("mediaType") MediaType mediaType);

        void deleteByWatchedDateBetween(LocalDate startDate, LocalDate endDate);

        @org.springframework.data.jpa.repository.Query("SELECT s.tenantId, s.mediaId, s.mediaType, s.maturityRating FROM MediaDailyWatchSummary s "
                        +
                        "GROUP BY s.tenantId, s.mediaId, s.mediaType, s.maturityRating HAVING COUNT(s) > 1")
        List<Object[]> findDuplicateKeys();

        List<MediaDailyWatchSummary> findByTenantIdAndMediaIdAndMediaTypeAndMaturityRatingOrderByIdAsc(
                        Long tenantId,
                        Long mediaId,
                        MediaType mediaType,
                        MaturityRating maturityRating);

        @Transactional
        @Modifying
        @Query(value = """
                        insert into media_daily_watch_summary_v2
                                (tenantId, mediaId, mediaType, maturityRating, watchedDate, totalWatchTime)
                        values
                                (:tenantId, :mediaId, :mediaType, :maturityRating, :watchedDate, :watchTime)
                        on duplicate key update
                                totalWatchTime = coalesce(totalWatchTime, 0) + values(totalWatchTime),
                                watchedDate = case
                                        when watchedDate is null then values(watchedDate)
                                        when values(watchedDate) is null then watchedDate
                                        when values(watchedDate) > watchedDate then values(watchedDate)
                                        else watchedDate
                                end
                        """, nativeQuery = true)
        void incrementSummary(
                        @Param("tenantId") Long tenantId,
                        @Param("mediaId") Long mediaId,
                        @Param("mediaType") String mediaType,
                        @Param("maturityRating") String maturityRating,
                        @Param("watchedDate") LocalDate watchedDate,
                        @Param("watchTime") Long watchTime);
}
