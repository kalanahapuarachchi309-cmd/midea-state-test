package lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.DeviceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.InterfaceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.MediaWatchSession;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

public interface MediaWatchSessionRepository extends JpaRepository<MediaWatchSession, Long> {
    interface ActiveStreamSessionProjection {
        Long getUserId();
        String getDeviceId();
        Long getMediaId();
        MediaType getMediaType();
        String getSlug();
        DeviceType getDeviceType();
        InterfaceType getInterfaceType();
        OffsetDateTime getWatchedAt();
    }

    interface PeakUsageHourRow {
        Integer getHourValue();
        Long getActiveUsers();
        Long getTotalWatchTime();
        Long getSessionCount();
    }

    interface UsageAggregateRow {
        String getGroupLabel();
        Long getActiveUsers();
        Long getTotalWatchTime();
        Long getSessionCount();
    }

    List<MediaWatchSession> findByTenantId(Long tenantId);

    @Query(value = """
            select
                coalesce(sum(mws.watchTime), 0) as totalWatchTime,
                count(distinct mws.userId) as distinctUsers
            from media_watch_session mws
            where mws.tenantId = :tenantId
            """, nativeQuery = true)
    Optional<OverallTenantStatsProjection> findOverallStatsByTenantId(@Param("tenantId") Long tenantId);

    List<MediaWatchSession> findByWatchedAtBetween(OffsetDateTime start, OffsetDateTime end);

    List<MediaWatchSession> findByTenantIdAndWatchedAtBetween(Long tenantId, OffsetDateTime start, OffsetDateTime end);

    List<MediaWatchSession> findByTenantIdAndAccountOwnerIdAndWatchedAtBetween(
            Long tenantId,
            Long accountOwnerId,
            OffsetDateTime start,
            OffsetDateTime end);

    @Query(value = """
            select count(distinct concat(cast(mws.userId as char), '|', coalesce(mws.deviceId, '')))
            from media_watch_session mws
            where mws.tenantId = :tenantId
                and mws.accountOwnerId = :accountOwnerId
                and mws.watchedAt between :start and :end
                and mws.userId is not null
                and mws.deviceId is not null
                and not (mws.userId = :userId and mws.deviceId = :deviceId)
            """, nativeQuery = true)
    long countDistinctOtherActiveStreams(
            @Param("tenantId") Long tenantId,
            @Param("accountOwnerId") Long accountOwnerId,
            @Param("userId") Long userId,
            @Param("deviceId") String deviceId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end);

    @Query("""
            select
                s.userId as userId,
                s.deviceId as deviceId,
                s.mediaId as mediaId,
                s.mediaType as mediaType,
                s.slug as slug,
                s.deviceType as deviceType,
                s.interfaceType as interfaceType,
                s.watchedAt as watchedAt
            from MediaWatchSession s
            where s.tenantId = :tenantId
                and s.accountOwnerId = :accountOwnerId
                and s.watchedAt between :start and :end
                and s.userId is not null
                and s.deviceId is not null
            order by s.watchedAt desc
            """)
    List<ActiveStreamSessionProjection> findActiveStreamSessions(
            @Param("tenantId") Long tenantId,
            @Param("accountOwnerId") Long accountOwnerId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end);

    @Query("""
            select
                year(s.watchedAt) as year,
                month(s.watchedAt) as month,
                day(s.watchedAt) as day,
                coalesce(sum(s.watchTime), 0) as totalWatchTime,
                count(distinct s.userId) as distinctUsers
            from MediaWatchSession s
            where s.tenantId = :tenantId
                and s.watchedAt between :start and :end
            group by year(s.watchedAt), month(s.watchedAt), day(s.watchedAt)
            order by year(s.watchedAt), month(s.watchedAt), day(s.watchedAt)
            """)
    List<DailySessionAggregateProjection> findDailyAggregatesByTenantIdAndWatchedAtBetween(
            @Param("tenantId") Long tenantId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end);

    @Query("""
            select
                year(s.watchedAt) as year,
                month(s.watchedAt) as month,
                day(s.watchedAt) as day,
                s.userId as userId
            from MediaWatchSession s
            where s.tenantId = :tenantId
                and s.watchedAt between :start and :end
            group by year(s.watchedAt), month(s.watchedAt), day(s.watchedAt), s.userId
            order by year(s.watchedAt), month(s.watchedAt), day(s.watchedAt)
            """)
    List<DailyDistinctUserProjection> findDistinctUsersPerDayByTenantIdAndWatchedAtBetween(
            @Param("tenantId") Long tenantId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end);

    List<MediaWatchSession> findByTenantIdAndMediaType(Long tenantId, MediaType mediaType);

    List<MediaWatchSession> findByTenantIdAndMediaTypeAndWatchedAtBetween(Long tenantId, MediaType mediaType, OffsetDateTime start, OffsetDateTime end);

    List<MediaWatchSession> findByTenantIdAndMediaIdAndMediaType(Long tenantId, Long mediaId, MediaType mediaType);

    List<MediaWatchSession> findByTenantIdAndMediaIdAndMediaTypeAndWatchedAtBetween(Long tenantId, Long mediaId, MediaType mediaType, OffsetDateTime start, OffsetDateTime end);

    List<MediaWatchSession> findByTenantIdAndMediaIdAndMediaTypeAndDeviceTypeAndInterfaceType(Long tenantId, Long mediaId, MediaType mediaType, DeviceType deviceType, InterfaceType interfaceType);

    List<MediaWatchSession> findByTenantIdAndDeviceType(Long tenantId, DeviceType deviceType);

    List<MediaWatchSession> findByTenantIdAndInterfaceType(Long tenantId, InterfaceType interfaceType);

    List<MediaWatchSession> findByTenantIdAndDeviceTypeAndInterfaceType(Long tenantId, DeviceType deviceType, InterfaceType interfaceType);

    @Query(value = """
            SELECT
                HOUR(mws.watchedAt) AS hourValue,
                COUNT(DISTINCT mws.userId) AS activeUsers,
                COALESCE(SUM(COALESCE(mws.watchTime, 0)), 0) AS totalWatchTime,
                COUNT(*) AS sessionCount
            FROM media_watch_session mws
            WHERE mws.tenantId = :tenantId
              AND mws.watchedAt >= :start
              AND mws.watchedAt <= :end
              AND mws.userId IS NOT NULL
              AND EXISTS (
                    SELECT 1
                    FROM media_watch_session au
                    WHERE au.tenantId = mws.tenantId
                      AND au.userId = mws.userId
                      AND au.userId IS NOT NULL
                      AND au.watchedAt >= :activeSince
                      AND au.watchedAt <= :activeUntil
                )
            GROUP BY HOUR(mws.watchedAt)
            ORDER BY activeUsers DESC, totalWatchTime DESC, hourValue ASC
            """, nativeQuery = true)
    List<PeakUsageHourRow> findPeakUsageHoursForActiveUsers(
            @Param("tenantId") Long tenantId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end,
            @Param("activeSince") OffsetDateTime activeSince,
            @Param("activeUntil") OffsetDateTime activeUntil);

    @Query(value = """
            SELECT
                HOUR(mws.watchedAt) AS hourValue,
                COUNT(DISTINCT mws.userId) AS activeUsers,
                COALESCE(SUM(COALESCE(mws.watchTime, 0)), 0) AS totalWatchTime,
                COUNT(*) AS sessionCount
            FROM media_watch_session mws
            WHERE mws.tenantId = :tenantId
              AND mws.watchedAt >= :start
              AND mws.watchedAt <= :end
              AND mws.userId IS NOT NULL
            GROUP BY HOUR(mws.watchedAt)
            ORDER BY activeUsers DESC, totalWatchTime DESC, hourValue ASC
            """, nativeQuery = true)
    List<PeakUsageHourRow> findPeakUsageHoursBasic(
            @Param("tenantId") Long tenantId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end);

    @Query(value = """
            SELECT
                COALESCE(mws.deviceType, 'UNKNOWN') AS groupLabel,
                COUNT(DISTINCT mws.userId) AS activeUsers,
                COALESCE(SUM(COALESCE(mws.watchTime, 0)), 0) AS totalWatchTime,
                COUNT(*) AS sessionCount
            FROM media_watch_session mws
            WHERE mws.tenantId = :tenantId
              AND mws.watchedAt >= :start
              AND mws.watchedAt <= :end
              AND mws.userId IS NOT NULL
              AND EXISTS (
                    SELECT 1
                    FROM media_watch_session au
                    WHERE au.tenantId = mws.tenantId
                      AND au.userId = mws.userId
                      AND au.userId IS NOT NULL
                      AND au.watchedAt >= :activeSince
                      AND au.watchedAt <= :activeUntil
                )
            GROUP BY COALESCE(mws.deviceType, 'UNKNOWN')
            ORDER BY activeUsers DESC, totalWatchTime DESC, groupLabel ASC
            """, nativeQuery = true)
    List<UsageAggregateRow> aggregateDeviceUsageForActiveUsers(
            @Param("tenantId") Long tenantId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end,
            @Param("activeSince") OffsetDateTime activeSince,
            @Param("activeUntil") OffsetDateTime activeUntil);

    @Query(value = """
            SELECT
                COALESCE(mws.deviceType, 'UNKNOWN') AS groupLabel,
                COUNT(DISTINCT mws.userId) AS activeUsers,
                COALESCE(SUM(COALESCE(mws.watchTime, 0)), 0) AS totalWatchTime,
                COUNT(*) AS sessionCount
            FROM media_watch_session mws
            WHERE mws.tenantId = :tenantId
              AND mws.watchedAt >= :start
              AND mws.watchedAt <= :end
              AND mws.userId IS NOT NULL
            GROUP BY COALESCE(mws.deviceType, 'UNKNOWN')
            ORDER BY activeUsers DESC, totalWatchTime DESC, groupLabel ASC
            """, nativeQuery = true)
    List<UsageAggregateRow> aggregateDeviceUsageBasic(
            @Param("tenantId") Long tenantId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end);

    @Query(value = """
            SELECT
                COALESCE(mws.interfaceType, 'UNKNOWN') AS groupLabel,
                COUNT(DISTINCT mws.userId) AS activeUsers,
                COALESCE(SUM(COALESCE(mws.watchTime, 0)), 0) AS totalWatchTime,
                COUNT(*) AS sessionCount
            FROM media_watch_session mws
            WHERE mws.tenantId = :tenantId
              AND mws.watchedAt >= :start
              AND mws.watchedAt <= :end
              AND mws.userId IS NOT NULL
              AND EXISTS (
                    SELECT 1
                    FROM media_watch_session au
                    WHERE au.tenantId = mws.tenantId
                      AND au.userId = mws.userId
                      AND au.userId IS NOT NULL
                      AND au.watchedAt >= :activeSince
                      AND au.watchedAt <= :activeUntil
                )
            GROUP BY COALESCE(mws.interfaceType, 'UNKNOWN')
            ORDER BY activeUsers DESC, totalWatchTime DESC, groupLabel ASC
            """, nativeQuery = true)
    List<UsageAggregateRow> aggregatePlatformUsageForActiveUsers(
            @Param("tenantId") Long tenantId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end,
            @Param("activeSince") OffsetDateTime activeSince,
            @Param("activeUntil") OffsetDateTime activeUntil);

    @Query(value = """
            SELECT
                COALESCE(mws.interfaceType, 'UNKNOWN') AS groupLabel,
                COUNT(DISTINCT mws.userId) AS activeUsers,
                COALESCE(SUM(COALESCE(mws.watchTime, 0)), 0) AS totalWatchTime,
                COUNT(*) AS sessionCount
            FROM media_watch_session mws
            WHERE mws.tenantId = :tenantId
              AND mws.watchedAt >= :start
              AND mws.watchedAt <= :end
              AND mws.userId IS NOT NULL
            GROUP BY COALESCE(mws.interfaceType, 'UNKNOWN')
            ORDER BY activeUsers DESC, totalWatchTime DESC, groupLabel ASC
            """, nativeQuery = true)
    List<UsageAggregateRow> aggregatePlatformUsageBasic(
            @Param("tenantId") Long tenantId,
            @Param("start") OffsetDateTime start,
            @Param("end") OffsetDateTime end);
}
