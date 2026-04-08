package lk.rumex.rumex_ott_mediaStat.mediaStatistics.service;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.ott_domain_models.episodes.model.Episodes;
import lk.rumex.ott_domain_models.movies.model.Movie;
import lk.rumex.ott_domain_models.shared.Enum.MaturityRating;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.repository.MovieRepository;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.DeviceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.InterfaceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.UserStatus;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.req.MediaWatchSessionDTO;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.req.MonthWiseStatsDTO;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res.*;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.exception.ConcurrentStreamLimitExceededException;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository.DailyDistinctUserProjection;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.mapper.MediaWatchSessionMapper;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.*;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository.DailySessionAggregateProjection;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository.MediaDailyWatchSummaryRepository;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository.MediaDataRepository;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository.MediaWatchSessionRepository;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.repository.EpisodeRepository;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.repository.TvShowRepository;
import lk.rumex.ott_domain_models.tvShows.model.TvShows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MediaWatchSessionService {

    private static final ZoneId SRI_LANKA_ZONE = ZoneId.of("Asia/Colombo");
    private static final int DB_BATCH_SIZE = 1000;
    private static final String PENDING_SESSIONS_ACTIVE_PREFIX = "buffer:mediaWatchSession:active:";
    private static final String PENDING_SESSIONS_PROCESSING_PREFIX = "buffer:mediaWatchSession:processing:";

    @Autowired
    private MediaWatchSessionRepository sessionRepo;

    @Autowired
    private MediaDataRepository mediaDataRepo;

    @Autowired
    private MediaWatchSessionMapper mapper;

    @Autowired
    private MediaDailyWatchSummaryRepository dailySummaryRepo;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private EpisodeRepository episodeRepository;

    @Autowired
    private TvShowRepository tvShowRepository;

    @Autowired
    private MovieRepository movieRepository;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Value("${concurrency.stream-limit.enabled:true}")
    private boolean streamLimitEnabled;

    @Value("${concurrency.stream-limit.max-active-streams:1}")
    private int maxActiveStreams;

    @Value("${concurrency.stream-limit.active-window-seconds:70}")
    private long activeWindowSeconds;

    @Value("${analytics.active-user.window-minutes:10}")
    private long activeUserWindowMinutes;

    @Value("${redis.fail-open.cooldown-seconds:120}")
    private long redisFailOpenCooldownSeconds;

    private volatile Instant redisUnavailableUntil = Instant.EPOCH;

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant.atZone(SRI_LANKA_ZONE).toOffsetDateTime();
    }

    private LocalDateTime toSriLankaLocalDateTime(OffsetDateTime dateTime) {
        return dateTime.toInstant().atZone(SRI_LANKA_ZONE).toLocalDateTime();
    }

    private final Map<MediaIdentity, Boolean> knownMediaCache = new ConcurrentHashMap<>();
    private final Map<SummarySourceKey, SummaryMetadata> summaryMetadataCache = new ConcurrentHashMap<>();
    private final Map<Long, Optional<Long>> episodeToShowCache = new ConcurrentHashMap<>();

    public void createWatchSession(MediaWatchSessionDTO dto, String title) {
        ensureMediaData(dto, title);

        validateConcurrentStreamLimit(dto);

        MediaWatchSession session = mapper.toEntity(dto);
        session.setWatchTime(15L);
        session.setWatchedAt(OffsetDateTime.now(SRI_LANKA_ZONE));
        redisTemplate.opsForZSet().add(activeSessionKey(dto.getTenantId()), session, sessionScore(session));
        evictAllMediaStats();
    }

    public List<PeakUsageHourResDTO> getPeakUsageHours(Long tenantId, Instant start, Instant end) {
        if (tenantId == null || start == null || end == null || end.isBefore(start)) {
            return List.of();
        }

        OffsetDateTime startDateTime = toOffsetDateTime(start);
        OffsetDateTime endDateTime = toOffsetDateTime(end);
        OffsetDateTime now = OffsetDateTime.now(SRI_LANKA_ZONE);
        OffsetDateTime activeSince = now.minusMinutes(activeUserWindowMinutes);
        OffsetDateTime activeWindowBucket = activeSince.truncatedTo(ChronoUnit.MINUTES);

        String key = buildMediaStatsKey("peakUsageActive", tenantId, startDateTime, endDateTime, activeWindowBucket);
        return getOrCacheMediaStatsList(key, () -> {
            try {
                return mapPeakUsageRows(sessionRepo.findPeakUsageHoursForActiveUsers(
                        tenantId, startDateTime, endDateTime, activeSince, now));
            } catch (Exception ex) {
                log.warn("Active-user peak usage query failed for tenant {}. Falling back to basic aggregate query. cause={}",
                        tenantId, ex.getMessage());
                return mapPeakUsageRows(sessionRepo.findPeakUsageHoursBasic(tenantId, startDateTime, endDateTime));
            }
        });
    }

    public DevicePlatformUsageResDTO getDevicePlatformUsage(Long tenantId, Instant start, Instant end) {
        if (tenantId == null || start == null || end == null || end.isBefore(start)) {
            return new DevicePlatformUsageResDTO(tenantId, List.of(), List.of());
        }

        OffsetDateTime startDateTime = toOffsetDateTime(start);
        OffsetDateTime endDateTime = toOffsetDateTime(end);
        OffsetDateTime now = OffsetDateTime.now(SRI_LANKA_ZONE);
        OffsetDateTime activeSince = now.minusMinutes(activeUserWindowMinutes);
        OffsetDateTime activeWindowBucket = activeSince.truncatedTo(ChronoUnit.MINUTES);

        String key = buildMediaStatsKey("devicePlatformUsageActive", tenantId, startDateTime, endDateTime, activeWindowBucket);
        return getOrCacheMediaStats(key, DevicePlatformUsageResDTO.class, () -> {
            try {
                List<DeviceUsageItemResDto> deviceUsage = mapUsageRows(
                        sessionRepo.aggregateDeviceUsageForActiveUsers(tenantId, startDateTime, endDateTime, activeSince, now));
                List<DeviceUsageItemResDto> platformUsage = mapUsageRows(
                        sessionRepo.aggregatePlatformUsageForActiveUsers(tenantId, startDateTime, endDateTime, activeSince, now));
                return new DevicePlatformUsageResDTO(tenantId, deviceUsage, platformUsage);
            } catch (Exception ex) {
                log.warn("Active-user device/platform query failed for tenant {}. Falling back to basic aggregate query. cause={}",
                        tenantId, ex.getMessage());
                List<DeviceUsageItemResDto> deviceUsage = mapUsageRows(
                        sessionRepo.aggregateDeviceUsageBasic(tenantId, startDateTime, endDateTime));
                List<DeviceUsageItemResDto> platformUsage = mapUsageRows(
                        sessionRepo.aggregatePlatformUsageBasic(tenantId, startDateTime, endDateTime));
                return new DevicePlatformUsageResDTO(tenantId, deviceUsage, platformUsage);
            }
        });
    }

    public List<DauResDTO> getDailyActiveUsers(Long tenantId, Instant start, Instant end) {
        if (tenantId == null || start == null || end == null || end.isBefore(start)) {
            return List.of();
        }

        // Keep DAU endpoint DB-first and cache-free for faster, stable response even when Redis is unavailable.
        return getDailyEngagementByDateRangeInternal(tenantId, start, end, false).stream()
                .map(day -> new DauResDTO(
                        day.getYear(),
                        day.getMonth(),
                        day.getDay(),
                        defaultLong(day.getDistinctUsers())))
                .toList();
    }

    private void ensureMediaData(MediaWatchSessionDTO dto, String title) {
        MediaIdentity mediaIdentity = new MediaIdentity(dto.getMediaId(), dto.getMediaType());
        if (Boolean.TRUE.equals(knownMediaCache.get(mediaIdentity))) {
            return;
        }

        synchronized (knownMediaCache) {
            if (Boolean.TRUE.equals(knownMediaCache.get(mediaIdentity))) {
                return;
            }

            MediaData existing = mediaDataRepo.findByMediaIdAndMediaType(dto.getMediaId(), dto.getMediaType());
            if (existing == null) {
                MediaData data = new MediaData();
                data.setMediaId(dto.getMediaId());
                data.setMediaType(dto.getMediaType());
                data.setSlug(dto.getSlug());
                data.setTitle(title);
                mediaDataRepo.save(data);
            }

            knownMediaCache.put(mediaIdentity, true);
        }
    }

    private void validateConcurrentStreamLimit(MediaWatchSessionDTO dto) {
        if (!streamLimitEnabled || maxActiveStreams <= 0) {
            return;
        }

        if (dto.getTenantId() == null
                || dto.getAccountOwnerId() == null
                || dto.getUserId() == null
                || !StringUtils.hasText(dto.getDeviceId())) {
            return;
        }

        ActiveStreamKey requestedStream = toActiveStreamKey(dto);
        if (requestedStream == null) {
            return;
        }

        OffsetDateTime now = OffsetDateTime.now(SRI_LANKA_ZONE);
        OffsetDateTime windowStart = now.minusSeconds(activeWindowSeconds);
        List<ActiveSessionSnapshot> pendingSessions = getPendingSessionsInRange(dto.getTenantId(), windowStart, now).stream()
                .filter(session -> Objects.equals(session.getTenantId(), dto.getTenantId())
                        && Objects.equals(session.getAccountOwnerId(), dto.getAccountOwnerId())
                        && session.getWatchedAt() != null
                        && !session.getWatchedAt().isBefore(windowStart))
                .map(this::toActiveSessionSnapshot)
                .filter(Objects::nonNull)
                .toList();

        long pendingOtherStreamCount = distinctOtherActiveStreamCount(pendingSessions, requestedStream);
        long dbOtherStreamCount = sessionRepo.countDistinctOtherActiveStreams(
                dto.getTenantId(),
                dto.getAccountOwnerId(),
                dto.getUserId(),
                dto.getDeviceId().trim(),
                windowStart,
                now);

        if (pendingOtherStreamCount + dbOtherStreamCount < maxActiveStreams) {
            return;
        }

        List<ActiveSessionSnapshot> activeSessions = new ArrayList<>(sessionRepo.findActiveStreamSessions(
                dto.getTenantId(),
                dto.getAccountOwnerId(),
                windowStart,
                now).stream()
                .map(this::toActiveSessionSnapshot)
                .filter(Objects::nonNull)
                .toList());
        activeSessions.addAll(pendingSessions);

        Set<String> activeStreamKeys = activeSessions.stream()
                .map(ActiveSessionSnapshot::key)
                .filter(Objects::nonNull)
                .map(this::streamIdentityToken)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (activeStreamKeys.contains(streamIdentityToken(requestedStream))) {
            return;
        }

        if (activeStreamKeys.size() >= maxActiveStreams) {
            throw new ConcurrentStreamLimitExceededException(buildConcurrentStreamConflict(dto, activeSessions));
        }
    }

    private ConcurrentStreamConflictResDTO buildConcurrentStreamConflict(
            MediaWatchSessionDTO dto,
            List<ActiveSessionSnapshot> activeSessions) {
        Map<ActiveStreamKey, ActiveSessionSnapshot> latestActiveSessions = new LinkedHashMap<>();
        for (ActiveSessionSnapshot session : activeSessions) {
            ActiveStreamKey key = session.key();
            if (key == null) {
                continue;
            }
            ActiveSessionSnapshot existing = latestActiveSessions.get(key);
            if (existing == null || isAfter(session.watchedAt(), existing.watchedAt())) {
                latestActiveSessions.put(key, session);
            }
        }

        List<ConcurrentStreamConflictResDTO.ActiveStreamResDTO> activeStreamDetails = latestActiveSessions.values()
                .stream()
                .sorted(Comparator.comparing(ActiveSessionSnapshot::watchedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .map(this::toActiveStreamResDTO)
                .toList();

        MediaData requestedMedia = getMediaData(dto.getMediaId(), dto.getMediaType());
        String requestedTitle = requestedMedia != null ? requestedMedia.getTitle() : null;

        return new ConcurrentStreamConflictResDTO(
                "CONCURRENT_STREAM_LIMIT_EXCEEDED",
                "Simultaneous stream limit exceeded for this account",
                dto.getAccountOwnerId(),
                dto.getUserId(),
                dto.getMediaId(),
                dto.getMediaType(),
                requestedTitle,
                maxActiveStreams,
                activeWindowSeconds,
                activeStreamDetails
        );
    }

    private ConcurrentStreamConflictResDTO.ActiveStreamResDTO toActiveStreamResDTO(MediaWatchSession session) {
        MediaData mediaData = getMediaData(session.getMediaId(), session.getMediaType());
        return new ConcurrentStreamConflictResDTO.ActiveStreamResDTO(
                session.getUserId(),
                session.getDeviceId(),
                session.getMediaId(),
                session.getMediaType(),
                mediaData != null ? mediaData.getTitle() : null,
                mediaData != null ? mediaData.getSlug() : session.getSlug(),
                session.getDeviceType() != null ? session.getDeviceType().name() : null,
                session.getInterfaceType() != null ? session.getInterfaceType().name() : null,
                session.getWatchedAt()
        );
    }

    private ConcurrentStreamConflictResDTO.ActiveStreamResDTO toActiveStreamResDTO(ActiveSessionSnapshot session) {
        MediaData mediaData = getMediaData(session.mediaId(), session.mediaType());
        return new ConcurrentStreamConflictResDTO.ActiveStreamResDTO(
                session.userId(),
                session.deviceId(),
                session.mediaId(),
                session.mediaType(),
                mediaData != null ? mediaData.getTitle() : null,
                mediaData != null ? mediaData.getSlug() : session.slug(),
                session.deviceType() != null ? session.deviceType().name() : null,
                session.interfaceType() != null ? session.interfaceType().name() : null,
                session.watchedAt()
        );
    }

    private boolean isAfter(OffsetDateTime candidate, OffsetDateTime reference) {
        if (candidate == null) {
            return false;
        }
        if (reference == null) {
            return true;
        }
        return candidate.isAfter(reference);
    }

    private ActiveStreamKey toActiveStreamKey(MediaWatchSessionDTO dto) {
        if (dto == null
                || dto.getTenantId() == null
                || dto.getAccountOwnerId() == null
                || dto.getUserId() == null
                || !StringUtils.hasText(dto.getDeviceId())) {
            return null;
        }
        return new ActiveStreamKey(
                dto.getTenantId(),
                dto.getAccountOwnerId(),
                dto.getUserId(),
                dto.getDeviceId().trim());
    }

    private ActiveStreamKey toActiveStreamKey(MediaWatchSession session) {
        if (session == null
                || session.getTenantId() == null
                || session.getAccountOwnerId() == null
                || session.getUserId() == null
                || !StringUtils.hasText(session.getDeviceId())) {
            return null;
        }
        return new ActiveStreamKey(
                session.getTenantId(),
                session.getAccountOwnerId(),
                session.getUserId(),
                session.getDeviceId().trim());
    }

    private ActiveSessionSnapshot toActiveSessionSnapshot(MediaWatchSession session) {
        ActiveStreamKey key = toActiveStreamKey(session);
        if (key == null) {
            return null;
        }
        return new ActiveSessionSnapshot(
                key,
                session.getMediaId(),
                session.getMediaType(),
                session.getSlug(),
                session.getDeviceType(),
                session.getInterfaceType(),
                session.getWatchedAt());
    }

    private ActiveSessionSnapshot toActiveSessionSnapshot(MediaWatchSessionRepository.ActiveStreamSessionProjection session) {
        if (session == null
                || session.getUserId() == null
                || !StringUtils.hasText(session.getDeviceId())) {
            return null;
        }
        return new ActiveSessionSnapshot(
                new ActiveStreamKey(null, null, session.getUserId(), session.getDeviceId().trim()),
                session.getMediaId(),
                session.getMediaType(),
                session.getSlug(),
                session.getDeviceType(),
                session.getInterfaceType(),
                session.getWatchedAt());
    }

    private long distinctOtherActiveStreamCount(List<ActiveSessionSnapshot> sessions, ActiveStreamKey requestedStream) {
        return sessions.stream()
                .map(ActiveSessionSnapshot::key)
                .filter(Objects::nonNull)
                .map(this::streamIdentityToken)
                .filter(token -> !Objects.equals(token, streamIdentityToken(requestedStream)))
                .distinct()
                .count();
    }

    private String streamIdentityToken(ActiveStreamKey key) {
        if (key == null || key.profileId() == null || !StringUtils.hasText(key.deviceId())) {
            return null;
        }
        return key.profileId() + "|" + key.deviceId().trim();
    }

    public List<TopWatchedMediaResDTO> getTopWatchedMediaByTenant(Long tenantId, int topN) {
        String key = buildDailyTopKey(tenantId, topN, null, null);
        return getOrCacheDailyTopStats(key, () -> {
            List<TopWatchedMediaResDTO> results = new ArrayList<>();
            results.addAll(getTopTvShowsWithEpisodeWatchTime(tenantId, topN, null));
            results.addAll(getTopMovies(tenantId, null));

            return results.stream()
                    .sorted(Comparator.comparingLong(TopWatchedMediaResDTO::getTotalWatchTime).reversed())
                    .limit(topN)
                    .collect(Collectors.toList());
        });
    }

    public List<MonthWiseStatsDTO> getMonthWiseStats(Long tenantId, MediaType mediaType,
            Date start, Date end) {
        String key = buildMediaStatsKey("month", tenantId, mediaType, start.toInstant().toEpochMilli(),
                end.toInstant().toEpochMilli());
        return getOrCacheMediaStatsList(key, () -> {
            OffsetDateTime startDateTime = toOffsetDateTime(start.toInstant());
            OffsetDateTime endDateTime = toOffsetDateTime(end.toInstant());

            List<MediaWatchSession> sessions = new ArrayList<>(sessionRepo
                    .findByTenantIdAndMediaTypeAndWatchedAtBetween(tenantId, mediaType,
                            startDateTime, endDateTime));
            sessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                    && s.getMediaType() == mediaType
                    && !s.getWatchedAt().isBefore(startDateTime)
                    && !s.getWatchedAt().isAfter(endDateTime)));

            if (mediaType == MediaType.TV_SHOW) {
                List<MediaWatchSession> episodeSessions = new ArrayList<>(sessionRepo
                        .findByTenantIdAndMediaTypeAndWatchedAtBetween(tenantId, MediaType.TV_EPISODE, startDateTime,
                                endDateTime));
                episodeSessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                        && s.getMediaType() == MediaType.TV_EPISODE
                        && !s.getWatchedAt().isBefore(startDateTime)
                        && !s.getWatchedAt().isAfter(endDateTime)));
                sessions.addAll(episodeSessions);
            }

            Map<YearMonth, List<MediaWatchSession>> grouped = sessions.stream()
                    .collect(Collectors.groupingBy(s -> YearMonth.from(toSriLankaLocalDateTime(s.getWatchedAt()))));

            return grouped.entrySet().stream()
                    .map(e -> {
                        YearMonth ym = e.getKey();
                        List<MediaWatchSession> list = e.getValue();
                        long totalWatchTime = list.stream().mapToLong(MediaWatchSession::getWatchTime).sum();
                        long distinctUsers = list.stream().map(MediaWatchSession::getUserId).distinct().count();
                        return new MonthWiseStatsDTO(ym.getYear(), ym.getMonthValue(), totalWatchTime, distinctUsers);
                    })
                    .sorted(Comparator.comparing(MonthWiseStatsDTO::getYear)
                            .thenComparing(MonthWiseStatsDTO::getMonth))
                    .collect(Collectors.toList());
        });
    }

    public OverallStatsResDTO getOverallStatsByTenant(Long tenantId) {
        String key = buildMediaStatsKey("overall", tenantId);
        return getOrCacheMediaStats(key, OverallStatsResDTO.class, () -> {
            return sessionRepo.findOverallStatsByTenantId(tenantId)
                    .map(stats -> new OverallStatsResDTO(
                            tenantId,
                            stats.getTotalWatchTime(),
                            stats.getDistinctUsers()))
                    .orElseGet(() -> new OverallStatsResDTO(tenantId, 0L, 0L));
        });
    }

    public List<DailyWatchTimeResDTO> getTotalWatchTimeByDateRange(Long tenantId, Instant start, Instant end) {
        String key = buildMediaStatsKey("dailyWT", tenantId, start, end);
        return getOrCacheMediaStatsList(key, () -> {
            return getDailyEngagementByDateRangeInternal(tenantId, start, end, true).stream()
                    .map(day -> new DailyWatchTimeResDTO(
                            day.getYear(),
                            day.getMonth(),
                            day.getDay(),
                            day.getTotalWatchTime()))
                    .toList();
        });
    }

    public List<DailyUniqueUsersResDTO> getUniqueUserCountByDateRange(Long tenantId, Instant start, Instant end) {
        String key = buildMediaStatsKey("uniqueUsers", tenantId, start, end);
        return getOrCacheMediaStatsList(key, () -> {
            return getDailyEngagementByDateRangeInternal(tenantId, start, end, true).stream()
                    .map(day -> new DailyUniqueUsersResDTO(
                            day.getYear(),
                            day.getMonth(),
                            day.getDay(),
                            day.getDistinctUsers()))
                    .toList();
        });
    }

    public List<DailyEngagementResDTO> getDailyEngagementByDateRange(Long tenantId, Instant start, Instant end) {
        String key = buildMediaStatsKey("dailyEngagement", tenantId, start, end);
        return getOrCacheMediaStatsList(key, () -> getDailyEngagementByDateRangeInternal(tenantId, start, end, true));
    }

    private List<DailyEngagementResDTO> getDailyEngagementByDateRangeInternal(Long tenantId, Instant start, Instant end,
            boolean includePendingSessions) {
            OffsetDateTime startDateTime = toOffsetDateTime(start);
            OffsetDateTime endDateTime = toOffsetDateTime(end);

            Map<LocalDate, DailyAccumulator> dailyMap = new TreeMap<>();

            for (DailySessionAggregateProjection projection : sessionRepo
                    .findDailyAggregatesByTenantIdAndWatchedAtBetween(tenantId, startDateTime, endDateTime)) {
                LocalDate date = LocalDate.of(projection.getYear(), projection.getMonth(), projection.getDay());
                DailyAccumulator accumulator = dailyMap.computeIfAbsent(date, ignored -> new DailyAccumulator());
                accumulator.totalWatchTime += defaultLong(projection.getTotalWatchTime());
            }

            for (DailyDistinctUserProjection projection : sessionRepo
                    .findDistinctUsersPerDayByTenantIdAndWatchedAtBetween(tenantId, startDateTime, endDateTime)) {
                LocalDate date = LocalDate.of(projection.getYear(), projection.getMonth(), projection.getDay());
                DailyAccumulator accumulator = dailyMap.computeIfAbsent(date, ignored -> new DailyAccumulator());
                accumulator.distinctUsers.add(projection.getUserId());
            }

            List<MediaWatchSession> cachedSessions = includePendingSessions
                    ? getCachedSessions(s -> s.getTenantId().equals(tenantId)
                            && !s.getWatchedAt().isBefore(startDateTime)
                            && !s.getWatchedAt().isAfter(endDateTime))
                    : Collections.emptyList();

            for (MediaWatchSession s : cachedSessions) {
                LocalDate d = toSriLankaLocalDateTime(s.getWatchedAt()).toLocalDate();
                DailyAccumulator accumulator = dailyMap.computeIfAbsent(d, ignored -> new DailyAccumulator());
                accumulator.totalWatchTime += defaultLong(s.getWatchTime());
                accumulator.distinctUsers.add(s.getUserId());
            }

            return dailyMap.entrySet().stream()
                    .map(e -> {
                        LocalDate d = e.getKey();
                        DailyAccumulator accumulator = e.getValue();
                        return new DailyEngagementResDTO(
                                d.getYear(),
                                d.getMonthValue(),
                                d.getDayOfMonth(),
                                accumulator.totalWatchTime,
                                (long) accumulator.distinctUsers.size());
                    })
                    .toList();
    }

    private long defaultLong(Long value) {
        return value == null ? 0L : value;
    }

    private List<PeakUsageHourResDTO> mapPeakUsageRows(List<MediaWatchSessionRepository.PeakUsageHourRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return List.of();
        }

        return rows.stream()
                .map(row -> new PeakUsageHourResDTO(
                        row.getHourValue() == null ? 0 : row.getHourValue(),
                        defaultLong(row.getActiveUsers()),
                        defaultLong(row.getTotalWatchTime()),
                        defaultLong(row.getSessionCount())))
                .toList();
    }

    private List<DeviceUsageItemResDto> mapUsageRows(List<MediaWatchSessionRepository.UsageAggregateRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return List.of();
        }

        return rows.stream()
                .map(row -> new DeviceUsageItemResDto(
                        StringUtils.hasText(row.getGroupLabel()) ? row.getGroupLabel() : "UNKNOWN",
                        defaultLong(row.getActiveUsers()),
                        defaultLong(row.getTotalWatchTime()),
                        defaultLong(row.getSessionCount())))
                .toList();
    }

    private static class DailyAccumulator {
        private long totalWatchTime;
        private final Set<Long> distinctUsers = new HashSet<>();
    }

    public MediaData getMediaData(Long mediaId, MediaType mediaType) {
        String key = buildMediaStatsKey("media", mediaId, mediaType);
        return getOrCacheMediaStats(key, MediaData.class,
                () -> mediaDataRepo.findByMediaIdAndMediaType(mediaId, mediaType));
    }

    public MediaStatsResDTO getStatByAllParams(Long tenantId, Long mediaId, MediaType mType,
            DeviceType dType, InterfaceType iType) {
        String key = buildMediaStatsKey("all", tenantId, mediaId, mType, dType, iType);
        return getOrCacheMediaStats(key, MediaStatsResDTO.class, () -> {
            List<MediaWatchSession> sessions;
            if (mType == MediaType.TV_SHOW) {
                sessions = getTvShowAndEpisodeSessions(tenantId, mediaId, dType, iType, null, null);
            } else {
                sessions = new ArrayList<>(sessionRepo
                        .findByTenantIdAndMediaIdAndMediaTypeAndDeviceTypeAndInterfaceType(
                                tenantId, mediaId, mType, dType, iType));
                sessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                        && s.getMediaId().equals(mediaId)
                        && s.getMediaType() == mType
                        && s.getDeviceType() == dType
                        && s.getInterfaceType() == iType));
            }

            long total = isDailySummarySupported(mType)
                    ? dailySummaryRepo.sumTotalWatchTimeByTenantIdAndMediaIdAndMediaType(
                            tenantId, mediaId, mType)
                    : sessions.stream().mapToLong(MediaWatchSession::getWatchTime).sum();
            long unique = sessions.stream().map(MediaWatchSession::getUserId).distinct().count();
            MediaData mediaData = getMediaData(mediaId, mType);
            String title = mediaData != null ? mediaData.getTitle() : "Title Not Available";
            String slug = mediaData != null ? mediaData.getSlug() : null;

            return new MediaStatsResDTO(tenantId, mediaId, mType, total, unique, title, slug, dType, iType);
        });
    }

    public MediaStatsResDTO getMediaStatsByMediaIdAndMediaType(Long tenantId, Long mediaId, MediaType mType) {
        String key = buildMediaStatsKey("mid", tenantId, mediaId, mType);
        return getOrCacheMediaStats(key, MediaStatsResDTO.class, () -> {
            List<MediaWatchSession> sessions;

            if (mType == MediaType.TV_SHOW) {
                sessions = getTvShowAndEpisodeSessions(tenantId, mediaId, null, null, null, null);
            } else {
                sessions = new ArrayList<>(sessionRepo
                        .findByTenantIdAndMediaIdAndMediaType(tenantId, mediaId, mType));
                sessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                        && s.getMediaId().equals(mediaId)
                        && s.getMediaType() == mType));
            }

            long total = isDailySummarySupported(mType)
                    ? dailySummaryRepo.sumTotalWatchTimeByTenantIdAndMediaIdAndMediaType(
                            tenantId, mediaId, mType)
                    : sessions.stream().mapToLong(MediaWatchSession::getWatchTime).sum();
            long unique = sessions.stream().map(MediaWatchSession::getUserId).distinct().count();
            MediaData mediaData = getMediaData(mediaId, mType);
            String title = mediaData != null ? mediaData.getTitle() : "Title Not Available";
            String slug = mediaData != null ? mediaData.getSlug() : null;

            return new MediaStatsResDTO(tenantId, mediaId, mType, total, unique, title, slug);
        });
    }

    public MediaStatsResDTO getTypeStatsByMediaType(Long tenantId, MediaType mType) {
        String key = buildMediaStatsKey("type", tenantId, mType);
        return getOrCacheMediaStats(key, MediaStatsResDTO.class, () -> {
            List<MediaWatchSession> sessions = new ArrayList<>(sessionRepo
                    .findByTenantIdAndMediaType(tenantId, mType));
            sessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                    && s.getMediaType() == mType));

            if (mType == MediaType.TV_SHOW) {
                List<MediaWatchSession> episodeSessions = new ArrayList<>(sessionRepo
                        .findByTenantIdAndMediaType(tenantId, MediaType.TV_EPISODE));
                episodeSessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                        && s.getMediaType() == MediaType.TV_EPISODE));
                sessions.addAll(episodeSessions);
            }

            long total = isDailySummarySupported(mType)
                    ? dailySummaryRepo.sumTotalWatchTimeByTenantIdAndMediaType(tenantId, mType)
                    : sessions.stream().mapToLong(MediaWatchSession::getWatchTime).sum();
            long unique = sessions.stream().map(MediaWatchSession::getUserId).distinct().count();

            return new MediaStatsResDTO(tenantId, mType, total, unique);
        });
    }

    public MediaStatsResDTO getStatByDeviceType(Long tenantId, DeviceType dType) {
        String key = buildMediaStatsKey("device", tenantId, dType);
        return getOrCacheMediaStats(key, MediaStatsResDTO.class, () -> {
            List<MediaWatchSession> sessions = new ArrayList<>(sessionRepo
                    .findByTenantIdAndDeviceType(tenantId, dType));
            sessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                    && s.getDeviceType() == dType));

            long total = sessions.stream().mapToLong(MediaWatchSession::getWatchTime).sum();
            long unique = sessions.stream().map(MediaWatchSession::getUserId).distinct().count();

            return new MediaStatsResDTO(tenantId, dType, total, unique);
        });
    }

    public MediaStatsResDTO getStatByInterfaceType(Long tenantId, InterfaceType iType) {
        String key = buildMediaStatsKey("interface", tenantId, iType);
        return getOrCacheMediaStats(key, MediaStatsResDTO.class, () -> {
            List<MediaWatchSession> sessions = new ArrayList<>(sessionRepo
                    .findByTenantIdAndInterfaceType(tenantId, iType));
            sessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                    && s.getInterfaceType() == iType));

            long total = sessions.stream().mapToLong(MediaWatchSession::getWatchTime).sum();
            long unique = sessions.stream().map(MediaWatchSession::getUserId).distinct().count();

            return new MediaStatsResDTO(tenantId, iType, total, unique);
        });
    }

    public MediaStatsResDTO getStatByDeviceAndInterfaceType(Long tenantId,
            DeviceType dType,
            InterfaceType iType) {
        String key = buildMediaStatsKey("deviceInterface", tenantId, dType, iType);
        return getOrCacheMediaStats(key, MediaStatsResDTO.class, () -> {
            List<MediaWatchSession> sessions = new ArrayList<>(sessionRepo
                    .findByTenantIdAndDeviceTypeAndInterfaceType(tenantId, dType, iType));
            sessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                    && s.getDeviceType() == dType
                    && s.getInterfaceType() == iType));

            long total = sessions.stream().mapToLong(MediaWatchSession::getWatchTime).sum();
            long unique = sessions.stream().map(MediaWatchSession::getUserId).distinct().count();

            return new MediaStatsResDTO(tenantId, dType, iType, total, unique);
        });
    }

    public long getUniqueUsersAtTimePoint(Long tenantId, Instant point, Instant end) {
        String key = buildMediaStatsKey("uniquePoint", tenantId, point, end);
        return getOrCacheMediaStats(key, Long.class, () -> {
            OffsetDateTime startDateTime = toOffsetDateTime(point);
            OffsetDateTime endDateTime = toOffsetDateTime(end);

            List<MediaWatchSession> sessions = new ArrayList<>(sessionRepo
                    .findByTenantIdAndWatchedAtBetween(tenantId, startDateTime, endDateTime));
            sessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                    && !s.getWatchedAt().isBefore(startDateTime)
                    && !s.getWatchedAt().isAfter(endDateTime)));
            return sessions.stream().map(MediaWatchSession::getUserId).distinct().count();
        });
    }

    public MediaStatsResDTO getMediaStatsByDate(Long tenantId, Long mediaId,
            MediaType mType, Instant dateStart) {
        String key = buildMediaStatsKey("date", tenantId, mediaId, mType, dateStart);
        OffsetDateTime startDateTime = toOffsetDateTime(dateStart);
        OffsetDateTime endDateTime = startDateTime.plus(1, ChronoUnit.DAYS);
        return getOrCacheMediaStats(key, MediaStatsResDTO.class, () -> {
            List<MediaWatchSession> sessions;

            if (mType == MediaType.TV_SHOW) {
                sessions = getTvShowAndEpisodeSessions(tenantId, mediaId, null, null, startDateTime, endDateTime);
            } else {
                sessions = new ArrayList<>(sessionRepo
                        .findByTenantIdAndMediaIdAndMediaTypeAndWatchedAtBetween(
                                tenantId, mediaId, mType, startDateTime, endDateTime));
                sessions.addAll(getCachedSessions(s -> s.getTenantId().equals(tenantId)
                        && s.getMediaId().equals(mediaId)
                        && s.getMediaType() == mType
                        && !s.getWatchedAt().isBefore(startDateTime)
                        && !s.getWatchedAt().isAfter(endDateTime)));
            }

            long total = sessions.stream().mapToLong(MediaWatchSession::getWatchTime).sum();
            long unique = sessions.stream().map(MediaWatchSession::getUserId).distinct().count();
            MediaData mediaData = getMediaData(mediaId, mType);
            String title = mediaData != null ? mediaData.getTitle() : "Title Not Available";
            String slug = mediaData != null ? mediaData.getSlug() : null;

            return new MediaStatsResDTO(tenantId, mediaId, mType, total, unique, title, slug);
        });
    }

    /**
     * Persist cached sessions to the database and clear the cache.
     */
    public void flushCacheToDb() {
        Set<String> processingKeys = promoteSessionBuffersForFlush();
        for (String processingKey : processingKeys) {
            List<MediaWatchSession> pending = readAllSessionsFromSortedSet(processingKey);
            if (pending.isEmpty()) {
                withRedisWrite("delete-processing-buffer", () -> redisTemplate.delete(processingKey));
                continue;
            }

            for (int start = 0; start < pending.size(); start += DB_BATCH_SIZE) {
                int end = Math.min(start + DB_BATCH_SIZE, pending.size());
                sessionRepo.saveAll(pending.subList(start, end));
            }
            flushAggregatedSummaries(pending);
            withRedisWrite("delete-processing-buffer", () -> redisTemplate.delete(processingKey));
        }
    }

    private List<MediaWatchSession> getCachedSessions(Predicate<MediaWatchSession> predicate) {
        return readAllPendingSessions().stream().filter(predicate).collect(Collectors.toList());
    }

    public List<MediaWatchSession> getPendingSessionsSnapshot() {
        return readAllPendingSessions();
    }

    public List<MediaWatchSession> getPendingSessionsInRange(Long tenantId, OffsetDateTime start, OffsetDateTime end) {
        if (tenantId == null || start == null || end == null) {
            return Collections.emptyList();
        }

        List<MediaWatchSession> sessions = new ArrayList<>();
        sessions.addAll(readSessionsFromSortedSetByScore(activeSessionKey(tenantId), start, end));
        sessions.addAll(readSessionsFromSortedSetByScore(processingSessionKey(tenantId), start, end));
        return sessions;
    }

    public List<TopWatchedMediaResDTO> getTopWatchedMediaByTenant(Long tenantId, int topN, MediaType mediaType) {
        String key = buildDailyTopKey(tenantId, topN, mediaType, null);
        return getOrCacheDailyTopStats(key, () -> {
            if (mediaType == MediaType.TV_SHOW) {
                return getTopTvShowsWithEpisodeWatchTime(tenantId, topN);
            }

            List<TopWatchedMediaResDTO> results = new ArrayList<>();
            List<MediaDailyWatchSummary> summaries = dailySummaryRepo
                    .findByTenantIdAndMediaTypeOrderByTotalWatchTimeDesc(
                            tenantId, mediaType);
            results.addAll(mapSummaryResults(tenantId, summaries));

            return results.stream()
                    .sorted(Comparator.comparingLong(TopWatchedMediaResDTO::getTotalWatchTime).reversed())
                    .limit(topN)
                    .collect(Collectors.toList());
        });
    }

    public List<TopWatchedMediaResDTO> getTopWatchedMediaByTenantAndStatus(Long tenantId,
            int topN,
            MaturityRating maturityRating) {
        String key = buildDailyTopKey(tenantId, topN, null, maturityRating);
        Collection<MaturityRating> statuses = expandMaturityRatings(maturityRating);
        log.info("Top watched by status request: tenantId={}, topN={}, maturityRating={}, expandedRatings={}, cacheKey={}",
                tenantId, topN, maturityRating, statuses, key);
        return getOrCacheDailyTopStats(key, () -> {
            List<TopWatchedMediaResDTO> results = new ArrayList<>();
            List<TopWatchedMediaResDTO> tvShows = getTopTvShowsWithEpisodeWatchTime(tenantId, topN, statuses);
            List<TopWatchedMediaResDTO> movies = getTopMovies(tenantId, statuses);
            results.addAll(tvShows);
            results.addAll(movies);

            List<TopWatchedMediaResDTO> finalResults = results.stream()
                    .sorted(Comparator.comparingLong(TopWatchedMediaResDTO::getTotalWatchTime).reversed())
                    .limit(topN)
                    .collect(Collectors.toList());

            log.info(
                    "Top watched by status computed: tenantId={}, maturityRating={}, expandedRatings={}, tvShowCount={}, movieCount={}, mergedCount={}, finalCount={}, topMediaIds={}",
                    tenantId,
                    maturityRating,
                    statuses,
                    tvShows.size(),
                    movies.size(),
                    results.size(),
                    finalResults.size(),
                    finalResults.stream().map(TopWatchedMediaResDTO::getMediaId).toList());

            return finalResults;
        });
    }

    private Collection<MaturityRating> expandMaturityRatings(MaturityRating maturityRating) {
        if (maturityRating == null) {
            return null;
        }
        return switch (maturityRating) {
            case G -> List.of(MaturityRating.G);
            case PG -> List.of(MaturityRating.G, MaturityRating.PG);
            case PG_13 -> List.of(MaturityRating.G, MaturityRating.PG, MaturityRating.PG_13);
            case R -> List.of(MaturityRating.G, MaturityRating.PG, MaturityRating.PG_13, MaturityRating.R);
            case NC_17 -> List.of(MaturityRating.G, MaturityRating.PG, MaturityRating.PG_13, MaturityRating.R,
                    MaturityRating.NC_17);
        };
    }

    private List<TopWatchedMediaResDTO> getTopTvShowsWithEpisodeWatchTime(Long tenantId, int topN) {
        return getTopTvShowsWithEpisodeWatchTime(tenantId, topN, null);
    }

    private List<TopWatchedMediaResDTO> getTopTvShowsWithEpisodeWatchTime(Long tenantId, int topN,
            Collection<MaturityRating> userStatuses) {
        List<MediaDailyWatchSummary> tvShowSummaries = (userStatuses == null || userStatuses.isEmpty())
                ? dailySummaryRepo.findByTenantIdAndMediaTypeOrderByTotalWatchTimeDesc(
                        tenantId, MediaType.TV_SHOW)
                : dailySummaryRepo.findByTenantIdAndMediaTypeAndMaturityRatingInOrderByTotalWatchTimeDesc(
                        tenantId, MediaType.TV_SHOW, userStatuses);
        List<MediaDailyWatchSummary> episodeSummaries = (userStatuses == null || userStatuses.isEmpty())
                ? dailySummaryRepo.findByTenantIdAndMediaTypeOrderByTotalWatchTimeDesc(
                        tenantId, MediaType.TV_EPISODE)
                : dailySummaryRepo.findByTenantIdAndMediaTypeAndMaturityRatingInOrderByTotalWatchTimeDesc(
                        tenantId, MediaType.TV_EPISODE, userStatuses);

        log.info("TV show top-source summaries loaded: tenantId={}, ratings={}, tvShowSummaryCount={}, episodeSummaryCount={}",
                tenantId, userStatuses, tvShowSummaries.size(), episodeSummaries.size());

        Set<Long> episodeIds = episodeSummaries.stream()
                .map(MediaDailyWatchSummary::getMediaId)
                .collect(Collectors.toSet());

        Map<Long, Long> episodeToShowMap = episodeIds.isEmpty()
                ? Collections.emptyMap()
                : episodeRepository.findAllById(episodeIds).stream()
                        .collect(Collectors.toMap(Episodes::getId, Episodes::getTvShowId));

        Map<Long, Long> watchTimeByShow = new HashMap<>();

        for (MediaDailyWatchSummary summary : tvShowSummaries) {
            watchTimeByShow.merge(summary.getMediaId(), summary.getTotalWatchTime(), Long::sum);
        }

        for (MediaDailyWatchSummary summary : episodeSummaries) {
            Long tvShowId = episodeToShowMap.get(summary.getMediaId());
            if (tvShowId != null) {
                watchTimeByShow.merge(tvShowId, summary.getTotalWatchTime(), Long::sum);
            }
        }

        return watchTimeByShow.entrySet().stream()
                .map(entry -> {
                    Long tvShowId = entry.getKey();
                    MediaData mediaData = getMediaData(tvShowId, MediaType.TV_SHOW);

                    String title = "Title Not Available";
                    String slug = null;

                    // Try to get from official TV Show domain model as source of truth for TV_SHOW
                    Optional<TvShows> tvShow = tvShowRepository.findById(tvShowId);
                    if (tvShow.isPresent()) {
                        TvShows show = tvShow.get();
                        title = (show.getTitle() != null) ? show.getTitle().getTitleEn() : "Title Not Available";
                        slug = show.getSlug();

                        // Sync/update denormalized MediaData cache if necessary
                        if (mediaData == null || !Objects.equals(mediaData.getSlug(), slug)
                                || !Objects.equals(mediaData.getTitle(), title)) {
                            if (mediaData == null) {
                                mediaData = new MediaData();
                                mediaData.setMediaId(tvShowId);
                                mediaData.setMediaType(MediaType.TV_SHOW);
                            }
                            mediaData.setTitle(title);
                            mediaData.setSlug(slug);
                            mediaDataRepo.save(mediaData);
                        }
                    } else if (mediaData != null) {
                        title = mediaData.getTitle();
                        slug = mediaData.getSlug();
                    }

                    return new TopWatchedMediaResDTO(
                            tenantId,
                            tvShowId,
                            MediaType.TV_SHOW,
                            entry.getValue(),
                            slug,
                            title);
                })
                .sorted(Comparator.comparingLong(TopWatchedMediaResDTO::getTotalWatchTime).reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }

    private List<TopWatchedMediaResDTO> getTopMovies(Long tenantId, Collection<MaturityRating> userStatuses) {
        List<MediaDailyWatchSummary> summaries = (userStatuses == null || userStatuses.isEmpty())
                ? dailySummaryRepo.findByTenantIdAndMediaTypeOrderByTotalWatchTimeDesc(
                        tenantId, MediaType.MOVIE)
                : dailySummaryRepo.findByTenantIdAndMediaTypeAndMaturityRatingInOrderByTotalWatchTimeDesc(
                        tenantId, MediaType.MOVIE, userStatuses);
        log.info("Movie top-source summaries loaded: tenantId={}, ratings={}, movieSummaryCount={}",
                tenantId, userStatuses, summaries.size());
        return mapSummaryResults(tenantId, summaries);
    }

    private List<MediaWatchSession> getTvShowAndEpisodeSessions(Long tenantId, Long tvShowId,
            DeviceType deviceType,
            InterfaceType interfaceType,
            OffsetDateTime startDateTime,
            OffsetDateTime endDateTime) {

        Predicate<MediaWatchSession> filters = s -> s.getTenantId().equals(tenantId)
                && (deviceType == null || s.getDeviceType() == deviceType)
                && (interfaceType == null || s.getInterfaceType() == interfaceType)
                && (startDateTime == null || !s.getWatchedAt().isBefore(startDateTime))
                && (endDateTime == null || !s.getWatchedAt().isAfter(endDateTime));

        List<MediaWatchSession> sessions = new ArrayList<>();

        List<MediaWatchSession> tvShowSessions;
        if (startDateTime != null && endDateTime != null) {
            tvShowSessions = sessionRepo.findByTenantIdAndMediaIdAndMediaTypeAndWatchedAtBetween(
                    tenantId, tvShowId, MediaType.TV_SHOW, startDateTime, endDateTime);
        } else if (deviceType != null && interfaceType != null) {
            tvShowSessions = sessionRepo.findByTenantIdAndMediaIdAndMediaTypeAndDeviceTypeAndInterfaceType(
                    tenantId, tvShowId, MediaType.TV_SHOW, deviceType, interfaceType);
        } else {
            tvShowSessions = sessionRepo.findByTenantIdAndMediaIdAndMediaType(tenantId, tvShowId, MediaType.TV_SHOW);
        }

        sessions.addAll(tvShowSessions.stream().filter(filters).collect(Collectors.toList()));

        Set<Long> episodeIds = episodeRepository.findByTvShowId(tvShowId).stream()
                .map(Episodes::getId)
                .collect(Collectors.toSet());

        if (!episodeIds.isEmpty()) {
            List<MediaWatchSession> episodeSessions;
            if (startDateTime != null && endDateTime != null) {
                episodeSessions = sessionRepo.findByTenantIdAndMediaTypeAndWatchedAtBetween(
                        tenantId, MediaType.TV_EPISODE, startDateTime, endDateTime);
            } else {
                episodeSessions = sessionRepo.findByTenantIdAndMediaType(tenantId, MediaType.TV_EPISODE);
            }

            sessions.addAll(episodeSessions.stream()
                    .filter(filters)
                    .filter(s -> episodeIds.contains(s.getMediaId()))
                    .collect(Collectors.toList()));
        }

        sessions.addAll(getCachedSessions(s -> filters.test(s)
                && ((s.getMediaType() == MediaType.TV_SHOW && s.getMediaId().equals(tvShowId))
                        || (s.getMediaType() == MediaType.TV_EPISODE && episodeIds.contains(s.getMediaId())))));

        return sessions;
    }

    public void clearTopMediaCache() {
        evictAllMediaStats();
        Set<String> keys = withRedisRead("read-topDaily-keys", () -> redisTemplate.keys("topDaily:*"), Collections.emptySet());
        if (keys != null && !keys.isEmpty()) {
            withRedisWrite("delete-topDaily-keys", () -> redisTemplate.delete(keys));
        }
    }

    private void evictAllMediaStats() {
        if (shouldSkipRedis()) {
            return;
        }

        Cache cache = cacheManager.getCache("mediaStats");
        if (cache != null) {
            try {
                cache.clear();
            } catch (RuntimeException ex) {
                if (isRedisConnectionFailure(ex)) {
                    markRedisUnavailable("clear-mediaStats-cache", ex);
                    return;
                }
                throw ex;
            }
        }
    }

    private <T> T getCachedMediaStats(String key, Class<T> type) {
        if (shouldSkipRedis()) {
            return null;
        }

        Cache cache = cacheManager.getCache("mediaStats");
        if (cache == null) {
            return null;
        }
        try {
            return cache.get(key, type);
        } catch (RuntimeException ex) {
            if (isRedisConnectionFailure(ex)) {
                markRedisUnavailable("get-mediaStats-cache", ex);
                return null;
            }
            throw ex;
        }
    }

    private <T> T getOrCacheMediaStats(String key, Class<T> type, Supplier<T> loader) {
        T cached = getCachedMediaStats(key, type);
        if (cached != null) {
            return cached;
        }
        T value = loader.get();
        cacheMediaStatsValue(key, value);
        return value;
    }

    private <T> List<T> getOrCacheMediaStatsList(String key, Supplier<List<T>> loader) {
        Cache cache = shouldSkipRedis() ? null : cacheManager.getCache("mediaStats");
        List<T> cached = null;
        if (cache != null) {
            try {
                @SuppressWarnings("unchecked")
                List<T> cacheValue = (List<T>) cache.get(key, Object.class);
                cached = cacheValue;
            } catch (RuntimeException ex) {
                if (isRedisConnectionFailure(ex)) {
                    markRedisUnavailable("get-mediaStats-list-cache", ex);
                } else {
                    throw ex;
                }
            }
        }
        if (cached != null) {
            return cached;
        }
        List<T> values = loader.get();
        cacheMediaStatsValue(key, values);
        return values;
    }

    private List<TopWatchedMediaResDTO> getOrCacheDailyTopStats(String key,
            Supplier<List<TopWatchedMediaResDTO>> loader) {
        Cache cache = shouldSkipRedis() ? null : cacheManager.getCache("mediaStats");
        List<TopWatchedMediaResDTO> cached = null;
        if (cache != null) {
            try {
                @SuppressWarnings("unchecked")
                List<TopWatchedMediaResDTO> cacheValue = (List<TopWatchedMediaResDTO>) cache.get(key, Object.class);
                cached = cacheValue;
            } catch (RuntimeException ex) {
                if (isRedisConnectionFailure(ex)) {
                    markRedisUnavailable("get-daily-top-spring-cache", ex);
                } else {
                    throw ex;
                }
            }
        }
        if (cached != null) {
            log.info("Top watched cache hit: source=spring-cache, key={}, resultCount={}", key, cached.size());
            return cached;
        }

        List<TopWatchedMediaResDTO> cachedRedis = convertCachedTopList(
                withRedisRead("get-daily-top-redis-cache", () -> redisTemplate.opsForValue().get(key), null));
        if (cachedRedis != null) {
            log.info("Top watched cache hit: source=redis, key={}, resultCount={}", key, cachedRedis.size());
            if (cache != null) {
                try {
                    cache.put(key, cachedRedis);
                } catch (RuntimeException ex) {
                    if (isRedisConnectionFailure(ex)) {
                        markRedisUnavailable("put-daily-top-spring-cache", ex);
                    } else {
                        throw ex;
                    }
                }
            }
            return cachedRedis;
        }

        log.info("Top watched cache miss: key={}", key);
        List<TopWatchedMediaResDTO> values = loader.get();
        Duration ttl = dailyTopCacheTtl();
        withRedisWrite("set-daily-top-redis-cache", () -> {
            if (!ttl.isNegative() && !ttl.isZero()) {
                redisTemplate.opsForValue().set(key, values, ttl);
            } else {
                redisTemplate.opsForValue().set(key, values);
            }
        });
        if (cache != null) {
            try {
                cache.put(key, values);
            } catch (RuntimeException ex) {
                if (isRedisConnectionFailure(ex)) {
                    markRedisUnavailable("put-daily-top-spring-cache", ex);
                } else {
                    throw ex;
                }
            }
        }
        log.info("Top watched cache populate: key={}, resultCount={}, ttlSeconds={}",
                key, values.size(), ttl != null ? ttl.getSeconds() : null);
        return values;
    }

    private void cacheMediaStatsValue(String key, Object value) {
        if (shouldSkipRedis()) {
            return;
        }

        Cache cache = cacheManager.getCache("mediaStats");
        if (cache != null) {
            try {
                cache.put(key, value);
            } catch (RuntimeException ex) {
                if (isRedisConnectionFailure(ex)) {
                    markRedisUnavailable("put-mediaStats-cache", ex);
                    return;
                }
                throw ex;
            }
        }
    }

    private List<TopWatchedMediaResDTO> convertCachedTopList(Object cached) {
        if (cached == null) {
            return null;
        }
        if (cached instanceof List<?> list) {
            if (list.isEmpty() || list.get(0) instanceof TopWatchedMediaResDTO) {
                @SuppressWarnings("unchecked")
                List<TopWatchedMediaResDTO> result = (List<TopWatchedMediaResDTO>) list;
                return result;
            }
        }
        return null;
    }

    private Duration dailyTopCacheTtl() {
        ZonedDateTime now = ZonedDateTime.now(SRI_LANKA_ZONE);
        ZonedDateTime next = now.toLocalDate().plusDays(1).atStartOfDay(SRI_LANKA_ZONE);
        return Duration.between(now, next);
    }

    private List<MediaWatchSession> readAllPendingSessions() {
        if (shouldSkipRedis()) {
            return Collections.emptyList();
        }

        List<MediaWatchSession> sessions = new ArrayList<>();
        for (String key : sessionBufferKeys(PENDING_SESSIONS_ACTIVE_PREFIX + "*")) {
            sessions.addAll(readAllSessionsFromSortedSet(key));
        }
        for (String key : sessionBufferKeys(PENDING_SESSIONS_PROCESSING_PREFIX + "*")) {
            sessions.addAll(readAllSessionsFromSortedSet(key));
        }
        return sessions;
    }

    private Set<String> promoteSessionBuffersForFlush() {
        Set<String> processingKeys = new LinkedHashSet<>(sessionBufferKeys(PENDING_SESSIONS_PROCESSING_PREFIX + "*"));
        for (String activeKey : sessionBufferKeys(PENDING_SESSIONS_ACTIVE_PREFIX + "*")) {
            String tenantSuffix = activeKey.substring(PENDING_SESSIONS_ACTIVE_PREFIX.length());
            String processingKey = PENDING_SESSIONS_PROCESSING_PREFIX + tenantSuffix;

            Boolean hasProcessingKey = withRedisRead(
                    "check-processing-buffer",
                    () -> redisTemplate.hasKey(processingKey),
                    Boolean.FALSE);
            if (Boolean.TRUE.equals(hasProcessingKey)) {
                processingKeys.add(processingKey);
                continue;
            }

            withRedisWrite("promote-active-buffer", () -> redisTemplate.rename(activeKey, processingKey));
            processingKeys.add(processingKey);
        }
        return processingKeys;
    }

    private List<MediaWatchSession> readAllSessionsFromSortedSet(String key) {
        Set<Object> raw = withRedisRead(
                "read-all-sessions-zset",
                () -> redisTemplate.opsForZSet().range(key, 0, -1),
                Collections.emptySet());
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }

        List<MediaWatchSession> sessions = new ArrayList<>(raw.size());
        for (Object obj : raw) {
            if (obj instanceof MediaWatchSession session) {
                sessions.add(session);
            }
        }
        return sessions;
    }

    private List<MediaWatchSession> readSessionsFromSortedSetByScore(String key, OffsetDateTime start, OffsetDateTime end) {
        Set<Object> raw = withRedisRead(
                "read-sessions-zset-by-score",
                () -> redisTemplate.opsForZSet().rangeByScore(
                        key,
                        start.toInstant().toEpochMilli(),
                        end.toInstant().toEpochMilli()),
                Collections.emptySet());
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }

        List<MediaWatchSession> sessions = new ArrayList<>(raw.size());
        for (Object obj : raw) {
            if (obj instanceof MediaWatchSession session) {
                sessions.add(session);
            }
        }
        return sessions;
    }

    private Set<String> sessionBufferKeys(String pattern) {
        Set<String> keys = withRedisRead("read-session-buffer-keys", () -> redisTemplate.keys(pattern), Collections.emptySet());
        return keys == null ? Collections.emptySet() : keys;
    }

    private boolean shouldSkipRedis() {
        return Instant.now().isBefore(redisUnavailableUntil);
    }

    private <T> T withRedisRead(String operation, Supplier<T> supplier, T fallback) {
        if (shouldSkipRedis()) {
            return fallback;
        }

        try {
            return supplier.get();
        } catch (RuntimeException ex) {
            if (isRedisConnectionFailure(ex)) {
                markRedisUnavailable(operation, ex);
                return fallback;
            }
            throw ex;
        }
    }

    private void withRedisWrite(String operation, Runnable action) {
        if (shouldSkipRedis()) {
            return;
        }

        try {
            action.run();
        } catch (RuntimeException ex) {
            if (isRedisConnectionFailure(ex)) {
                markRedisUnavailable(operation, ex);
                return;
            }
            throw ex;
        }
    }

    private boolean isRedisConnectionFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof RedisConnectionFailureException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void markRedisUnavailable(String operation, Throwable throwable) {
        long cooldownSeconds = Math.max(10L, redisFailOpenCooldownSeconds);
        redisUnavailableUntil = Instant.now().plusSeconds(cooldownSeconds);
        log.warn("Redis unavailable during {}. Skipping redis for {}s. Cause={}",
                operation, cooldownSeconds, throwable.getMessage());
    }

    private String activeSessionKey(Long tenantId) {
        return PENDING_SESSIONS_ACTIVE_PREFIX + tenantId;
    }

    private String processingSessionKey(Long tenantId) {
        return PENDING_SESSIONS_PROCESSING_PREFIX + tenantId;
    }

    private double sessionScore(MediaWatchSession session) {
        if (session.getWatchedAt() == null) {
            return 0D;
        }
        return session.getWatchedAt().toInstant().toEpochMilli();
    }

    private void flushAggregatedSummaries(List<MediaWatchSession> sessions) {
        if (sessions == null || sessions.isEmpty()) {
            return;
        }

        Map<Long, Long> episodeToShowMap = buildEpisodeToShowMap(sessions);
        Map<SummaryKey, Long> totals = new LinkedHashMap<>();
        Map<SummaryKey, LocalDate> latestDates = new LinkedHashMap<>();

        for (MediaWatchSession session : sessions) {
            SummaryKey key = buildSummaryKey(session, episodeToShowMap);
            if (key == null) {
                continue;
            }

            totals.merge(key, defaultLong(session.getWatchTime()), Long::sum);
            if (session.getWatchedAt() != null) {
                LocalDate watchedDate = toSriLankaLocalDateTime(session.getWatchedAt()).toLocalDate();
                latestDates.merge(key, watchedDate, (current, next) -> next.isAfter(current) ? next : current);
            }
        }

        for (Map.Entry<SummaryKey, Long> entry : totals.entrySet()) {
            SummaryKey key = entry.getKey();
            dailySummaryRepo.incrementSummary(
                    key.tenantId(),
                    key.mediaId(),
                    key.mediaType().name(),
                    key.maturityRating().name(),
                    latestDates.get(key),
                    entry.getValue());
        }
    }

    public void rebuildCumulativeSummaries() {
        OffsetDateTime startDateTime = OffsetDateTime.now(SRI_LANKA_ZONE).minusYears(10); // Far enough in past
        OffsetDateTime endDateTime = OffsetDateTime.now(SRI_LANKA_ZONE);

        List<MediaWatchSession> sessions = sessionRepo.findByWatchedAtBetween(startDateTime, endDateTime);
        Map<Long, Long> episodeToShowMap = buildEpisodeToShowMap(sessions);
        Map<SummaryKey, Long> totals = new HashMap<>();
        Map<SummaryKey, LocalDate> lastWatchedDates = new HashMap<>();

        for (MediaWatchSession session : sessions) {
            SummaryKey key = buildSummaryKey(session, episodeToShowMap);
            if (key != null) {
                totals.merge(key, session.getWatchTime(), Long::sum);
                LocalDate watchedDate = toSriLankaLocalDateTime(session.getWatchedAt()).toLocalDate();
                lastWatchedDates.merge(key, watchedDate, (o, n) -> n.isAfter(o) ? n : o);
            }
        }

        dailySummaryRepo.deleteAll();
        List<MediaDailyWatchSummary> summaries = totals.entrySet().stream()
                .map(entry -> {
                    SummaryKey key = entry.getKey();
                    MediaDailyWatchSummary summary = new MediaDailyWatchSummary();
                    summary.setTenantId(key.tenantId());
                    summary.setMediaId(key.mediaId());
                    summary.setMediaType(key.mediaType());
                    summary.setMaturityRating(key.maturityRating);
                    summary.setWatchedDate(lastWatchedDates.get(key));
                    summary.setTotalWatchTime(entry.getValue());
                    return summary;
                })
                .collect(Collectors.toList());
        dailySummaryRepo.saveAll(summaries);
    }

    @Transactional
    public IncrementalSummaryRebuildResult rebuildCumulativeSummariesIncrementally(
            LocalDate startDate,
            LocalDate endDate,
            Long tenantId) {
        LocalDate effectiveStartDate = startDate != null ? startDate : LocalDate.now(SRI_LANKA_ZONE);
        LocalDate effectiveEndDate = endDate != null ? endDate : effectiveStartDate;

        if (effectiveEndDate.isBefore(effectiveStartDate)) {
            throw new IllegalArgumentException("endDate must not be before startDate");
        }

        OffsetDateTime startDateTime = effectiveStartDate.atStartOfDay(SRI_LANKA_ZONE).toOffsetDateTime();
        OffsetDateTime endDateTime = effectiveEndDate.plusDays(1)
                .atStartOfDay(SRI_LANKA_ZONE)
                .minusNanos(1)
                .toOffsetDateTime();

        List<MediaWatchSession> sessions = tenantId != null
                ? sessionRepo.findByTenantIdAndWatchedAtBetween(tenantId, startDateTime, endDateTime)
                : sessionRepo.findByWatchedAtBetween(startDateTime, endDateTime);

        if (sessions.isEmpty()) {
            return new IncrementalSummaryRebuildResult(effectiveStartDate, effectiveEndDate, tenantId, 0, 0);
        }

        Map<Long, Long> episodeToShowMap = buildEpisodeToShowMap(sessions);
        Set<CompactKey> affectedKeys = sessions.stream()
                .map(session -> buildSummaryKey(session, episodeToShowMap))
                .filter(Objects::nonNull)
                .map(key -> new CompactKey(key.tenantId(), key.mediaId(), key.mediaType(), key.maturityRating()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        int updatedSummaries = 0;
        for (CompactKey key : affectedKeys) {
            upsertSummaryForKey(key);
            updatedSummaries++;
        }

        return new IncrementalSummaryRebuildResult(
                effectiveStartDate,
                effectiveEndDate,
                tenantId,
                sessions.size(),
                updatedSummaries);
    }

    public void consolidateExistingSummaries() {
        List<MediaDailyWatchSummary> all = dailySummaryRepo.findAll();
        Map<CompactKey, Long> totals = new HashMap<>();
        Map<CompactKey, LocalDate> lastDates = new HashMap<>();

        for (MediaDailyWatchSummary s : all) {
            CompactKey key = new CompactKey(s.getTenantId(), s.getMediaId(), s.getMediaType(), s.getMaturityRating());
            totals.merge(key, s.getTotalWatchTime(), Long::sum);
            if (s.getWatchedDate() != null) {
                lastDates.merge(key, s.getWatchedDate(), (o, n) -> n.isAfter(o) ? n : o);
            }
        }

        dailySummaryRepo.deleteAll();
        List<MediaDailyWatchSummary> consolidated = totals.entrySet().stream()
                .map(e -> {
                    CompactKey k = e.getKey();
                    MediaDailyWatchSummary summary = new MediaDailyWatchSummary();
                    summary.setTenantId(k.tenantId());
                    summary.setMediaId(k.mediaId());
                    summary.setMediaType(k.mediaType());
                    summary.setMaturityRating(k.maturityRating());
                    summary.setTotalWatchTime(e.getValue());
                    summary.setWatchedDate(lastDates.get(k));
                    return summary;
                })
                .collect(Collectors.toList());
        dailySummaryRepo.saveAll(consolidated);
    }

    private record CompactKey(Long tenantId, Long mediaId, MediaType mediaType, MaturityRating maturityRating) {
    }

    public record IncrementalSummaryRebuildResult(
            LocalDate startDate,
            LocalDate endDate,
            Long tenantId,
            int processedSessions,
            int updatedSummaries) {
    }

    private SummaryKey buildSummaryKey(MediaWatchSession session,
            Map<Long, Long> episodeToShowMap) {
        SummaryMetadata metadata = resolveSummaryMetadata(session, episodeToShowMap);
        if (metadata == null) {
            return null;
        }

        return new SummaryKey(
                session.getTenantId(),
                metadata.mediaId(),
                metadata.mediaType(),
                metadata.maturityRating()
        );
    }

    private SummaryMetadata resolveSummaryMetadata(MediaWatchSession session, Map<Long, Long> episodeToShowMap) {
        SummarySourceKey sourceKey = new SummarySourceKey(session.getMediaId(), session.getMediaType());
        SummaryMetadata cached = summaryMetadataCache.get(sourceKey);
        if (cached != null) {
            return cached;
        }

        SummaryMetadata resolved = loadSummaryMetadata(session, episodeToShowMap);
        if (resolved != null) {
            summaryMetadataCache.put(sourceKey, resolved);
        }
        return resolved;
    }

    private SummaryMetadata loadSummaryMetadata(MediaWatchSession session, Map<Long, Long> episodeToShowMap) {
        MediaType mediaType = session.getMediaType();
        if (mediaType == MediaType.MOVIE) {
            Movie movie = movieRepository.findById(session.getMediaId()).orElse(null);
            if (movie == null) {
                log.warn("Skipping summary rebuild for orphan movie session: sessionId={}, tenantId={}, mediaId={}",
                        session.getId(), session.getTenantId(), session.getMediaId());
                return null;
            }

            return new SummaryMetadata(session.getMediaId(), MediaType.MOVIE, movie.getMaturityRating());
        }

        if (mediaType == MediaType.TV_SHOW) {
            TvShows tvShows = tvShowRepository.findById(session.getMediaId()).orElse(null);
            if (tvShows == null) {
                log.warn("Skipping summary rebuild for orphan tv-show session: sessionId={}, tenantId={}, mediaId={}",
                        session.getId(), session.getTenantId(), session.getMediaId());
                return null;
            }

            return new SummaryMetadata(session.getMediaId(), MediaType.TV_SHOW, tvShows.getMaturityRating());
        }

        if (mediaType != MediaType.TV_EPISODE) {
            return null;
        }

        Long tvShowId = resolveTvShowId(session.getMediaId(), episodeToShowMap);
        if (tvShowId == null) {
            log.warn("Skipping summary rebuild for episode without parent show mapping: sessionId={}, tenantId={}, mediaId={}",
                    session.getId(), session.getTenantId(), session.getMediaId());
            return null;
        }

        TvShows tvShows = tvShowRepository.findById(tvShowId).orElse(null);
        if (tvShows == null) {
            log.warn("Skipping summary rebuild for episode with missing parent show: sessionId={}, tenantId={}, mediaId={}, tvShowId={}",
                    session.getId(), session.getTenantId(), session.getMediaId(), tvShowId);
            return null;
        }

        return new SummaryMetadata(tvShowId, MediaType.TV_SHOW, tvShows.getMaturityRating());
    }

    private Long resolveTvShowId(Long episodeId, Map<Long, Long> episodeToShowMap) {
        if (episodeToShowMap != null && episodeToShowMap.containsKey(episodeId)) {
            return episodeToShowMap.get(episodeId);
        }

        Optional<Long> cached = episodeToShowCache.get(episodeId);
        if (cached != null) {
            return cached.orElse(null);
        }

        Optional<Long> resolved = episodeRepository.findById(episodeId)
                .map(Episodes::getTvShowId);
        episodeToShowCache.put(episodeId, resolved);
        return resolved.orElse(null);
    }

    private Map<Long, Long> buildEpisodeToShowMap(List<MediaWatchSession> sessions) {
        Set<Long> episodeIds = sessions.stream()
                .filter(session -> session.getMediaType() == MediaType.TV_EPISODE)
                .map(MediaWatchSession::getMediaId)
                .collect(Collectors.toSet());
        if (episodeIds.isEmpty()) {
            return Collections.emptyMap();
        }
        return episodeRepository.findAllById(episodeIds).stream()
                .collect(Collectors.toMap(Episodes::getId, Episodes::getTvShowId));
    }

    private void upsertSummaryForKey(CompactKey key) {
        List<MediaWatchSession> sourceSessions = loadSessionsForSummaryKey(key);
        if (sourceSessions.isEmpty()) {
            return;
        }

        long totalWatchTime = 0L;
        LocalDate latestWatchedDate = null;

        for (MediaWatchSession session : sourceSessions) {
            if (session.getWatchTime() != null) {
                totalWatchTime += session.getWatchTime();
            }
            if (session.getWatchedAt() != null) {
                LocalDate watchedDate = toSriLankaLocalDateTime(session.getWatchedAt()).toLocalDate();
                if (latestWatchedDate == null || watchedDate.isAfter(latestWatchedDate)) {
                    latestWatchedDate = watchedDate;
                }
            }
        }

        MediaDailyWatchSummary summary = dailySummaryRepo
                .findByTenantIdAndMediaIdAndMediaTypeAndMaturityRating(
                        key.tenantId(),
                        key.mediaId(),
                        key.mediaType(),
                        key.maturityRating())
                .orElseGet(MediaDailyWatchSummary::new);

        summary.setTenantId(key.tenantId());
        summary.setMediaId(key.mediaId());
        summary.setMediaType(key.mediaType());
        summary.setMaturityRating(key.maturityRating());
        summary.setWatchedDate(latestWatchedDate);
        summary.setTotalWatchTime(totalWatchTime);
        dailySummaryRepo.save(summary);
    }

    private List<MediaWatchSession> loadSessionsForSummaryKey(CompactKey key) {
        if (key.mediaType() == MediaType.MOVIE) {
            return sessionRepo.findByTenantIdAndMediaIdAndMediaType(
                    key.tenantId(),
                    key.mediaId(),
                    MediaType.MOVIE);
        }

        if (key.mediaType() != MediaType.TV_SHOW) {
            return List.of();
        }

        List<MediaWatchSession> sessions = new ArrayList<>(sessionRepo.findByTenantIdAndMediaIdAndMediaType(
                key.tenantId(),
                key.mediaId(),
                MediaType.TV_SHOW));

        Set<Long> episodeIds = episodeRepository.findByTvShowId(key.mediaId()).stream()
                .map(Episodes::getId)
                .collect(Collectors.toSet());

        if (!episodeIds.isEmpty()) {
            sessions.addAll(sessionRepo.findByTenantIdAndMediaType(key.tenantId(), MediaType.TV_EPISODE).stream()
                    .filter(session -> episodeIds.contains(session.getMediaId()))
                    .toList());
        }

        return sessions;
    }

    private List<TopWatchedMediaResDTO> mapSummaryResults(Long tenantId,
            List<MediaDailyWatchSummary> summaries) {
        List<TopWatchedMediaResDTO> results = new ArrayList<>();
        for (MediaDailyWatchSummary summary : summaries) {
            MediaData mediaData = getMediaData(summary.getMediaId(), summary.getMediaType());
            String title = mediaData != null ? mediaData.getTitle() : "Title Not Available";
            String slug = mediaData != null ? mediaData.getSlug() : null;
            results.add(new TopWatchedMediaResDTO(
                    tenantId,
                    summary.getMediaId(),
                    summary.getMediaType(),
                    summary.getTotalWatchTime(),
                    slug,
                    title));
        }
        return results;
    }

    private boolean isDailySummarySupported(MediaType mediaType) {
        return mediaType == MediaType.MOVIE || mediaType == MediaType.TV_SHOW;
    }

    private String buildDailyTopKey(Long tenantId, int topN, MediaType mediaType, MaturityRating maturityRating) {
        LocalDate today = LocalDate.now(SRI_LANKA_ZONE);
        String typeToken = mediaType == null ? "all" : mediaType.name();
        String statusToken = maturityRating == null ? "all" : maturityRating.name();
        return buildMediaStatsKey("topDaily", tenantId, topN, typeToken, statusToken, today);
    }

    private String buildMediaStatsKey(String prefix, Object... parts) {
        return prefix + ":" + Arrays.stream(parts)
                .map(String::valueOf)
                .collect(Collectors.joining(":"));
    }

    private record SummaryKey(Long tenantId,
                              Long mediaId,
                              MediaType mediaType,
                              MaturityRating maturityRating) {
    }

    private record MediaIdentity(Long mediaId, MediaType mediaType) {
    }

    private record SummarySourceKey(Long mediaId, MediaType mediaType) {
    }

    private record SummaryMetadata(Long mediaId, MediaType mediaType, MaturityRating maturityRating) {
    }

    private record ActiveSessionSnapshot(ActiveStreamKey key,
                                         Long mediaId,
                                         MediaType mediaType,
                                         String slug,
                                         DeviceType deviceType,
                                         InterfaceType interfaceType,
                                         OffsetDateTime watchedAt) {
        private Long userId() {
            return key != null ? key.profileId() : null;
        }

        private String deviceId() {
            return key != null ? key.deviceId() : null;
        }
    }

    private record ActiveStreamKey(Long tenantId,
                                   Long accountOwnerId,
                                   Long profileId,
                                   String deviceId) {
    }
}
