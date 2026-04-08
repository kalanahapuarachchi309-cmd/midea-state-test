package lk.rumex.rumex_ott_mediaStat.commonStats.service;

import jakarta.validation.Valid;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.req.UserWatchHistoryDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.service.UserWatchHistoryService;
import lk.rumex.rumex_ott_mediaStat.commonStats.dto.req.CommonStatReqDTO;
import lk.rumex.rumex_ott_mediaStat.commonStats.dto.res.DailyStatsResDTO;
import lk.rumex.rumex_ott_mediaStat.commonStats.dto.dto.DeviceMetrics;
import lk.rumex.rumex_ott_mediaStat.commonStats.dto.res.PerMinuteStatsResDTO;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.DeviceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.req.MediaWatchSessionDTO;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.service.MediaWatchSessionService;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.MediaWatchSession;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository.MediaWatchSessionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class CommonStatService {

    private final MediaWatchSessionService mediaWatchSessionService;
    private final UserWatchHistoryService userWatchHistoryService;
    private final MediaWatchSessionRepository sessionRepo;
    private final RedisTemplate<String, Object> redisTemplate;
    private final CacheManager cacheManager;

    private static final ZoneId SRI_LANKA_ZONE = ZoneId.of("Asia/Colombo");

    private LocalDateTime toSriLankaLocalDateTime(OffsetDateTime dateTime) {
        return dateTime.toInstant().atZone(SRI_LANKA_ZONE).toLocalDateTime();
    }

    @Autowired
    public CommonStatService(MediaWatchSessionService mediaWatchSessionService,
                             UserWatchHistoryService userWatchHistoryService,
                             MediaWatchSessionRepository sessionRepo,
                             RedisTemplate<String, Object> redisTemplate,
                             CacheManager cacheManager) {
        this.mediaWatchSessionService = mediaWatchSessionService;
        this.userWatchHistoryService = userWatchHistoryService;
        this.sessionRepo = sessionRepo;
        this.redisTemplate = redisTemplate;
        this.cacheManager = cacheManager;
    }

    public void createCommonStat(@Valid CommonStatReqDTO dto, String title) {
        mediaWatchSessionService.createWatchSession(
                new MediaWatchSessionDTO(
                        dto.getTenantId(),
                        dto.getMediaId(),
                        dto.getMediaType(),
                        dto.getSlug(),
                        dto.getUserId(),
                        dto.getAccountOwnerId(),
                        dto.getDeviceId(),
                        /*dto.getUserStatus(),*/
                        dto.getWatchTime(),
                        dto.getDeviceType(),
                        dto.getInterfaceType()
                ),
                title
        );

        userWatchHistoryService.createOrUpdateUserWatchHistory(
                new UserWatchHistoryDTO(
                        dto.getUserId(),
                        dto.getMediaId(),
                        dto.getMediaType(),
                        dto.getSlug(),
                        dto.getLastWatchPosition(),
                        dto.getTotalDuration(),
                        dto.getIsCompleted()
                )
        );
    }

    private List<PerMinuteStatsResDTO> computeLast3Hours(Long tenantId) {
        OffsetDateTime now = OffsetDateTime.now(SRI_LANKA_ZONE);
        OffsetDateTime from = now.minus(3, ChronoUnit.HOURS);

        List<MediaWatchSession> dbSessions = sessionRepo
                .findByTenantIdAndWatchedAtBetween(tenantId, from, now);

        List<MediaWatchSession> cached = mediaWatchSessionService
                .getPendingSessionsInRange(tenantId, from, now);

        List<MediaWatchSession> sessions = new ArrayList<>(dbSessions);
        sessions.addAll(cached);

        Map<LocalDateTime, Map<DeviceType, Set<Long>>> grouped = new HashMap<>();
        for (MediaWatchSession s : sessions) {

            LocalDateTime key = toSriLankaLocalDateTime(s.getWatchedAt())
                    .truncatedTo(ChronoUnit.MINUTES);

            grouped.computeIfAbsent(key, k -> new HashMap<>())
                    .computeIfAbsent(s.getDeviceType(), k -> new HashSet<>())
                    .add(s.getUserId());
        }

        List<PerMinuteStatsResDTO> filled = new ArrayList<>(180);
        LocalDateTime nowDateTime = now.toLocalDateTime().truncatedTo(ChronoUnit.MINUTES);
        LocalDateTime current = nowDateTime.minusHours(3);

        while (!current.isAfter(nowDateTime)) {

            Map<DeviceType, Set<Long>> byDevice = grouped.getOrDefault(current, Collections.emptyMap());

            int totalUnique = byDevice.values().stream()
                    .mapToInt(Set::size)
                    .sum();

            List<DeviceMetrics> deviceMetrics = byDevice.entrySet().stream()
                    .map(e -> new DeviceMetrics(e.getKey(), e.getValue().size(), 0L))
                    .collect(Collectors.toList());

            filled.add(new PerMinuteStatsResDTO(
                    current.getYear(),
                    current.getMonthValue(),
                    current.getDayOfMonth(),
                    current.getHour(),
                    current.getMinute(),
                    totalUnique,
                    deviceMetrics.isEmpty() ? null : deviceMetrics
            ));

            current = current.plusMinutes(1);
        }

        return filled;
    }

    public List<PerMinuteStatsResDTO> getLast3HoursStatsPerMinute(Long tenantId) {
        List<PerMinuteStatsResDTO> cached = getCachedLast3HoursStats(tenantId);
        List<PerMinuteStatsResDTO> stats = computeLast3Hours(tenantId);
        List<PerMinuteStatsResDTO> merged = mergeWithCachedStats(stats, cached);
        cacheLast3HoursStats(tenantId, merged);
        return merged;
    }

    private void cacheLast3HoursStats(Long tenantId, List<PerMinuteStatsResDTO> stats) {
        String key = last3HoursKey(tenantId);
        List<PerMinuteStatsResDTO> cacheableStats = excludeRecentMinutes(stats, 3);
        redisTemplate.opsForValue().set(key, cacheableStats);
        Cache mediaStatsCache = cacheManager.getCache("mediaStats");
        if (mediaStatsCache != null) {
            mediaStatsCache.put(key, cacheableStats);
        }
    }

    @SuppressWarnings("unchecked")
    private List<PerMinuteStatsResDTO> getCachedLast3HoursStats(Long tenantId) {
        String key = last3HoursKey(tenantId);
        Cache mediaStatsCache = cacheManager.getCache("mediaStats");

        Object cached = mediaStatsCache != null ? mediaStatsCache.get(key, Object.class) : null;

        if (cached == null) {
            cached = redisTemplate.opsForValue().get(key);
        }

        return convertCachedStats(cached);
    }

    private List<PerMinuteStatsResDTO> convertCachedStats(Object cached) {
        if (cached == null) {
            return null;
        }

        if (cached instanceof List<?> list) {
            if (list.isEmpty() || list.get(0) instanceof PerMinuteStatsResDTO) {
                return (List<PerMinuteStatsResDTO>) list;
            }

            return list.stream()
                    .map(this::convertCachedDto)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private List<DailyStatsResDTO> getCachedLast30DaysStats(Long tenantId, MediaType mediaType) {
        String key = last30DaysKey(tenantId, mediaType);
        Cache mediaStatsCache = cacheManager.getCache("mediaStats");

        Object cached = mediaStatsCache != null ? mediaStatsCache.get(key, Object.class) : null;

        if (cached == null) {
            cached = redisTemplate.opsForValue().get(key);
        }

        return convertCachedDailyStats(cached);
    }

    private void cacheLast30DaysStats(Long tenantId, MediaType mediaType, List<DailyStatsResDTO> stats) {
        String key = last30DaysKey(tenantId, mediaType);
        redisTemplate.opsForValue().set(key, stats);
        Cache mediaStatsCache = cacheManager.getCache("mediaStats");
        if (mediaStatsCache != null) {
            mediaStatsCache.put(key, stats);
        }
    }

    private List<DailyStatsResDTO> convertCachedDailyStats(Object cached) {
        if (cached == null) {
            return null;
        }

        if (cached instanceof List<?> list) {
            if (list.isEmpty() || list.get(0) instanceof DailyStatsResDTO) {
                return (List<DailyStatsResDTO>) list;
            }

            return list.stream()
                    .map(this::convertCachedDailyDto)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        return null;
    }

    private DailyStatsResDTO convertCachedDailyDto(Object obj) {
        if (!(obj instanceof Map<?, ?> map)) {
            return null;
        }

        Object devicesObj = map.get("deviceMetrics");
        List<DeviceMetrics> devices = null;
        if (devicesObj instanceof List<?> rawDevices) {
            devices = rawDevices.stream()
                    .filter(item -> item instanceof Map<?, ?>)
                    .map(item -> (Map<?, ?>) item)
                    .map(this::toDeviceMetrics)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        Integer year = (Integer) map.get("year");
        Integer month = (Integer) map.get("month");
        Integer day = (Integer) map.get("day");
        String date = (String) map.get("date");

        Long uniqueViewers = null;
        Object uniqueViewersObj = map.get("uniqueViewers");
        if (uniqueViewersObj instanceof Number num) {
            uniqueViewers = num.longValue();
        }

        Long totalWatchTime = null;
        Object totalWatchTimeObj = map.get("totalWatchTime");
        if (totalWatchTimeObj instanceof Number num) {
            totalWatchTime = num.longValue();
        }

        if (year == null || month == null || day == null || date == null || uniqueViewers == null || totalWatchTime == null) {
            return null;
        }

        return new DailyStatsResDTO(
                year,
                month,
                day,
                date,
                uniqueViewers,
                totalWatchTime,
                devices
        );
    }

    private PerMinuteStatsResDTO convertCachedDto(Object obj) {
        if (!(obj instanceof Map<?, ?> map)) {
            return null;
        }

        Object devicesObj = map.get("deviceMetrics");
        List<DeviceMetrics> devices = null;
        if (devicesObj instanceof List<?> rawDevices) {
            devices = rawDevices.stream()
                    .filter(item -> item instanceof Map<?, ?>)
                    .map(item -> (Map<?, ?>) item)
                    .map(this::toDeviceMetrics)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        Integer year = (Integer) map.get("year");
        Integer month = (Integer) map.get("month");
        Integer day = (Integer) map.get("day");
        Integer hour = (Integer) map.get("hour");
        Integer minute = (Integer) map.get("minute");
        Integer uniqueUserCount = (Integer) map.get("uniqueUserCount");

        if (year == null || month == null || day == null || hour == null || minute == null || uniqueUserCount == null) {
            return null;
        }

        return new PerMinuteStatsResDTO(
                year,
                month,
                day,
                hour,
                minute,
                uniqueUserCount,
                devices
        );
    }

    private DeviceMetrics toDeviceMetrics(Map<?, ?> map) {
        Object deviceTypeObj = map.get("deviceType");
        DeviceType deviceType = null;
        if (deviceTypeObj instanceof String typeStr) {
            try {
                deviceType = DeviceType.valueOf(typeStr);
            } catch (IllegalArgumentException ex) {
            }
        }

        Long uniqueViewers = null;
        Object uniqueViewersObj = map.get("uniqueViewers");
        if (uniqueViewersObj instanceof Number num) {
            uniqueViewers = num.longValue();
        }

        Long watchTime = null;
        Object watchTimeObj = map.get("watchTime");
        if (watchTimeObj instanceof Number num) {
            watchTime = num.longValue();
        }

        if (uniqueViewers == null || watchTime == null) {
            return null;
        }

        return new DeviceMetrics(deviceType, uniqueViewers, watchTime);
    }

    private List<PerMinuteStatsResDTO> mergeWithCachedStats(List<PerMinuteStatsResDTO> fresh,
                                                            List<PerMinuteStatsResDTO> cached) {
        LocalDateTime nowDateTime = LocalDateTime.now(SRI_LANKA_ZONE).truncatedTo(ChronoUnit.MINUTES);
        LocalDateTime earliest = nowDateTime.minusHours(3);

        List<PerMinuteStatsResDTO> cachedWithinRange = filterStatsWithinRange(cached, earliest, nowDateTime);

        if (cachedWithinRange.isEmpty()) {
            return fresh;
        }

        Map<LocalDateTime, PerMinuteStatsResDTO> merged = new HashMap<>();
        cachedWithinRange.forEach(dto -> merged.put(toDateTime(dto), dto));

        for (PerMinuteStatsResDTO dto : fresh) {
            LocalDateTime key = toDateTime(dto);
            PerMinuteStatsResDTO existing = merged.get(key);
            if (existing == null) {
                merged.put(key, dto);
                continue;
            }

            Map<DeviceType, Long> mergedDeviceCounts = new HashMap<>();
            addDeviceMetrics(existing, mergedDeviceCounts);
            addDeviceMetrics(dto, mergedDeviceCounts);

            List<DeviceMetrics> mergedDevices = mergedDeviceCounts.entrySet().stream()
                    .map(e -> new DeviceMetrics(e.getKey(), e.getValue(), 0L))
                    .sorted(Comparator.comparing(e -> e.getDeviceType() != null ? e.getDeviceType().name() : ""))
                    .collect(Collectors.toList());

            int mergedTotal = mergedDevices.stream()
                    .mapToInt(dm -> (int) dm.getUniqueViewers())
                    .sum();

            mergedTotal = Math.max(mergedTotal,
                    Math.max(existing.getUniqueUserCount(), dto.getUniqueUserCount()));

            merged.put(key, new PerMinuteStatsResDTO(
                    key.getYear(),
                    key.getMonthValue(),
                    key.getDayOfMonth(),
                    key.getHour(),
                    key.getMinute(),
                    mergedTotal,
                    mergedDevices
            ));
        }

        return merged.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    private void addDeviceMetrics(PerMinuteStatsResDTO dto, Map<DeviceType, Long> mergedDeviceCounts) {
        if (dto.getDeviceMetrics() == null) {
            return;
        }

        for (DeviceMetrics dm : dto.getDeviceMetrics()) {
            mergedDeviceCounts.merge(dm.getDeviceType(), dm.getUniqueViewers(), Math::max);
        }
    }

    private List<PerMinuteStatsResDTO> filterStatsWithinRange(List<PerMinuteStatsResDTO> stats,
                                                             LocalDateTime fromInclusive,
                                                             LocalDateTime toInclusive) {
        if (stats == null || stats.isEmpty()) {
            return Collections.emptyList();
        }

        return stats.stream()
                .filter(Objects::nonNull)
                .filter(dto -> isWithinRange(dto, fromInclusive, toInclusive))
                .collect(Collectors.toList());
    }

    private boolean isWithinRange(PerMinuteStatsResDTO dto, LocalDateTime fromInclusive, LocalDateTime toInclusive) {
        LocalDateTime dateTime = toDateTime(dto);
        return !dateTime.isBefore(fromInclusive) && !dateTime.isAfter(toInclusive);
    }

    private LocalDateTime toDateTime(PerMinuteStatsResDTO dto) {
        return LocalDateTime.of(dto.getYear(), dto.getMonth(), dto.getDay(), dto.getHour(), dto.getMinute());
    }

    private String last3HoursKey(Long tenantId) {
        return "last3h:" + tenantId;
    }

    private List<PerMinuteStatsResDTO> excludeRecentMinutes(List<PerMinuteStatsResDTO> stats, long minutes) {
        if (stats == null || stats.isEmpty()) {
            return Collections.emptyList();
        }

        LocalDateTime cutoff = LocalDateTime.now(SRI_LANKA_ZONE)
                .truncatedTo(ChronoUnit.MINUTES)
                .minusMinutes(minutes);

        return stats.stream()
                .filter(Objects::nonNull)
                .filter(dto -> toDateTime(dto).isBefore(cutoff))
                .collect(Collectors.toList());
    }

    private String last30DaysKey(Long tenantId, MediaType mediaType) {
        return "last30d:" + tenantId + ":" + mediaType.name();
    }

    public List<DailyStatsResDTO> getLast30DaysStatsForMediaType(
            Long tenantId,
            MediaType mediaType
    ) {
        List<DailyStatsResDTO> cached = getCachedLast30DaysStats(tenantId, mediaType);
        if (cached != null && !cached.isEmpty()) {
            return cached;
        }

        OffsetDateTime now = OffsetDateTime.now(SRI_LANKA_ZONE);
        OffsetDateTime startDateTime = now.minus(30, ChronoUnit.DAYS);

        List<MediaWatchSession> sessions = sessionRepo
                .findByTenantIdAndMediaTypeAndWatchedAtBetween(tenantId, mediaType, startDateTime, now);

        Map<LocalDate, List<MediaWatchSession>> byDate = sessions.stream()
                .collect(Collectors.groupingBy(s ->
                        toSriLankaLocalDateTime(s.getWatchedAt()).toLocalDate()));

        List<DailyStatsResDTO> result = new ArrayList<>(30);
        LocalDate today = LocalDate.now(SRI_LANKA_ZONE);
        LocalDate date = today.minusDays(29);
        while (!date.isAfter(today)) {
            List<MediaWatchSession> daySessions = byDate.getOrDefault(date, Collections.emptyList());

            long totalWatchTime = daySessions.stream().mapToLong(MediaWatchSession::getWatchTime).sum();
            long uniqueViewers = daySessions.stream().map(MediaWatchSession::getUserId).distinct().count();

            long mobileUV = daySessions.stream().filter(s -> s.getDeviceType() == DeviceType.MOBILE)
                    .map(MediaWatchSession::getUserId).distinct().count();
            long mobileWT = daySessions.stream().filter(s -> s.getDeviceType() == DeviceType.MOBILE)
                    .mapToLong(MediaWatchSession::getWatchTime).sum();

            long desktopUV = daySessions.stream().filter(s -> s.getDeviceType() == DeviceType.DESKTOP)
                    .map(MediaWatchSession::getUserId).distinct().count();
            long desktopWT = daySessions.stream().filter(s -> s.getDeviceType() == DeviceType.DESKTOP)
                    .mapToLong(MediaWatchSession::getWatchTime).sum();

            long otherUV = daySessions.stream()
                    .filter(s -> s.getDeviceType() == DeviceType.TABLET || s.getDeviceType() == DeviceType.TV)
                    .map(MediaWatchSession::getUserId).distinct().count();
            long otherWT = daySessions.stream()
                    .filter(s -> s.getDeviceType() == DeviceType.TABLET || s.getDeviceType() == DeviceType.TV)
                    .mapToLong(MediaWatchSession::getWatchTime).sum();

            List<DeviceMetrics> metrics = Arrays.asList(
                    new DeviceMetrics(DeviceType.MOBILE, mobileUV, mobileWT),
                    new DeviceMetrics(DeviceType.DESKTOP, desktopUV, desktopWT),
                    new DeviceMetrics(null, otherUV, otherWT)
            );

            result.add(new DailyStatsResDTO(
                    date.getYear(),
                    date.getMonthValue(),
                    date.getDayOfMonth(),
                    date.toString(),
                    uniqueViewers,
                    totalWatchTime,
                    metrics
            ));

            date = date.plusDays(1);
        }

        cacheLast30DaysStats(tenantId, mediaType, result);
        return result;
    }

}
