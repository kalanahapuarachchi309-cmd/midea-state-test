package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.service;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.ott_domain_models.episodes.model.Episodes;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.req.UserWatchHistoryDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.res.EpisodeWatchPositionResDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.res.UserWatchHistoryResDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.mapper.UserWatchHistoryMapper;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.model.UserWatchHistory;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.repository.EpisodeRepository;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.repository.UserWatchHistoryRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UserWatchHistoryService {

    private static final ZoneId SRI_LANKA_ZONE = ZoneId.of("Asia/Colombo");
    private static final int DB_BATCH_SIZE = 1000;
    private static final String PENDING_HISTORY_KEY = "buffer:userWatchHistory:active";
    private static final String PENDING_HISTORY_PROCESSING_KEY = "buffer:userWatchHistory:processing";

    @Autowired
    private UserWatchHistoryRepository repository;

    @Autowired
    private UserWatchHistoryMapper mapper;

    @Autowired
    private EpisodeRepository episodeRepository;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    public UserWatchHistoryResDTO createOrUpdateUserWatchHistory(UserWatchHistoryDTO reqDTO) {
        if (reqDTO.getMediaType().equals(MediaType.LIVE_CHANNEL)){
            return null;
        }
        String key = buildKey(reqDTO.getUserId(), reqDTO.getMediaId(), reqDTO.getMediaType());

        UserWatchHistory history = getPendingHistory(key);

        if (history == null) {
            history = repository
                    .findByUserIdAndMediaIdAndMediaType(
                            reqDTO.getUserId(),
                            reqDTO.getMediaId(),
                            reqDTO.getMediaType()
                    )
                    .orElse(null);
            if (history != null) {
                mapper.updateEntityFromDto(reqDTO, history);
            } else {
                history = mapper.toEntity(reqDTO);
            }
        } else {
            mapper.updateEntityFromDto(reqDTO, history);
        }

        if (history.getLastWatchPosition() >= history.getTotalDuration()) {
            history.setIsCompleted(true);
        }
        history.setUpdatedAt(ZonedDateTime.now(SRI_LANKA_ZONE).toInstant());

        putPendingHistory(key, history);
        evictWatchHistoryCaches(reqDTO.getUserId(), key);
        return mapper.toResponse(history);
    }

    public List<UserWatchHistoryResDTO> getAllUserWatchHistory(Long userId) {
        return getOrCacheList("watchHistoryAll", userId, () -> buildCollapsedResponses(userId, h -> true));
    }

    public List<UserWatchHistoryResDTO> getCompletedWatchHistory(Long userId) {
        return getOrCacheList("watchHistoryCompleted", userId,
                () -> buildCollapsedResponses(userId, h -> Boolean.TRUE.equals(h.getIsCompleted())));
    }

    public List<UserWatchHistoryResDTO> getIncompleteWatchHistory(Long userId) {
        return getOrCacheList("watchHistoryIncomplete", userId,
                () -> {
                    List<UserWatchHistoryResDTO> results =
                            buildCollapsedResponses(userId, h -> Boolean.FALSE.equals(h.getIsCompleted()));
                    results.forEach(dto -> dto.setUpdatedAt(null));
                    return results;
                });
    }

    public List<EpisodeWatchPositionResDTO> getTvShowEpisodePositions(Long userId, Long tvShowId) {
        List<Episodes> episodes = episodeRepository.findByTvShowId(tvShowId);
        if (episodes.isEmpty()) return Collections.emptyList();

        Set<Long> episodeIds = episodes.stream()
                .map(Episodes::getId)
                .collect(Collectors.toSet());

        List<UserWatchHistory> merged = mergeAndSort(userId, h ->
                h.getMediaType() == MediaType.TV_EPISODE && episodeIds.contains(h.getMediaId()));

        List<UserWatchHistory> deduped = collapseLatestPerMedia(merged);

        return deduped.stream()
                .map(history -> toEpisodeWatchPositionResponse(tvShowId, history))
                .sorted(Comparator.comparing(EpisodeWatchPositionResDTO::getUpdatedAt,
                                Comparator.nullsLast(Comparator.naturalOrder()))
                        .reversed())
                .toList();
    }

    /**
     * Collapse TV_EPISODE items so only the latest (by updatedAt) per tvShowId remains.
     * Others pass through unchanged.
     */
    private List<UserWatchHistory> collapseLatestEpisodePerShow(List<UserWatchHistory> sortedDesc) {
        if (sortedDesc.isEmpty()) return sortedDesc;

        Set<Long> episodeIds = sortedDesc.stream()
                .filter(h -> h.getMediaType() == MediaType.TV_EPISODE)
                .map(UserWatchHistory::getMediaId)
                .collect(Collectors.toSet());

        Map<Long, Long> episodeIdToShowId = loadTvShowIds(episodeIds);

        List<UserWatchHistory> result = new ArrayList<>();
        Set<Long> seenShows = new HashSet<>();

        for (UserWatchHistory history : sortedDesc) {
            Long tvShowId = resolveTvShowId(history, episodeIdToShowId);
            if (tvShowId == null) {
                result.add(history);
                continue;
            }

            if (seenShows.add(tvShowId)) {
                result.add(history);
            }
        }

        return result;
    }

    private Long resolveTvShowId(UserWatchHistory history, Map<Long, Long> episodeIdToShowId) {
        if (history.getMediaType() == MediaType.TV_SHOW) {
            return history.getMediaId();
        }

        if (history.getMediaType() == MediaType.TV_EPISODE) {
            return episodeIdToShowId.get(history.getMediaId());
        }

        return null;
    }

    private List<UserWatchHistoryResDTO> mapCollapsedToResponses(List<UserWatchHistory> histories) {
        Set<Long> episodeIds = histories.stream()
                .filter(h -> h.getMediaType() == MediaType.TV_EPISODE)
                .map(UserWatchHistory::getMediaId)
                .collect(Collectors.toSet());

        Map<Long, Long> episodeIdToShowId = loadTvShowIds(episodeIds);

        return histories.stream()
                .map(h -> {
                    Long tvShowId = episodeIdToShowId.get(h.getMediaId());
                    if (tvShowId == null) {
                        UserWatchHistoryResDTO dto = mapper.toResponse(h);
                        dto.setLastWatchedEpisodeId(h.getMediaType() == MediaType.TV_EPISODE ? h.getMediaId() : null);
                        return dto;
                    }
                    return toTvShowResponse(h, tvShowId);
                })
                .sorted(Comparator.comparing(UserWatchHistoryResDTO::getUpdatedAt,
                                Comparator.nullsLast(Comparator.naturalOrder()))
                        .reversed())
                .toList();
    }

    private UserWatchHistoryResDTO toTvShowResponse(UserWatchHistory history, Long tvShowId) {
        UserWatchHistoryResDTO dto = mapper.toResponse(history);
        dto.setTvShowId(tvShowId);
        dto.setMediaId(tvShowId);
        dto.setMediaType("TV_SHOW");
        dto.setLastWatchedEpisodeId(history.getMediaId());
        return dto;
    }

    private EpisodeWatchPositionResDTO toEpisodeWatchPositionResponse(Long tvShowId, UserWatchHistory history) {
        return new EpisodeWatchPositionResDTO(
                tvShowId,
                history.getMediaId(),
                history.getLastWatchPosition(),
                history.getTotalDuration(),
                history.getIsCompleted(),
                history.getUpdatedAt() != null ? Date.from(history.getUpdatedAt()) : null
        );
    }

    private Map<Long, Long> loadTvShowIds(Set<Long> episodeIds) {
        if (episodeIds.isEmpty()) return Collections.emptyMap();

        return episodeRepository.findAllById(episodeIds)
                .stream()
                .collect(Collectors.toMap(
                        Episodes::getId,
                        Episodes::getTvShowId,
                        (a, b) -> a));
    }


    public UserWatchHistoryResDTO getWatchHistory(Long userId, Long mediaId, MediaType mediaType) {
        String key = buildKey(userId, mediaId, mediaType);
        return getOrCacheSingle("watchHistory", key, () -> {
            UserWatchHistory history = getPendingHistory(key);
            if (history == null) {
                history = repository
                        .findByUserIdAndMediaIdAndMediaType(userId, mediaId, mediaType)
                        .orElseThrow(() -> new ResponseStatusException(
                                HttpStatus.NOT_FOUND,
                                "No watch history found for this media."));
            }
            return mapper.toResponse(history);
        });
    }

    public void removeIncompleteWatchHistory(Long userId, Long mediaId, MediaType mediaType) {
        String key = buildKey(userId, mediaId, mediaType);

        UserWatchHistory pending = getPendingHistory(key);
        if (pending != null && Boolean.FALSE.equals(pending.getIsCompleted())) {
            removePendingHistory(key);
        }

        repository.findByUserIdAndMediaIdAndMediaType(userId, mediaId, mediaType)
                .filter(history -> Boolean.FALSE.equals(history.getIsCompleted()))
                .ifPresent(repository::delete);
        evictWatchHistoryCaches(userId, key);
    }

    private List<UserWatchHistoryResDTO> buildCollapsedResponses(Long userId, Predicate<UserWatchHistory> filter) {
        List<UserWatchHistory> merged = mergeAndSort(userId, filter);

        List<UserWatchHistory> tvCollapsed = collapseLatestEpisodePerShow(merged);
        List<UserWatchHistory> deduped = collapseLatestPerMedia(tvCollapsed);

        return mapCollapsedToResponses(deduped);
    }

    /** Persist all cached watch histories to the database and clear the cache. */
    public void flushCacheToDb() {
        if (!promotePendingHistoryForFlush()) {
            return;
        }

        Map<Object, Object> pendingEntries = readPendingHistoryEntries(PENDING_HISTORY_PROCESSING_KEY);
        if (pendingEntries.isEmpty()) {
            redisTemplate.delete(PENDING_HISTORY_PROCESSING_KEY);
            return;
        }

        for (Map.Entry<Object, Object> entry : pendingEntries.entrySet()) {
            String fieldKey = entry.getKey() instanceof String key ? key : null;
            if (fieldKey == null || !(entry.getValue() instanceof UserWatchHistory history)) {
                if (fieldKey != null) {
                    redisTemplate.opsForHash().delete(PENDING_HISTORY_PROCESSING_KEY, fieldKey);
                }
                continue;
            }

            if (persistPendingHistory(fieldKey, history)) {
                redisTemplate.opsForHash().delete(PENDING_HISTORY_PROCESSING_KEY, fieldKey);
            }
        }

        if (Boolean.FALSE.equals(redisTemplate.hasKey(PENDING_HISTORY_PROCESSING_KEY))) {
            return;
        }

        Long remaining = redisTemplate.opsForHash().size(PENDING_HISTORY_PROCESSING_KEY);
        if (remaining == null || remaining == 0L) {
            redisTemplate.delete(PENDING_HISTORY_PROCESSING_KEY);
        }
    }

    private boolean promotePendingHistoryForFlush() {
        if (Boolean.TRUE.equals(redisTemplate.hasKey(PENDING_HISTORY_PROCESSING_KEY))) {
            return true;
        }

        if (!Boolean.TRUE.equals(redisTemplate.hasKey(PENDING_HISTORY_KEY))) {
            return false;
        }

        redisTemplate.rename(PENDING_HISTORY_KEY, PENDING_HISTORY_PROCESSING_KEY);
        return true;
    }

    private UserWatchHistory getPendingHistory(String fieldKey) {
        Object active = redisTemplate.opsForHash().get(PENDING_HISTORY_KEY, fieldKey);
        if (active instanceof UserWatchHistory history) {
            return history;
        }

        Object processing = redisTemplate.opsForHash().get(PENDING_HISTORY_PROCESSING_KEY, fieldKey);
        if (processing instanceof UserWatchHistory history) {
            return history;
        }

        return null;
    }

    private void putPendingHistory(String fieldKey, UserWatchHistory history) {
        redisTemplate.opsForHash().put(PENDING_HISTORY_KEY, fieldKey, history);
    }

    private void removePendingHistory(String fieldKey) {
        redisTemplate.opsForHash().delete(PENDING_HISTORY_KEY, fieldKey);
        redisTemplate.opsForHash().delete(PENDING_HISTORY_PROCESSING_KEY, fieldKey);
    }

    private List<UserWatchHistory> getAllPendingHistory() {
        List<UserWatchHistory> histories = new ArrayList<>();
        histories.addAll(readPendingHistoryValues(PENDING_HISTORY_KEY));
        histories.addAll(readPendingHistoryValues(PENDING_HISTORY_PROCESSING_KEY));
        return histories;
    }

    private List<UserWatchHistory> readPendingHistoryValues(String hashKey) {
        List<Object> rawValues = redisTemplate.opsForHash().values(hashKey);
        if (rawValues == null || rawValues.isEmpty()) {
            return Collections.emptyList();
        }

        List<UserWatchHistory> histories = new ArrayList<>(rawValues.size());
        for (Object value : rawValues) {
            if (value instanceof UserWatchHistory history) {
                histories.add(history);
            }
        }
        return histories;
    }

    private Map<Object, Object> readPendingHistoryEntries(String hashKey) {
        Map<Object, Object> entries = redisTemplate.opsForHash().entries(hashKey);
        return entries == null ? Collections.emptyMap() : entries;
    }

    private boolean persistPendingHistory(String fieldKey, UserWatchHistory history) {
        attachExistingId(history);

        try {
            repository.save(history);
            return true;
        } catch (DataIntegrityViolationException ex) {
            attachExistingId(history);
            if (history.getId() == null) {
                log.error("Failed to persist watch history {}; unique key still unresolved", fieldKey, ex);
                return false;
            }

            try {
                repository.save(history);
                return true;
            } catch (DataIntegrityViolationException retryEx) {
                log.error("Failed to persist watch history {} after retry", fieldKey, retryEx);
                return false;
            }
        }
    }

    private void attachExistingId(UserWatchHistory history) {
        repository.findByUserIdAndMediaIdAndMediaType(
                        history.getUserId(),
                        history.getMediaId(),
                        history.getMediaType())
                .map(UserWatchHistory::getId)
                .ifPresent(history::setId);
    }

    private String buildKey(Long userId, Long mediaId, MediaType mediaType) {
        return userId + ":" + mediaId + ":" + mediaType;
    }

    private void evictWatchHistoryCaches(Long userId, String mediaKey) {
        evictCache("watchHistoryAll", userId);
        evictCache("watchHistoryCompleted", userId);
        evictCache("watchHistoryIncomplete", userId);
        evictCache("watchHistory", mediaKey);
    }

    private void evictCache(String cacheName, Object key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache != null) {
            cache.evict(key);
        }
    }

    private <T> T getCachedValue(String cacheName, Object key, Class<T> type) {
        Cache cache = cacheManager.getCache(cacheName);
        return cache != null ? cache.get(key, type) : null;
    }

    private void cacheValue(String cacheName, Object key, Object value) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache != null) {
            cache.put(key, value);
        }
    }

    private List<UserWatchHistoryResDTO> getOrCacheList(String cacheName, Long userId,
                                                        Supplier<List<UserWatchHistoryResDTO>> loader) {
        @SuppressWarnings("unchecked")
        List<UserWatchHistoryResDTO> cached =
                (List<UserWatchHistoryResDTO>) getCachedValue(cacheName, userId, List.class);
        if (cached != null) {
            return cached;
        }
        List<UserWatchHistoryResDTO> results = loader.get();
        cacheValue(cacheName, userId, results);
        return results;
    }

    private UserWatchHistoryResDTO getOrCacheSingle(String cacheName, String key,
                                                    Supplier<UserWatchHistoryResDTO> loader) {
        UserWatchHistoryResDTO cached = getCachedValue(cacheName, key, UserWatchHistoryResDTO.class);
        if (cached != null) {
            return cached;
        }
        UserWatchHistoryResDTO result = loader.get();
        cacheValue(cacheName, key, result);
        return result;
    }

    /* ---------------------- helpers: merge/sort + dedupe ---------------------- */

    /** Merge DB + pending for the user, filter by predicate, sort by updatedAt DESC (nulls last). */
    private List<UserWatchHistory> mergeAndSort(Long userId, Predicate<UserWatchHistory> filter) {
        List<UserWatchHistory> list = new ArrayList<>();

        // DB (get all for user once; apply filter here to be consistent with pending)
        list.addAll(
                repository.findByUserIdOrderByUpdatedAtDesc(userId)
                        .stream()
                        .filter(filter)
                        .toList()
        );

        // Pending overlay
        getAllPendingHistory().stream()
                .filter(h -> h.getUserId().equals(userId))
                .filter(filter)
                .forEach(list::add);

        // Global sort so "latest wins" works across sources
        list.sort(Comparator.comparing(UserWatchHistory::getUpdatedAt,
                        Comparator.nullsLast(Comparator.naturalOrder()))
                .reversed());

        return list;
    }

    /** Keep only the latest entry per (mediaType, mediaId) for the given user. Assumes input sorted DESC by updatedAt. */
    private List<UserWatchHistory> collapseLatestPerMedia(List<UserWatchHistory> sortedDesc) {
        Map<String, UserWatchHistory> firstSeen = new LinkedHashMap<>();
        for (UserWatchHistory h : sortedDesc) {
            String k = h.getMediaType() + ":" + h.getMediaId();
            // because sorted DESC, the first time we see the media key is the latest
            firstSeen.putIfAbsent(k, h);
        }
        return new ArrayList<>(firstSeen.values());
    }
}
