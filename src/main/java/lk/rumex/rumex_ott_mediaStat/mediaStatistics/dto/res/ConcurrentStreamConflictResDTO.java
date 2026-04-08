package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res;

import lk.rumex.ott_domain_models.category.Enum.MediaType;

import java.time.OffsetDateTime;
import java.util.List;

public record ConcurrentStreamConflictResDTO(
        String errorCode,
        String message,
        Long accountOwnerId,
        Long requestedProfileId,
        Long requestedMediaId,
        MediaType requestedMediaType,
        String requestedTitle,
        int maxActiveStreams,
        long activeWindowSeconds,
        List<ActiveStreamResDTO> activeStreams) {

    public record ActiveStreamResDTO(
            Long profileId,
            String deviceId,
            Long mediaId,
            MediaType mediaType,
            String title,
            String slug,
            String deviceType,
            String interfaceType,
            OffsetDateTime lastSeenAt) {
    }
}
