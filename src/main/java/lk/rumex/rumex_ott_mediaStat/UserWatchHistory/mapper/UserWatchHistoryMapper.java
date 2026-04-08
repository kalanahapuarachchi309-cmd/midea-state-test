package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.mapper;

import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.req.UserWatchHistoryDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.res.UserWatchHistoryResDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.model.UserWatchHistory;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class UserWatchHistoryMapper {
    public UserWatchHistoryResDTO toResponse(UserWatchHistory entity) {
        if (entity == null) {
            return null;
        }

        UserWatchHistoryResDTO dto = new UserWatchHistoryResDTO();
        dto.setId(entity.getId() != null ? String.valueOf(entity.getId()) : null);
        dto.setUserId(entity.getUserId());
        dto.setMediaId(entity.getMediaId());
        dto.setMediaType(entity.getMediaType() != null ? entity.getMediaType().name() : null);
        dto.setTvShowId(null);
        dto.setLastWatchedEpisodeId(null);
        dto.setSlug(entity.getSlug());
        dto.setLastWatchPosition(entity.getLastWatchPosition());
        dto.setTotalDuration(entity.getTotalDuration());
        dto.setIsCompleted(entity.getIsCompleted());
        dto.setUpdatedAt(entity.getUpdatedAt() != null ? Date.from(entity.getUpdatedAt()) : null);
        return dto;
    }

    public UserWatchHistory toEntity(UserWatchHistoryDTO reqDTO) {
        if (reqDTO == null) {
            return null;
        }

        UserWatchHistory entity = new UserWatchHistory();
        updateEntityFromDto(reqDTO, entity);
        return entity;
    }

    public void updateEntityFromDto(UserWatchHistoryDTO reqDTO, UserWatchHistory entity) {
        if (reqDTO == null || entity == null) {
            return;
        }

        entity.setUserId(reqDTO.getUserId());
        entity.setMediaId(reqDTO.getMediaId());
        entity.setMediaType(reqDTO.getMediaType());
        entity.setSlug(reqDTO.getSlug());
        entity.setLastWatchPosition(reqDTO.getLastWatchPosition());
        entity.setTotalDuration(reqDTO.getTotalDuration());
        if (reqDTO.getIsCompleted() != null) {
            entity.setIsCompleted(reqDTO.getIsCompleted());
        }
    }
}
