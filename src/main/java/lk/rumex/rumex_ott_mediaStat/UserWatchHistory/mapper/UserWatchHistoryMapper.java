package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.mapper;

import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.req.UserWatchHistoryDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.res.UserWatchHistoryResDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.model.UserWatchHistory;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;

@Mapper(componentModel = "spring")
public interface UserWatchHistoryMapper {
    @Mapping(target = "tvShowId", ignore = true)
    @Mapping(target = "lastWatchedEpisodeId", ignore = true)
    UserWatchHistoryResDTO toResponse(UserWatchHistory entity);

    @Mapping(target = "totalDuration", source = "totalDuration")
    UserWatchHistory toEntity(UserWatchHistoryDTO reqDTO);

    void updateEntityFromDto(UserWatchHistoryDTO reqDTO, @MappingTarget UserWatchHistory entity);
}
