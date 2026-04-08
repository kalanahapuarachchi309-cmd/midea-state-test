package lk.rumex.rumex_ott_mediaStat.mediaStatistics.mapper;

import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.req.MediaWatchSessionDTO;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.MediaWatchSession;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface MediaWatchSessionMapper {
    @Mapping(target = "mediaId",source = "mediaId")
    MediaWatchSession toEntity(MediaWatchSessionDTO dto);
}
