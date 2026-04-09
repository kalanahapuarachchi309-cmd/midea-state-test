package lk.rumex.rumex_ott_mediaStat.mediaStatistics.mapper;

import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.req.MediaWatchSessionDTO;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.MediaWatchSession;
import org.springframework.stereotype.Component;

@Component
public class MediaWatchSessionMapper {

    public MediaWatchSession toEntity(MediaWatchSessionDTO dto) {
        if (dto == null) {
            return null;
        }

        MediaWatchSession entity = new MediaWatchSession();
        entity.setTenantId(dto.getTenantId());
        entity.setMediaId(dto.getMediaId());
        entity.setMediaType(dto.getMediaType());
        entity.setSlug(dto.getSlug());
        entity.setDeviceType(dto.getDeviceType());
        entity.setInterfaceType(dto.getInterfaceType());
        entity.setUserId(dto.getUserId());
        entity.setAccountOwnerId(dto.getAccountOwnerId());
        entity.setDeviceId(dto.getDeviceId());
        entity.setWatchTime(dto.getWatchTime());
        return entity;
    }
}
