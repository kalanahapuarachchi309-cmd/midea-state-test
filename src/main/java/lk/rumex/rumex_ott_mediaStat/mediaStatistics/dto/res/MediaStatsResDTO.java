package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.DeviceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.InterfaceType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MediaStatsResDTO {
    private Long tenantId;
    private Long mediaId;
    private MediaType mediaType;
    private Long totalWatchTime;
    private Long uniqueUserCount;
    private String title;
    private String slug;
    private DeviceType deviceType;
    private InterfaceType interfaceType;

    public MediaStatsResDTO(Long tenantId, Long mediaId, MediaType mediaType, Long totalWatchTime, Long uniqueUserCount, String title, String slug) {
        this.tenantId = tenantId;
        this.mediaId = mediaId;
        this.mediaType = mediaType;
        this.totalWatchTime = totalWatchTime;
        this.uniqueUserCount = uniqueUserCount;
        this.title = title;
        this.slug = slug;
    }

    public MediaStatsResDTO(Long tenantId, MediaType mediaType, Long totalWatchTime, Long uniqueUserCount) {
        this.tenantId = tenantId;
        this.mediaType = mediaType;
        this.totalWatchTime = totalWatchTime;
        this.uniqueUserCount = uniqueUserCount;
    }

    public MediaStatsResDTO(Long tenantId, DeviceType deviceType, Long totalWatchTime, Long uniqueUserCount) {
        this.tenantId = tenantId;
        this.deviceType = deviceType;
        this.totalWatchTime = totalWatchTime;
        this.uniqueUserCount = uniqueUserCount;
    }

    public MediaStatsResDTO(Long tenantId, InterfaceType interfaceType, Long totalWatchTime, Long uniqueUserCount) {
        this.tenantId = tenantId;
        this.interfaceType = interfaceType;
        this.totalWatchTime = totalWatchTime;
        this.uniqueUserCount = uniqueUserCount;
    }

    public MediaStatsResDTO(Long tenantId, DeviceType deviceType, InterfaceType interfaceType, Long totalWatchTime, Long uniqueUserCount) {
        this.tenantId = tenantId;
        this.deviceType = deviceType;
        this.interfaceType = interfaceType;
        this.totalWatchTime = totalWatchTime;
        this.uniqueUserCount = uniqueUserCount;
    }
}
