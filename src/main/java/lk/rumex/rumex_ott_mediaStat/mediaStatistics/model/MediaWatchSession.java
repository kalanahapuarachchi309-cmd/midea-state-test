package lk.rumex.rumex_ott_mediaStat.mediaStatistics.model;

import jakarta.persistence.*;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.converter.UserStatusConverter;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.DeviceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.InterfaceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.UserStatus;
import lombok.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "media_watch_session", indexes = {
        @Index(name = "idx_mws_tenant_owner_watched", columnList = "tenantId, accountOwnerId, watchedAt"),
        @Index(name = "idx_mws_tenant_media_watched", columnList = "tenantId, mediaType, mediaId, watchedAt")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MediaWatchSession {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long tenantId;
    private Long mediaId;
    @Enumerated(EnumType.STRING)
    private MediaType mediaType;

    private String slug;
    @Enumerated(EnumType.STRING)
    private DeviceType deviceType;
    @Enumerated(EnumType.STRING)
    private InterfaceType interfaceType;
    private Long userId;
    private Long accountOwnerId;
    private String deviceId;
    private Long watchTime;
    private OffsetDateTime watchedAt;
}
