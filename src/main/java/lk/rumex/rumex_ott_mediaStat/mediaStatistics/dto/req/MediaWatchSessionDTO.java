package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.req;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.DeviceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.InterfaceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.UserStatus;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class MediaWatchSessionDTO {

    private Long tenantId;

    @NotNull(message = "mediaId must not be null")
    private Long mediaId;

    @NotNull(message = "mediaType must not be null")
    private MediaType mediaType;

    @NotNull(message = "slug must not be null")
    private String slug;

    @NotNull(message = "userId must not be null")
    private Long userId;

    private Long accountOwnerId;

    private String deviceId;

   /*
   @NotNull(message = "userStatus must not be null")
    private UserStatus userStatus;
    */

    @NotNull(message = "watchTime must not be null")
    @Min(value = 1, message = "watchTime must be at least 1 second")
    private Long watchTime;

    @NotNull(message = "deviceType must not be null")
    private DeviceType deviceType;

    @NotNull(message = "interfaceType must not be null")
    private InterfaceType interfaceType;
}
