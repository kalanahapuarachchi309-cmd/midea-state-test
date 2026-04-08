package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.req;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class UserWatchHistoryDTO {
    @NotNull(message = "User ID must not be null")
    private Long userId;

    @NotNull(message = "Media ID must not be null")
    private Long mediaId;

    @NotNull(message = "Media type must not be null")
    private MediaType mediaType;

    @NotNull(message = "Slug must not be null")
    private String slug;

    @NotNull(message = "Last watch position must not be null")
    @Min(value = 0, message = "Last watch position cannot be negative")
    private Long lastWatchPosition;

    @NotNull(message = "Total duration must not be null")
    @Min(value = 1, message = "Total duration must be at least 1 second")
    private Long totalDuration;

    private Boolean isCompleted = false;
}
