package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.res;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserWatchHistoryResDTO {
    private String id;
    private Long userId;
    private Long mediaId;
    private String mediaType;
    private Long tvShowId;
    private Long lastWatchedEpisodeId;
    private String slug;
    private Long lastWatchPosition;
    private Long totalDuration;
    private Boolean isCompleted;
    private Date updatedAt;
}
