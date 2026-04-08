package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.res;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EpisodeWatchPositionResDTO {
    private Long tvShowId;
    private Long episodeId;
    private Long lastWatchPosition;
    private Long totalDuration;
    private Boolean isCompleted;
    private Date updatedAt;
}
