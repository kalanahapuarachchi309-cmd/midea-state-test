package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OverallStatsResDTO {
    private Long tenantId;
    private Long totalWatchTime;
    private Long distinctUsers;
}
