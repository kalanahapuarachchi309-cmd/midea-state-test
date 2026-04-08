package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MonthWiseStatsDTO {
    private Integer year;
    private Integer month;
    private Long totalWatchTime;
    private Long distinctUsers;
}
