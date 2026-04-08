package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PeakUsageHourResDTO {
    private int hour;
    private long activeUsers;
    private long totalWatchTime;
    private long sessionCount;
}

