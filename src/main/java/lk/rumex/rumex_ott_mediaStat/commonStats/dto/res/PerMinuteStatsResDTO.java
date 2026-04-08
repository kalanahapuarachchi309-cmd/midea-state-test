package lk.rumex.rumex_ott_mediaStat.commonStats.dto.res;

import lk.rumex.rumex_ott_mediaStat.commonStats.dto.dto.DeviceMetrics;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PerMinuteStatsResDTO {
    private int year;
    private int month;
    private int day;
    private int hour;
    private int minute;
    private int uniqueUserCount;
    private List<DeviceMetrics> deviceMetrics;
}