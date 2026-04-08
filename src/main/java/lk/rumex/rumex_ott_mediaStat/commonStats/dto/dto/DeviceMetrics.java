package lk.rumex.rumex_ott_mediaStat.commonStats.dto.dto;

import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.DeviceType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeviceMetrics {
    private DeviceType deviceType;
    private long uniqueViewers;
    private long watchTime;

    public DeviceMetrics(DeviceType deviceType, long uniqueViewers) {
        this.deviceType = deviceType;
        this.uniqueViewers = uniqueViewers;
    }
}
