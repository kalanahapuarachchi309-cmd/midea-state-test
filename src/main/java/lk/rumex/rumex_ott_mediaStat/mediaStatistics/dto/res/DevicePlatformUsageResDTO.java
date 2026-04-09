package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DevicePlatformUsageResDTO {
    private Long tenantId;
    private List<DeviceUsageItemResDto> deviceUsage;
    private List<DeviceUsageItemResDto> platformUsage;
}

