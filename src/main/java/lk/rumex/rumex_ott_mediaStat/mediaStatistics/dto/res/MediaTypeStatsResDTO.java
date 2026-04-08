package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MediaTypeStatsResDTO {
    private Long tenantId;
    private MediaType mediaType;
    private Long totalWatchTime;
    private Long distinctUserCount;
}
