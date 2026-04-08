package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopWatchedMediaResDTO {
    private Long tenantId;
    private Long mediaId;
    private MediaType mediaType;
    private Long totalWatchTime;
    private String slug;
    private String title;
}
