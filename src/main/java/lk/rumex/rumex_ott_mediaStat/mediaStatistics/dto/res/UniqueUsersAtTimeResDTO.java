package lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.Date;

@Data
@AllArgsConstructor
public class UniqueUsersAtTimeResDTO {
    private Long tenantId;
    private Long uniqueUserCount;
    private Date timePoint;
}
