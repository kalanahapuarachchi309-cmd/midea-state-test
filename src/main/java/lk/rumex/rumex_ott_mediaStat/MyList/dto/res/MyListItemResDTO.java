package lk.rumex.rumex_ott_mediaStat.MyList.dto.res;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MyListItemResDTO {
    private String id;
    private Long userId;
    private Long mediaId;
    private String mediaType;
    private String slug;
    private Date createdAt;
}
