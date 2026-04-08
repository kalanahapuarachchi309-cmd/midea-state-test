package lk.rumex.rumex_ott_mediaStat.MyList.dto.req;

import jakarta.validation.constraints.NotNull;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MyListItemDTO {
    @NotNull(message = "User ID must not be null")
    private Long userId;

    @NotNull(message = "Media ID must not be null")
    private Long mediaId;

    @NotNull(message = "Media type must not be null")
    private MediaType mediaType;

    @NotNull(message = "Media slug must not be null")
    private String slug;
}
