package lk.rumex.rumex_ott_mediaStat.mediaStatistics.model;

import jakarta.persistence.*;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lombok.*;

@Entity
@Table(name = "media_data")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MediaData {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long mediaId;
    @Enumerated(EnumType.STRING)
    private MediaType mediaType;
    private String slug;
    private String title;
}
