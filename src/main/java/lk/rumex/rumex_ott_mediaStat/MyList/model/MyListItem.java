package lk.rumex.rumex_ott_mediaStat.MyList.model;

import jakarta.persistence.*;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;

@Entity
@Table(name = "my_list")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyListItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long userId;

    private Long mediaId;

    private MediaType mediaType;

    private String slug;

    @CreationTimestamp
    private Instant createdAt;
}
