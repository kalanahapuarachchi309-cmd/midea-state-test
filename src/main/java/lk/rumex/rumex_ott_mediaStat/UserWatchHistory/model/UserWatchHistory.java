package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.model;

import jakarta.persistence.*;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "user_watch_history")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserWatchHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long userId;

    private Long mediaId;

    private MediaType mediaType;

    private String slug;

    private Long lastWatchPosition = 0L;

    private Long totalDuration;

    private Boolean isCompleted = false;

    private Instant updatedAt;
}
