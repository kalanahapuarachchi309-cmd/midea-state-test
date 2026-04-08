package lk.rumex.rumex_ott_mediaStat.mediaStatistics.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Convert;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.ott_domain_models.shared.Enum.MaturityRating;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.UserStatus;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.converter.UserStatusConverter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Entity
@Table(name = "media_daily_watch_summary_v2", uniqueConstraints = @UniqueConstraint(columnNames = {
                "tenantId",
                "mediaId",
                "mediaType",
                "maturityRating"
}))
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MediaDailyWatchSummary {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        private Long tenantId;
        private Long mediaId;
        @Enumerated(EnumType.STRING)
        private MediaType mediaType;
        @Enumerated(EnumType.STRING)
        private MaturityRating maturityRating;
        private LocalDate watchedDate;
        private Long totalWatchTime;
}
