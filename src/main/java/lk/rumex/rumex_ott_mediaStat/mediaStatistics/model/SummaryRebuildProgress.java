package lk.rumex.rumex_ott_mediaStat.mediaStatistics.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.OffsetDateTime;

@Entity
@Table(name = "summary_rebuild_progress", uniqueConstraints = @UniqueConstraint(columnNames = "jobName"))
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SummaryRebuildProgress {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String jobName;
    private LocalDate startDate;
    private LocalDate targetDate;
    private LocalDate lastCompletedDate;
    private Long tenantId;
    private String status;
    private Integer lastProcessedSessions;
    private Integer lastUpdatedSummaries;
    private String lastMessage;
    private OffsetDateTime lastRunAt;
}
