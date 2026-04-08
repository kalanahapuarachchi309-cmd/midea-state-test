package lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository;

import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.SummaryRebuildProgress;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SummaryRebuildProgressRepository extends JpaRepository<SummaryRebuildProgress, Long> {
    Optional<SummaryRebuildProgress> findByJobName(String jobName);
}
