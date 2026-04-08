package lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository;

public interface OverallTenantStatsProjection {
    Long getTotalWatchTime();

    Long getDistinctUsers();
}
