package lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository;

public interface DailySessionAggregateProjection {
    Integer getYear();

    Integer getMonth();

    Integer getDay();

    Long getTotalWatchTime();

    Long getDistinctUsers();
}
