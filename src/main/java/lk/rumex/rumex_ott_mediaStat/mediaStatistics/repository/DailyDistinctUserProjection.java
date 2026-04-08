package lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository;

public interface DailyDistinctUserProjection {
    Integer getYear();

    Integer getMonth();

    Integer getDay();

    Long getUserId();
}
