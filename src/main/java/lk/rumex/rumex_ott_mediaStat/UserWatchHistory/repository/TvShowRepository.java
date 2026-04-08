package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.repository;

import lk.rumex.ott_domain_models.tvShows.model.TvShows;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TvShowRepository extends JpaRepository<TvShows, Long> {
}
