package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.repository;

import lk.rumex.ott_domain_models.movies.model.Movie;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MovieRepository extends JpaRepository<Movie, Long> {
}
