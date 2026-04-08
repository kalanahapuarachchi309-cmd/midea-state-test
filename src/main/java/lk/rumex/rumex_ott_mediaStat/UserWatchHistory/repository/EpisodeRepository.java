package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.repository;

import lk.rumex.ott_domain_models.episodes.model.Episodes;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface EpisodeRepository extends JpaRepository<Episodes, Long> {

    List<Episodes> findByTvShowId(Long tvShowId);
}
