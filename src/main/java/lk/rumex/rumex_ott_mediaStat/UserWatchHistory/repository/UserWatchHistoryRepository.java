package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.repository;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.model.UserWatchHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface UserWatchHistoryRepository
        extends JpaRepository<UserWatchHistory, Long> {

    Optional<UserWatchHistory> findByUserIdAndMediaIdAndMediaType(
            Long userId, Long mediaId, MediaType mediaType);

    List<UserWatchHistory> findByUserIdOrderByUpdatedAtDesc(Long userId);

    List<UserWatchHistory> findByUserIdAndIsCompletedTrueOrderByUpdatedAtDesc(Long userId);

    List<UserWatchHistory> findByUserIdAndIsCompletedFalseOrderByUpdatedAtDesc(Long userId);
}
