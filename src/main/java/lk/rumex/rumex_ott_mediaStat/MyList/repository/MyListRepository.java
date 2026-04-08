package lk.rumex.rumex_ott_mediaStat.MyList.repository;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.MyList.model.MyListItem;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface MyListRepository extends JpaRepository<MyListItem, Long> {
    Optional<MyListItem> findByUserIdAndMediaIdAndMediaType(Long userId, Long mediaId, MediaType mediaType);
    List<MyListItem> findByUserIdOrderByCreatedAtDesc(Long userId);

    Boolean existsByUserIdAndMediaIdAndMediaType(Long userId, Long mediaId, MediaType mediaType);
}
