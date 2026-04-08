package lk.rumex.rumex_ott_mediaStat.mediaStatistics.repository;

import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.model.MediaData;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MediaDataRepository extends JpaRepository<MediaData, Long> {
    boolean existsByMediaIdAndMediaType(Long mediaId, MediaType mediaType);
    MediaData findByMediaIdAndMediaType(Long mediaId, MediaType mediaType);
}
