package lk.rumex.rumex_ott_mediaStat.MyList.service;

import lk.rumex.rumex_ott_mediaStat.MyList.dto.req.MyListItemDTO;
import lk.rumex.rumex_ott_mediaStat.MyList.dto.res.MyListItemResDTO;
import lk.rumex.rumex_ott_mediaStat.MyList.mapper.MyListMapper;
import lk.rumex.rumex_ott_mediaStat.MyList.model.MyListItem;
import lk.rumex.rumex_ott_mediaStat.MyList.repository.MyListRepository;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MyListService {

    @Autowired
    private MyListRepository repository;

    @Autowired
    private MyListMapper mapper;

    public MyListItemResDTO addToMyList(MyListItemDTO dto) {
        var existing = repository.findByUserIdAndMediaIdAndMediaType(
                dto.getUserId(),
                dto.getMediaId(),
                dto.getMediaType()
        );
        MyListItem item = existing.orElseGet(() -> repository.save(mapper.toEntity(dto)));
        return mapper.toResponse(item);
    }

    public List<MyListItemResDTO> getMyList(Long userId) {
        return repository.findByUserIdOrderByCreatedAtDesc(userId)
                .stream()
                .map(mapper::toResponse)
                .toList();
    }

    public void removeFromMyList(Long userId, Long mediaId, MediaType mediaType) {
        repository.findByUserIdAndMediaIdAndMediaType(userId, mediaId, mediaType)
                .ifPresent(repository::delete);
    }

    public Boolean existsInMyList(Long userId, Long mediaId, MediaType mediaType) {
        return repository.existsByUserIdAndMediaIdAndMediaType(userId, mediaId, mediaType);
    }
}
