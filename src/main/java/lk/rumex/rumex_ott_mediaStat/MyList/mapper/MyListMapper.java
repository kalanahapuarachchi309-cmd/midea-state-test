package lk.rumex.rumex_ott_mediaStat.MyList.mapper;

import lk.rumex.rumex_ott_mediaStat.MyList.dto.req.MyListItemDTO;
import lk.rumex.rumex_ott_mediaStat.MyList.dto.res.MyListItemResDTO;
import lk.rumex.rumex_ott_mediaStat.MyList.model.MyListItem;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface MyListMapper {
    MyListItemResDTO toResponse(MyListItem entity);
    MyListItem toEntity(MyListItemDTO dto);
}
