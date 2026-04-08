package lk.rumex.rumex_ott_mediaStat.MyList.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.validation.Valid;
import lk.rumex.rumex_ott_mediaStat.MyList.dto.req.MyListItemDTO;
import lk.rumex.rumex_ott_mediaStat.MyList.dto.res.MyListItemResDTO;
import lk.rumex.rumex_ott_mediaStat.MyList.service.MyListService;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin
@RequestMapping("/my-list")
public class MyListController {

    @Autowired
    private MyListService myListService;

    @Operation(
            summary = "Add to MyList",
            description = "Adds an item to the user's MyList."
    )
    @PostMapping
    public MyListItemResDTO addToMyList(
            @Parameter(description = "MyList item payload") @Valid @RequestBody MyListItemDTO reqDTO) {
        return myListService.addToMyList(reqDTO);
    }

    @Operation(
            summary = "Get MyList",
            description = "Retrieves the MyList of the specified user."
    )
    @GetMapping("/user/{userId}")
    public List<MyListItemResDTO> getMyList(
            @Parameter(description = "User ID") @PathVariable Long userId) {
        return myListService.getMyList(userId);
    }

    @Operation(
            summary = "Remove from MyList",
            description = "Removes an item from the user's MyList."
    )
    @DeleteMapping("/user/{userId}/media/{mediaId}/type/{mediaType}")
    public void removeFromMyList(
            @Parameter(description = "User ID") @PathVariable Long userId,
            @Parameter(description = "Media ID") @PathVariable Long mediaId,
            @Parameter(description = "Media Type") @PathVariable MediaType mediaType) {
        myListService.removeFromMyList(userId, mediaId, mediaType);
    }

    @Operation(
            summary = "Exists in MyList",
            description = "Check an item exists in the user's MyList."
    )
    @GetMapping("/user/{userId}/media/{mediaId}/type/{mediaType}")
    public  Boolean existsInMyList(
            @Parameter(description = "User ID") @PathVariable Long userId,
            @Parameter(description = "Media ID") @PathVariable Long mediaId,
            @Parameter(description = "Media Type") @PathVariable MediaType mediaType) {
        return myListService.existsInMyList(userId, mediaId, mediaType);
    }


}
