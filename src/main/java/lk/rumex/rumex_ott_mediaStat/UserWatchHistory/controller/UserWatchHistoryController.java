package lk.rumex.rumex_ott_mediaStat.UserWatchHistory.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.validation.Valid;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.req.UserWatchHistoryDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.res.EpisodeWatchPositionResDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.dto.res.UserWatchHistoryResDTO;
import lk.rumex.rumex_ott_mediaStat.UserWatchHistory.service.UserWatchHistoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin
@RequestMapping("/watch-history")
public class UserWatchHistoryController {

    private static final Long RESTRICTED_TENANT_ID = 100101118L;

    @Autowired
    private UserWatchHistoryService userWatchHistoryService;

    @Operation(
            summary = "Save or Update Watch History",
            description = "Creates or updates a watch history record for a user."
    )
    @PostMapping
    public UserWatchHistoryResDTO saveOrUpdate(
            @Parameter(description = "User watch history payload") @Valid @RequestBody UserWatchHistoryDTO reqDTO) {
        return userWatchHistoryService.createOrUpdateUserWatchHistory(reqDTO);
    }

    @Operation(
            summary = "Get All Watch History",
            description = "Retrieves all watch history records for the specified user."
    )
    @GetMapping("/user/{userId}/all")
    public List<UserWatchHistoryResDTO> getAllHistory(
            @Parameter(description = "User ID for which to retrieve watch history") @PathVariable Long userId) {
        return userWatchHistoryService.getAllUserWatchHistory(userId);
    }

    @Operation(
            summary = "Get Completed Watch History",
            description = "Retrieves all completed watch history records for the specified user."
    )
    @GetMapping("/user/{userId}/completed")
    public List<UserWatchHistoryResDTO> getCompletedHistory(
            @Parameter(description = "User ID for which to retrieve completed watch history") @PathVariable Long userId) {
        return userWatchHistoryService.getCompletedWatchHistory(userId);
    }

    @Operation(
            summary = "Get Incomplete Watch History",
            description = "Retrieves all incomplete watch history records for the specified user."
    )
    @GetMapping("/user/{userId}/incomplete")
    public List<UserWatchHistoryResDTO> getIncompleteHistory(
            @Parameter(description = "User ID for which to retrieve incomplete watch history") @PathVariable Long userId,
            @Parameter(description = "Tenant ID", required = false) @RequestHeader(value = "X-Tenant-Id", required = false) Long tenantId) {
        if (isRestrictedContinueWatchingTenant(tenantId)) {
            return List.of();
        }
        return userWatchHistoryService.getIncompleteWatchHistory(userId);
    }

    private boolean isRestrictedContinueWatchingTenant(Long tenantId) {
        return RESTRICTED_TENANT_ID.equals(tenantId);
    }

    @Operation(
            summary = "Get Episode Positions for TV Show",
            description = "Returns the latest watch position for each episode in the specified TV show for the given user."
    )
    @GetMapping("/user/{userId}/tvshow/{tvShowId}/episodes")
    public List<EpisodeWatchPositionResDTO> getEpisodePositions(
            @Parameter(description = "User ID") @PathVariable Long userId,
            @Parameter(description = "TV Show ID") @PathVariable Long tvShowId) {
        return userWatchHistoryService.getTvShowEpisodePositions(userId, tvShowId);
    }

    @Operation(
            summary = "Remove Incomplete Watch History Item",
            description = "Removes an incomplete watch history record for the specified user."
    )
    @DeleteMapping("/user/{userId}/media/{mediaId}/type/{mediaType}/incomplete")
    public void removeIncompleteHistory(
            @Parameter(description = "User ID") @PathVariable Long userId,
            @Parameter(description = "Media ID") @PathVariable Long mediaId,
            @Parameter(description = "Media Type") @PathVariable MediaType mediaType) {
        userWatchHistoryService.removeIncompleteWatchHistory(userId, mediaId, mediaType);
    }

    @Operation(
            summary = "Get Specific Watch Record",
            description = "Retrieves a particular watch history record based on user ID, media ID, and media type."
    )
    @GetMapping("/user/{userId}/media/{mediaId}/type/{mediaType}")
    public UserWatchHistoryResDTO getWatchRecord(
            @Parameter(description = "User ID") @PathVariable Long userId,
            @Parameter(description = "Media ID") @PathVariable Long mediaId,
            @Parameter(description = "Media Type") @PathVariable MediaType mediaType) {
        return userWatchHistoryService.getWatchHistory(userId, mediaId, mediaType);
    }
}
