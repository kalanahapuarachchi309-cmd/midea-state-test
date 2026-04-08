package lk.rumex.rumex_ott_mediaStat.mediaStatistics.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import jakarta.validation.Valid;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.ott_domain_models.shared.Enum.MaturityRating;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.DeviceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.InterfaceType;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.UserStatus;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.req.MediaWatchSessionDTO;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.req.MonthWiseStatsDTO;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.dto.res.*;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.service.MediaWatchSessionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;

@CrossOrigin
@RestController
@RequestMapping("/watch-sessions")
public class MediaWatchSessionController {

        private static final String TENANT_HEADER = "X-Tenant-Id";
        private static final Long RESTRICTED_TENANT_ID = 100101118L;

        @Autowired
        private MediaWatchSessionService mediaWatchSessionService;

        @Operation(summary = "Create Watch Session", description = "Creates a new watch session record for the given media along with title info.")
        @PostMapping
        public void createWatchSession(
                        @Valid @RequestBody MediaWatchSessionDTO dto,
                        @Parameter(description = "Title of the media") @RequestParam String title) {
                mediaWatchSessionService.createWatchSession(dto, title);
        }

        @Operation(summary = "Combined Media Stats", description = "Fetches media statistics based on a combination of parameters like mediaId, mediaType, deviceType, and interfaceType.")
        @GetMapping("/combined")
        public ResponseEntity<?> getCombinedMediaStats(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Media ID (optional)") @RequestParam(required = false) Long mediaId,
                        @Parameter(description = "Media Type (optional)") @RequestParam(required = false) MediaType mediaType,
                        @Parameter(description = "Device Type (optional)") @RequestParam(required = false) DeviceType deviceType,
                        @Parameter(description = "Interface Type (optional)") @RequestParam(required = false) InterfaceType interfaceType) {

                if (mediaId != null && mediaType != null && deviceType != null && interfaceType != null) {
                        MediaStatsResDTO result = mediaWatchSessionService
                                        .getStatByAllParams(tenantId, mediaId, mediaType, deviceType, interfaceType);
                        return ResponseEntity.ok(result);
                } else if (mediaId != null && mediaType != null) {
                        MediaStatsResDTO result = mediaWatchSessionService
                                        .getMediaStatsByMediaIdAndMediaType(tenantId, mediaId, mediaType);
                        return ResponseEntity.ok(result);
                } else if (mediaType != null) {
                        MediaStatsResDTO result = mediaWatchSessionService
                                        .getTypeStatsByMediaType(tenantId, mediaType);
                        return ResponseEntity.ok(result);
                } else if (deviceType != null) {
                        MediaStatsResDTO result = mediaWatchSessionService
                                        .getStatByDeviceType(tenantId, deviceType);
                        return ResponseEntity.ok(result);
                } else if (interfaceType != null) {
                        MediaStatsResDTO result = mediaWatchSessionService
                                        .getStatByInterfaceType(tenantId, interfaceType);
                        return ResponseEntity.ok(result);
                } else if (interfaceType != null && deviceType != null) {
                        MediaStatsResDTO result = mediaWatchSessionService
                                        .getStatByDeviceAndInterfaceType(tenantId, deviceType, interfaceType);
                        return ResponseEntity.ok(result);
                } else {
                        String errorMessage = "Invalid parameters: Provide at least mediaType, or mediaId with mediaType.";
                        return ResponseEntity.badRequest().body(errorMessage);
                }
        }

        @Operation(summary = "Top Watched Media", description = "Retrieves the top N watched media records for a given tenant.")
        @GetMapping("/top")
        public List<TopWatchedMediaResDTO> getTopWatched(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Top N results (default 10)") @RequestParam(defaultValue = "10") int topN) {
                if (isRestrictedTopTenant(tenantId)) {
                        return List.of();
                }
                return mediaWatchSessionService.getTopWatchedMediaByTenant(tenantId, topN);
        }

        @Operation(summary = "Top Watched Media (General)", description = "Retrieves the top N watched media records for a given tenant (all users).")
        @GetMapping("/top/general")
        public List<TopWatchedMediaResDTO> getTopWatchedGeneral(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Top N results (default 10)") @RequestParam(defaultValue = "10") int topN) {
                if (isRestrictedTopTenant(tenantId)) {
                        return List.of();
                }
                return mediaWatchSessionService.getTopWatchedMediaByTenant(tenantId, topN);
        }

        @Operation(summary = "Top Watched Media (G)", description = "Retrieves the top N watched media records for G-rated users.")
        @GetMapping("/top/g")
        public List<TopWatchedMediaResDTO> getTopWatchedG(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Top N results (default 10)") @RequestParam(defaultValue = "10") int topN) {
                if (isRestrictedTopTenant(tenantId)) {
                        return List.of();
                }
                return mediaWatchSessionService.getTopWatchedMediaByTenantAndStatus(tenantId, topN, MaturityRating.G);
        }

        @Operation(summary = "Top Watched Media (PG)", description = "Retrieves the top N watched media records for PG-rated users.")
        @GetMapping("/top/pg")
        public List<TopWatchedMediaResDTO> getTopWatchedPG(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Top N results (default 10)") @RequestParam(defaultValue = "10") int topN) {
                if (isRestrictedTopTenant(tenantId)) {
                        return List.of();
                }
                return mediaWatchSessionService.getTopWatchedMediaByTenantAndStatus(tenantId, topN, MaturityRating.PG);
        }

        @Operation(summary = "Top Watched Media (PG-13)", description = "Retrieves the top N watched media records for PG-13 users.")
        @GetMapping("/top/pg-13")
        public List<TopWatchedMediaResDTO> getTopWatchedPG13(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Top N results (default 10)") @RequestParam(defaultValue = "10") int topN) {
                if (isRestrictedTopTenant(tenantId)) {
                        return List.of();
                }
                return mediaWatchSessionService.getTopWatchedMediaByTenantAndStatus(tenantId, topN, MaturityRating.PG_13);
        }

        @Operation(summary = "Top Watched Media (R)", description = "Retrieves the top N watched media records for R-rated users.")
        @GetMapping("/top/r")
        public List<TopWatchedMediaResDTO> getTopWatchedR(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Top N results (default 10)") @RequestParam(defaultValue = "10") int topN) {
                if (isRestrictedTopTenant(tenantId)) {
                        return List.of();
                }
                return mediaWatchSessionService.getTopWatchedMediaByTenantAndStatus(tenantId, topN, MaturityRating.R);
        }

        @Operation(summary = "Top Watched Media (NC-17)", description = "Retrieves the top N watched media records for NC-17-rated users.")
        @GetMapping("/top/nc-17")
        public List<TopWatchedMediaResDTO> getTopWatchedNC17(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Top N results (default 10)") @RequestParam(defaultValue = "10") int topN) {
                if (isRestrictedTopTenant(tenantId)) {
                        return List.of();
                }
                return mediaWatchSessionService.getTopWatchedMediaByTenantAndStatus(tenantId, topN, MaturityRating.NC_17);
        }

        @Operation(summary = "Top Watched Media", description = "Retrieves the top N watched media records for a given tenant.")
        @GetMapping("/top/by-type")
        public List<TopWatchedMediaResDTO> getTopWatched(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Top N results (default 10)") @RequestParam(defaultValue = "10") int topN,
                        @Parameter(description = "Media Type (optional)") @RequestParam(required = false) MediaType mediaType) {
                if (isRestrictedTopTenant(tenantId)) {
                        return List.of();
                }
                return mediaWatchSessionService.getTopWatchedMediaByTenant(tenantId, topN, mediaType);
        }

        private boolean isRestrictedTopTenant(Long tenantId) {
                return RESTRICTED_TENANT_ID.equals(tenantId);
        }

        @Operation(summary = "Daily Active Users (DAU)", description = "Returns daily active users for a tenant within a specified date range.")
        @GetMapping("/dau")
        public List<DauResDTO> getDailyActiveUsers(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Start date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date startDate,
                        @Parameter(description = "End date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date endDate) {
                return mediaWatchSessionService.getDailyActiveUsers(tenantId, startDate.toInstant(), endDate.toInstant());
        }

        @Operation(summary = "Peak Usage Hours", description = "Returns hourly usage summary (active users, watch time, session count) for active users.")
        @GetMapping("/peak-usage-hours")
        public List<PeakUsageHourResDTO> getPeakUsageHours(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Start date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date startDate,
                        @Parameter(description = "End date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date endDate) {
                return mediaWatchSessionService.getPeakUsageHours(tenantId, startDate.toInstant(), endDate.toInstant());
        }

        @Operation(summary = "Device and Platform Usage", description = "Returns device-wise and platform-wise usage summary for active users.")
        @GetMapping("/device-platform-usage")
        public DevicePlatformUsageResDTO getDevicePlatformUsage(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Start date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date startDate,
                        @Parameter(description = "End date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date endDate) {
                return mediaWatchSessionService.getDevicePlatformUsage(tenantId, startDate.toInstant(), endDate.toInstant());
        }

        @Operation(summary = "Month-wise Stats", description = "Fetches month-wise statistics for a specific media type within a provided date range.")
        @GetMapping("/monthWiseStats")
        public List<MonthWiseStatsDTO> getMonthWiseStats(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Media Type") @RequestParam MediaType mediaType,
                        @Parameter(description = "Start date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date startDate,
                        @Parameter(description = "End date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date endDate) {
                return mediaWatchSessionService.getMonthWiseStats(tenantId, mediaType, startDate, endDate);
        }

        @Operation(summary = "Overall Stats", description = "Retrieves overall watch time and unique user count for a tenant.")
        @GetMapping("/overall")
        public OverallStatsResDTO getOverallStats(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId) {
                return mediaWatchSessionService.getOverallStatsByTenant(tenantId);
        }

        @Operation(summary = "Total Watch Time by Date Range", description = "Returns the daily total watch time for a tenant within a specified date range.")
        @GetMapping("/total-watch-time")
        public List<DailyWatchTimeResDTO> getTotalWatchTimeByDateRange(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Start date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date startDate,
                        @Parameter(description = "End date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date endDate) {
                return mediaWatchSessionService.getTotalWatchTimeByDateRange(tenantId, startDate.toInstant(),
                                endDate.toInstant());
        }

        @Operation(summary = "Daily Unique Users", description = "Returns the daily unique user count for a tenant within a specified date range.")
        @GetMapping("/unique-users")
        public List<DailyUniqueUsersResDTO> getUniqueUserCountByDateRange(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Start date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date startDate,
                        @Parameter(description = "End date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date endDate) {
                return mediaWatchSessionService.getUniqueUserCountByDateRange(tenantId, startDate.toInstant(),
                                endDate.toInstant());
        }

        @Operation(summary = "Daily Engagement", description = "Returns daily total watch time and unique user count for a tenant within a specified date range.")
        @GetMapping("/daily-engagement")
        public List<DailyEngagementResDTO> getDailyEngagementByDateRange(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Start date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date startDate,
                        @Parameter(description = "End date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date endDate) {
                return mediaWatchSessionService.getDailyEngagementByDateRange(tenantId, startDate.toInstant(),
                                endDate.toInstant());
        }

        @GetMapping("/unique-users/at")
        public ResponseEntity<UniqueUsersAtTimeResDTO> getUniqueUsersAtTimePoint(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime pointTime) {

                Instant start = pointTime.toInstant();
                Instant end = start.plus(1, ChronoUnit.MINUTES);

                Long uniqueUserCount = mediaWatchSessionService.getUniqueUsersAtTimePoint(tenantId, start, end);
                UniqueUsersAtTimeResDTO responseDTO = new UniqueUsersAtTimeResDTO(tenantId, uniqueUserCount,
                                Date.from(start));
                return ResponseEntity.ok(responseDTO);
        }

        @Operation(summary = "Daily Media Stats", description = "Retrieves watch time and unique user count for a specific movie or episode on a given date.")
        @GetMapping("/stats-by-date")
        public ResponseEntity<MediaStatsResDTO> getMediaStatsByDate(
                        @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER) @RequestHeader(TENANT_HEADER) Long tenantId,
                        @Parameter(description = "Media ID") @RequestParam Long mediaId,
                        @Parameter(description = "Media Type") @RequestParam MediaType mediaType,
                        @Parameter(description = "Date (yyyy-MM-dd)") @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date date) {
                MediaStatsResDTO stats = mediaWatchSessionService.getMediaStatsByDate(tenantId, mediaId, mediaType,
                                date.toInstant());
                return ResponseEntity.ok(stats);
        }

}
