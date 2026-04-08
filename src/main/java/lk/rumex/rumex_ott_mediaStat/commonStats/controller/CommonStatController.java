package lk.rumex.rumex_ott_mediaStat.commonStats.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import jakarta.validation.Valid;
import lk.rumex.ott_domain_models.category.Enum.MediaType;
import lk.rumex.rumex_ott_mediaStat.commonStats.dto.req.CommonStatReqDTO;
import lk.rumex.rumex_ott_mediaStat.commonStats.dto.res.DailyStatsResDTO;
import lk.rumex.rumex_ott_mediaStat.commonStats.dto.res.PerMinuteStatsResDTO;
import lk.rumex.rumex_ott_mediaStat.commonStats.service.CommonStatService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@CrossOrigin
@RestController
@RequestMapping("/common-stat")
@RequiredArgsConstructor
public class CommonStatController {

    private static final String TENANT_HEADER = "X-Tenant-Id";

    private final CommonStatService commonStatService;

    @Operation(summary = "Create Common Stat",
            description = "Creates a new common stat record for the given media along with title info.")
    @PostMapping
    public void createCommonStat(
            @Valid @RequestBody CommonStatReqDTO dto,
            @Parameter(description = "Title of the media") @RequestParam String title
    ) {
        commonStatService.createCommonStat(dto,title);
    }

    @GetMapping("/last3hours-per-minute")
    public ResponseEntity<List<PerMinuteStatsResDTO>> last3HoursPerMinute(
            @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER)
            @RequestHeader(TENANT_HEADER) Long tenantId) {
        return ResponseEntity.ok(commonStatService.getLast3HoursStatsPerMinute(tenantId));
    }

    @GetMapping("/last-30days-per-day")
    @Operation(
            summary = "Last 30 days, per-day stats for one mediaType",
            description = "Returns an array of { date:\"YYYY/MM/DD\", uniqueViewers, totalWatchTime } for a given mediaType."
    )
    public ResponseEntity<List<DailyStatsResDTO>> last30DaysByTypePerDay(
            @Parameter(description = "Tenant ID", in = ParameterIn.HEADER, name = TENANT_HEADER)
            @RequestHeader(TENANT_HEADER) Long tenantId,
            @Parameter(description = "Media Type") @RequestParam MediaType mediaType) {

        List<DailyStatsResDTO> stats =
                commonStatService.getLast30DaysStatsForMediaType(tenantId, mediaType);

        return ResponseEntity.ok(stats);
    }

}
