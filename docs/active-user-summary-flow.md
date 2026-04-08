# Active User Summary Flow

This document explains how the media-stat service loads daily active-user data for the CMS dashboard and how the summary-table flow reduces runtime load.

## Goal

The dashboard needs active-user counts without scanning large raw tables on every request.

The current implementation uses:

1. A summary table for daily active users
2. A scheduler that refreshes recent days
3. A dashboard read path that uses summary rows instead of running a full heavy query every time

## Main Request Flow

Frontend request:

- `GET /api/cms-dashboard/active-users`

CMS proxy forwards to media-stat backend:

- `GET /watch-sessions/dau`

Backend entry points:

- `MediaWatchSessionController#getDailyActiveUsers(...)`
- `MediaWatchSessionService#getDailyActiveUsers(...)`
- `MediaDailyActiveUserSummaryService#getOrBuildDailySummaries(...)`

## Code Path

### 1. Controller

File:

- `src/main/java/lk/rumex/rumex_ott_mediaStat/mediaStatistics/controller/MediaWatchSessionController.java`

Method:

- `getDailyActiveUsers(...)`

Responsibility:

- Read tenant and date range from the request
- Delegate to `MediaWatchSessionService`

### 2. Service entry

File:

- `src/main/java/lk/rumex/rumex_ott_mediaStat/mediaStatistics/service/MediaWatchSessionService.java`

Method:

- `getDailyActiveUsers(...)`

Responsibility:

- Convert the requested time range into local dates
- Read the daily active-user summaries
- Map summary rows into `DauResDTO`

### 3. Summary service

File:

- `src/main/java/lk/rumex/rumex_ott_mediaStat/mediaStatistics/service/MediaDailyActiveUserSummaryService.java`

Main methods:

- `refreshDailySummaries(...)`
- `getOrBuildDailySummaries(...)`
- `refreshRecentDailySummariesForAllTenants(...)`

Responsibilities:

- Build summary rows for one tenant and date range
- Return stored summary rows for dashboard reads
- Refresh recent days for all tenants using a scheduler

## Current Data Source

Repository file:

- `src/main/java/lk/rumex/rumex_ott_mediaStat/mediaStatistics/repository/MediaWatchSessionRepository.java`

Current summary builder query:

- `findDailyActiveUsersFromUsers(...)`

Current tenant discovery query:

- `findDistinctUserTenantIds()`

Important detail:

The active-user summary is currently built from the `users` table, not from `media_watch_session`.

Query rules:

- `tenant_id = :tenantId`
- `accountStatus IN ('ACTIVE', 'VERIFIED')`
- date filtering is applied on `createdAt`
- rows are grouped by year, month, and day

This matches the lighter query pattern used during debugging and avoids heavier watch-session based scans for this dashboard card.

## Summary Table Strategy

Summary entity:

- `MediaDailyActiveUserSummary`

Summary repository:

- `MediaDailyActiveUserSummaryRepository`

Stored values per day:

- `tenantId`
- `watchedDate`
- `activeUsers`
- `calculatedAt`

Why this helps:

- dashboard reads become simple summary lookups
- missing days can be stored as zero
- repeated requests for the same range do not need to recalculate every time

## Scheduler Flow

Scheduler file:

- `src/main/java/lk/rumex/rumex_ott_mediaStat/config/ActiveUserSummaryScheduler.java`

Method:

- `refreshRecentDailySummaries()`

Schedule:

- Every 30 minutes

What it does:

1. Load all tenant IDs from `users`
2. Refresh recent daily active-user summary rows for each tenant
3. Save zero-filled days where needed

Current lookback:

- `45` days

## Read Flow

`getOrBuildDailySummaries(...)` currently works like this:

1. Count how many summary days already exist for the requested tenant and range
2. If some days are missing, rebuild the requested range
3. Read summary rows ordered by day
4. Return them to `MediaWatchSessionService`

This means the endpoint mostly reads a small summary table rather than a large runtime aggregation.

## Why This Is Faster

The optimized path reduces load because:

- the dashboard reads daily summaries instead of scanning full watch-session history
- tenant discovery is done once per scheduler run
- the request path returns already-grouped rows
- date ranges are filled at summary time rather than in repeated frontend requests

## Current Behavior Note

Because the summary is currently built from `users.createdAt`, the dashboard shows:

- daily counts for qualifying users created in the requested range
- zero for days without matching user rows

If you need playback-based active users instead, switch the summary builder back to:

- `findDailyActiveUsers(...)`

That query reads `media_watch_session` and uses `watchTime` thresholds.

## How To Extend It

### Switch back to session-based active users

Change:

- `MediaDailyActiveUserSummaryService#refreshDailySummaries(...)`

From:

- `findDailyActiveUsersFromUsers(...)`

To:

- `findDailyActiveUsers(...)`

Use this when the business meaning of active user is "watched content recently" instead of "eligible active account created on that day".

### Add weekly or monthly summary tables

If daily summaries become too large, you can add:

- weekly summary table
- monthly summary table

Recommended approach:

1. Keep daily summaries as source-of-truth
2. Build weekly/monthly rollups from daily rows
3. Let dashboard endpoints choose the smallest summary source needed

### Add caching later

Caching can be added on top of summary reads, but the summary table itself already removes most of the heavy request load.

## Implementation Checklist

Use this pattern when building another stat endpoint:

1. Define the smallest summary grain that can support the dashboard.
2. Create a summary entity and repository.
3. Add a refresh service for one tenant and date range.
4. Add a scheduler for recent data.
5. Make the read endpoint consume summary rows first.
6. Keep the source query narrow and tenant-aware.

