# rumex-ott-media-stat

## Current Git branch

The current working branch in this repository is:

`batch-processing`

## Main DTO flow

The main API for sending playback/stat updates is:

- `POST /common-stat?title={mediaTitle}`

Request body uses `CommonStatReqDTO`.

This endpoint is the combined flow. One request is split into:

1. `MediaWatchSessionDTO` for watch/session statistics
2. `UserWatchHistoryDTO` for continue-watching / last-position history

Code path:

- `CommonStatController.createCommonStat(...)`
- `CommonStatService.createCommonStat(...)`
- `MediaWatchSessionService.createWatchSession(...)`
- `UserWatchHistoryService.createOrUpdateUserWatchHistory(...)`

## What to send in `CommonStatReqDTO`

You send these properties in the request body:

| Field | Required | Notes |
| --- | --- | --- |
| `tenantId` | No in validation, but should be sent | Tenant identifier used for stats queries and stored in watch sessions |
| `mediaId` | Yes | Media or episode id |
| `mediaType` | Yes | Used in both stats and history. Seen values in code: `MOVIE`, `TV_SHOW`, `TV_EPISODE`, `LIVE_CHANNEL` |
| `slug` | Yes | Stored in both watch session metadata and watch history |
| `userId` | Yes | Viewer id |
| `accountOwnerId` | Needed for concurrent-stream restriction | Auth/account user id that owns the selected profile |
| `deviceId` | Needed for concurrent-stream restriction | Stable device identifier used to distinguish active streams |
| `userStatus` | No | Defaults to `R` if not sent. Allowed values: `G`, `PG_13`, `R` |
| `watchTime` | Yes | Minimum `1`. Input watch time for the stats flow |
| `deviceType` | Yes | `DESKTOP`, `MOBILE`, `TABLET`, `TV` |
| `interfaceType` | Yes | `WEB`, `ANDROID`, `IOS`, `IPAD` |
| `lastWatchPosition` | Yes | Minimum `0`. Used for continue-watching |
| `totalDuration` | Yes | Minimum `1` |
| `isCompleted` | No | Defaults to `false` |

## Property flow

### 1. `CommonStatReqDTO` -> `MediaWatchSessionDTO`

These properties are copied into the session stats flow:

- `tenantId`
- `mediaId`
- `mediaType`
- `slug`
- `userId`
- `accountOwnerId`
- `deviceId`
- `userStatus`
- `watchTime`
- `deviceType`
- `interfaceType`

Then `MediaWatchSessionService.createWatchSession(...)`:

- creates `MediaData` if that media is not already known
- saves `title` from query param together with `mediaId`, `mediaType`, and `slug`
- maps DTO to `MediaWatchSession`
- overrides `watchTime` to `15L`
- sets `watchedAt` to current Sri Lanka time
- adds the session to pending batch cache
- updates daily summary tables/cache

Important note:

- Even if client sends `watchTime`, the service currently forces stored session `watchTime` to `15L`

### 2. `CommonStatReqDTO` -> `UserWatchHistoryDTO`

These properties are copied into the history flow:

- `userId`
- `mediaId`
- `mediaType`
- `slug`
- `lastWatchPosition`
- `totalDuration`
- `isCompleted`

Then `UserWatchHistoryService.createOrUpdateUserWatchHistory(...)`:

- skips history completely for `LIVE_CHANNEL`
- finds existing history by `userId + mediaId + mediaType`
- updates existing record or creates a new one
- if `lastWatchPosition >= totalDuration`, forces `isCompleted = true`
- sets `updatedAt`
- keeps latest value in in-memory pending cache before DB flush

## Full data flow

### Step 1. Client sends one combined request

Client sends:

- `POST /common-stat?title={mediaTitle}`
- JSON body using `CommonStatReqDTO`

This is the main write API when the player wants to update both:

- watch/session statistics
- continue-watching / last-position history

### Step 2. Server splits the request into two internal flows

Inside `CommonStatService.createCommonStat(...)`, the server creates:

1. `MediaWatchSessionDTO`
2. `UserWatchHistoryDTO`

So one client request updates two server-side data areas.

### Step 3. Server manages stats/session data

Stats flow goes through `MediaWatchSessionService.createWatchSession(...)`.

The server manages these records:

- `MediaData`
  - used as metadata store for `mediaId`, `mediaType`, `slug`, `title`
- `MediaWatchSession`
  - used as raw watch session/stat record
- `MediaDailyWatchSummary`
  - used as cumulative summary for fast top/stat queries

What happens in this flow:

1. Check whether media metadata already exists.
2. If not, create `MediaData`.
3. Map request data into `MediaWatchSession`.
4. Force `watchTime = 15L`.
5. Set `watchedAt` to current `Asia/Colombo` time.
6. Store the session in `pendingSessions` first.
7. Update daily cumulative summary.
8. Clear stat caches so future reads return fresh values.

Session data stored by server:

| Field | Stored in |
| --- | --- |
| `tenantId` | `MediaWatchSession`, summary queries |
| `mediaId` | `MediaData`, `MediaWatchSession`, `MediaDailyWatchSummary` |
| `mediaType` | `MediaData`, `MediaWatchSession`, `MediaDailyWatchSummary` |
| `slug` | `MediaData`, `MediaWatchSession` |
| `title` | `MediaData` |
| `userId` | `MediaWatchSession` |
| `accountOwnerId` | `MediaWatchSession` |
| `deviceId` | `MediaWatchSession` |
| `userStatus` | `MediaWatchSession`, `MediaDailyWatchSummary` |
| `deviceType` | `MediaWatchSession` |
| `interfaceType` | `MediaWatchSession` |
| `watchTime` | `MediaWatchSession`, `MediaDailyWatchSummary` |
| `watchedAt` | `MediaWatchSession` |

Important behavior:

- session watch time is currently hard-coded to `15L` after mapping
- stats reads combine database records and still-pending in-memory records
- if `accountOwnerId` and `deviceId` are sent, the service enforces an account-level simultaneous stream limit using a recent heartbeat window

Concurrent stream conflict response example:

```json
{
  "errorCode": "CONCURRENT_STREAM_LIMIT_EXCEEDED",
  "message": "Simultaneous stream limit exceeded for this account",
  "accountOwnerId": 120,
  "requestedProfileId": 4502,
  "requestedMediaId": 777,
  "requestedMediaType": "MOVIE",
  "requestedTitle": "Avatar The Way of Water",
  "maxActiveStreams": 1,
  "activeWindowSeconds": 70,
  "activeStreams": [
    {
      "profileId": 4501,
      "deviceId": "android-6f8d2a91",
      "mediaId": 120,
      "mediaType": "MOVIE",
      "title": "John Wick 4",
      "slug": "john-wick-4",
      "deviceType": "MOBILE",
      "interfaceType": "ANDROID",
      "lastSeenAt": "2026-03-24T15:01:00+05:30"
    }
  ]
}
```

### Step 4. Server manages watch history data

History flow goes through `UserWatchHistoryService.createOrUpdateUserWatchHistory(...)`.

The server manages these records:

- `UserWatchHistory`
  - continue-watching data
  - last watched position
  - completion status

What happens in this flow:

1. If `mediaType` is `LIVE_CHANNEL`, history is skipped.
2. Build unique key as `userId:mediaId:mediaType`.
3. Check `pendingCache` first.
4. If not in memory, check DB.
5. Update existing record or create new one.
6. If `lastWatchPosition >= totalDuration`, force `isCompleted = true`.
7. Set `updatedAt`.
8. Keep newest value in memory until batch flush/persist.
9. Evict history caches so future reads are refreshed.

History data stored by server:

| Field | Stored in |
| --- | --- |
| `userId` | `UserWatchHistory` |
| `mediaId` | `UserWatchHistory` |
| `mediaType` | `UserWatchHistory` |
| `slug` | `UserWatchHistory` |
| `lastWatchPosition` | `UserWatchHistory` |
| `totalDuration` | `UserWatchHistory` |
| `isCompleted` | `UserWatchHistory` |
| `updatedAt` | `UserWatchHistory` |

## How data is managed internally

### In-memory first, DB later

The application uses in-memory holding areas before full persistence:

- `pendingSessions`
  - temporary list of `MediaWatchSession`
- `pendingCache`
  - temporary map of `UserWatchHistory`

This means newly written data may be available to read APIs even before it is fully flushed to DB, because service methods merge:

- database data
- pending in-memory data

### Cache management

The application also uses cache layers for read performance:

- Spring cache `mediaStats`
- Spring caches for watch history
- Redis for some stat responses

When new data is written:

- stat caches are cleared in watch-session flow
- watch-history caches are evicted in history flow

### Summary management

For fast reporting, watch sessions also update daily summary data:

- `MediaDailyWatchSummary`

This summary is used by endpoints like:

- top watched media
- type-level totals
- some aggregated media stats

For TV episodes:

- summary logic can roll episode watch time up to the parent TV show

## How to get information back

There are two main read areas:

1. stats/session reads
2. watch-history reads

### Stats/session read APIs

Main controller: `MediaWatchSessionController`

Available endpoints:

- `GET /watch-sessions/combined`
  - combined stat lookup by `mediaId`, `mediaType`, `deviceType`, `interfaceType`
- `GET /watch-sessions/top`
  - top watched media for tenant
- `GET /watch-sessions/top/general`
  - top watched media for all users
- `GET /watch-sessions/top/g`
  - top watched media for `G`
- `GET /watch-sessions/top/pg-13`
  - top watched media for `PG_13`
- `GET /watch-sessions/top/r`
  - top watched media for `R`
- `GET /watch-sessions/top/by-type`
  - top watched media route by type
- `GET /watch-sessions/monthWiseStats`
  - month-wise totals by media type and date range
- `GET /watch-sessions/overall`
  - overall total watch time and unique users
- `GET /watch-sessions/total-watch-time`
  - daily watch time in date range
- `GET /watch-sessions/unique-users`
  - daily unique users in date range
- `GET /watch-sessions/unique-users/at`
  - unique users at one time point
- `GET /watch-sessions/stats-by-date`
  - one media item stats for one date

Additional common-stat reporting endpoints:

- `GET /common-stat/last3hours-per-minute`
  - per-minute unique viewers for last 3 hours
- `GET /common-stat/last-30days-per-day`
  - per-day stats for one media type for last 30 days

For many stat read APIs, tenant is sent in header:

- `X-Tenant-Id`

### Watch-history read APIs

Main controller: `UserWatchHistoryController`

Available endpoints:

- `POST /watch-history`
  - create or update history directly using `UserWatchHistoryDTO`
- `GET /watch-history/user/{userId}/all`
  - all history for one user
- `GET /watch-history/user/{userId}/completed`
  - completed items only
- `GET /watch-history/user/{userId}/incomplete`
  - incomplete items only
- `GET /watch-history/user/{userId}/tvshow/{tvShowId}/episodes`
  - latest watch position for episodes in a TV show
- `GET /watch-history/user/{userId}/media/{mediaId}/type/{mediaType}`
  - one specific watch record
- `DELETE /watch-history/user/{userId}/media/{mediaId}/type/{mediaType}/incomplete`
  - remove unfinished history item

### How read results are built

When reading data, the server often does more than direct DB fetch:

- merges DB data with pending in-memory data
- applies cache if available
- collapses duplicate watch-history items so latest one wins
- groups TV episode history under TV show when needed
- aggregates session data by minute, day, month, device, tenant, and media type

## Server-side rules to remember

- `title` comes from query parameter, not JSON body
- `tenantId` is sent in request body for create flow
- `X-Tenant-Id` is used by many read endpoints
- `watchTime` currently becomes `15L` in session storage
- `LIVE_CHANNEL` does not create watch-history data
- `isCompleted` becomes `true` automatically when `lastWatchPosition >= totalDuration`
- new writes can be visible through merged read logic before DB flush

## Recommended request example

```json
{
  "tenantId": 1,
  "mediaId": 120,
  "mediaType": "MOVIE",
  "slug": "avatar-the-way-of-water",
  "userId": 4501,
  "accountOwnerId": 120,
  "deviceId": "android-6f8d2a91",
  "userStatus": "R",
  "watchTime": 15,
  "deviceType": "MOBILE",
  "interfaceType": "ANDROID",
  "lastWatchPosition": 300,
  "totalDuration": 7200,
  "isCompleted": false
}
```

Example call:

```http
POST /common-stat?title=Avatar%20The%20Way%20of%20Water
Content-Type: application/json

{
  "tenantId": 1,
  "mediaId": 120,
  "mediaType": "MOVIE",
  "slug": "avatar-the-way-of-water",
  "userId": 4501,
  "accountOwnerId": 120,
  "deviceId": "android-6f8d2a91",
  "userStatus": "R",
  "watchTime": 15,
  "deviceType": "MOBILE",
  "interfaceType": "ANDROID",
  "lastWatchPosition": 300,
  "totalDuration": 7200,
  "isCompleted": false
}
```

## Practical summary

- If you want one API call for both stats and continue-watching, use `/common-stat`
- Send all fields from `CommonStatReqDTO`
- `title` is not inside the JSON body; it is sent as request parameter
- `tenantId` is part of the JSON body for create flow
- to restrict simultaneous playback across profiles, client must send `accountOwnerId` as the auth user id and `deviceId` as a stable playback device id
- For reads, many endpoints use header `X-Tenant-Id`
- History is not stored for `LIVE_CHANNEL`
- Stored session watch time is currently fixed to `15L` in service logic
- Data is managed in two parts: stats/session data and watch-history data
- Read APIs use DB data plus pending in-memory data plus cache-aware aggregation
