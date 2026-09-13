# Queue-aware Phase 3: Starr webhooks, Decypharr capacity, and fairness

Phase 3 is additive. Adaptive Starr polling remains the source of reconciliation and safety when a webhook is delayed or lost. Existing instances keep all Phase 1/2 behavior until the new options are enabled.

## Optional Sonarr/Radarr webhook wake

Webhooks are **off by default** and are configured per Sonarr/Radarr instance.

1. Open the Huntarr instance editor, enable **Starr Webhook Wake**, and save. Huntarr generates a cryptographically random secret if the field is empty.
2. Copy the generated secret after saving.
3. In Sonarr or Radarr, open **Settings → Connect → + → Webhook**.
4. Use method `POST` and URL:

   ```text
   https://HUNTARR_HOST/api/webhooks/starr/APP/INSTANCE_ID
   ```

   Replace `APP` with `sonarr` or `radarr`. The stable instance ID is shown in Huntarr's instance editor.
5. Configure authentication using either:
   - custom request header `X-Huntarr-Webhook-Secret: GENERATED_SECRET` (preferred), or
   - HTTP Basic authentication with username `huntarr` and the generated secret as the password.
6. Enable Test, Grab, Download/Import completion, Download Failed, Import Failed, and Manual Interaction Required where the installed Starr version exposes them. Send the Starr test event and expect HTTP 200.

Huntarr accepts only known event types and correlatable Sonarr/Radarr IDs. Requests must be JSON and at most 256 KiB. Authentication uses constant-time comparison. Duplicate deliveries are durably identified by a SHA-256 digest and do not repeat lifecycle transitions or wakes. Secrets, authorization headers, request bodies, and endpoint credentials are never logged.

A webhook advances only an existing unresolved `PipelineState` row. It never creates a search claim or overwrites a terminal `completed`, `failed`, `timed_out`, or `no_grab` row. Every accepted non-duplicate lifecycle event wakes the corresponding app loop and invalidates its shared queue observation; adaptive polling still reconciles the real Starr queue afterward.

## Optional Decypharr capacity

Enable **Use Decypharr Worker Capacity** on a Sonarr/Radarr instance that already uses the instance's existing **qBittorrent-compatible torrent client** settings. No second client integration or credential store is created.

Huntarr reads:

- current active jobs from qBittorrent-compatible `/api/v2/torrents/info`;
- the job limit from `/api/v2/app/preferences` `max_active_downloads`;
- optionally, **Decypharr Max Active Jobs** as a fallback when that compatible endpoint does not report a limit.

Dispatch capacity is the minimum of Starr free slots, Decypharr free slots, and the instance's remaining hourly search budget. A healthy full Decypharr blocks dispatch. Disabled, unconfigured, or unavailable Decypharr telemetry explicitly fails open to the existing safe Starr behavior. Failed observations back off from 30 seconds to at most 300 seconds and expose the fail-open reason in cycle status. Client passwords are not included in cache keys, status output, or logs.

## Weighted Sonarr/Radarr scheduling

**Shared Capacity Weight** defaults to `1`. When Sonarr and Radarr instance workers contend, a process-wide weighted round-robin scheduler serializes the final dispatch grant. Equal defaults prevent one app from monopolizing shared capacity; larger values grant proportionally more turns. The grant is held only across the final capacity check and search POST, then released on success or failure. Other Arr types do not use this scheduler.

The cycle status API and Home UI show free slots, hourly search use, Decypharr health/free capacity, and exact pause reasons such as a full Starr queue, full Decypharr, exhausted search budget, or another Sonarr/Radarr instance holding the weighted shared grant.
