# progress/

This folder contains lightweight JSON manifests that track pipeline
progress per flood event. These are committed to git so teammates can
see what has already been done before starting work.

## Files

- `flood_{id:03d}_{iso}.json` — one file per flood event
- `summary.json` — aggregate view across all 150 events

## How to update

After completing any pipeline stage, run:

```bash
python generate_progress.py            # update all events
python generate_progress.py --flood-id 12   # update one event
```

Then commit and push:

```bash
git add progress/
git commit -m "chore: update pipeline progress"
git push
```

## Example manifest

```json
{
  "flood_id": 12,
  "iso": "IRN",
  "country": "Iran",
  "last_updated": "2026-03-10T14:32:00Z",
  "stages_complete": [1, 2, 3, 4],
  "cache_size": "2.3 GB",
  "pointers_total": 8423,
  "pointers_downloaded": 8176,
  "warc_fetch_success_rate": 0.97
}
```

## Before starting a new flood event

1. `git pull` to get the latest progress
2. Check `summary.json` or the event's individual manifest
3. If `stages_complete` includes stage 4, the WARCs are already downloaded
   — check `cache_size` and confirm where the cache lives before re-running
