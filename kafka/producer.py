"""Kafka producer: polls Ticketmaster Discovery API for recent event updates.

Publishes genuine event status changes (new listings, cancellations, etc.)
to the ``ticket_events`` Kafka topic every POLL_INTERVAL_SECONDS (default 300 s).

Uses the **same filtering criteria** as the batch ingestion pipeline:
- Music events only (classificationName=music)
- Same country codes (TICKETMASTER_COUNTRY_CODES)
- Future events only (today → 1 year forward)
- Excludes cancelled/postponed events, non-artist types, and events
  below the minimum upcoming-events threshold

Toggle on/off with ``make run-streaming`` / ``make stop-streaming``.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import error, parse, request

from kafka import KafkaProducer


def _http_json(url: str, params: dict | None = None) -> dict:
    if params:
        query = parse.urlencode({k: v for k, v in params.items() if v is not None})
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{query}"
    req = request.Request(url=url)
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    api_key = os.getenv("TICKETMASTER_API_KEY", "").strip()
    if not api_key:
        print("[producer] TICKETMASTER_API_KEY not set — exiting", file=sys.stderr)
        raise SystemExit(1)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "ticket_events")
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
    countries = [c.strip() for c in os.getenv("TICKETMASTER_COUNTRY_CODES", "US").split(",") if c.strip()]
    min_upcoming = int(os.getenv("TICKETMASTER_MIN_UPCOMING_EVENTS", "3"))

    # Write PID file for clean shutdown via make stop-streaming
    pid_path = Path(__file__).resolve().parent.parent / "logs" / "producer.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    seen_event_ids: set[str] = set()
    print(
        f"[producer] Polling TM Discovery API every {poll_interval}s "
        f"(countries={','.join(countries)}, min_upcoming={min_upcoming})",
        flush=True,
    )

    while running:
        now = datetime.now(timezone.utc)
        start_dt = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_dt = (now + timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")
        published = 0

        try:
            for country_code in countries:
                if not running:
                    break
                data = _http_json(
                    "https://app.ticketmaster.com/discovery/v2/events.json",
                    params={
                        "apikey": api_key,
                        "classificationName": "music",
                        "countryCode": country_code,
                        "size": 50,
                        "sort": "date,asc",
                        "startDateTime": start_dt,
                        "endDateTime": end_dt,
                    },
                )
                events = data.get("_embedded", {}).get("events", [])
                for event in events:
                    event_id = event.get("id")
                    if not event_id:
                        continue

                    attractions = event.get("_embedded", {}).get("attractions", [])
                    if not attractions:
                        continue
                    primary = attractions[0]

                    # Same filters as batch: min upcoming events
                    upcoming_total = (primary.get("upcomingEvents") or {}).get("_total", 0)
                    if upcoming_total < min_upcoming:
                        continue

                    # Same filters as batch: exclude non-artist types
                    classifications = primary.get("classifications") or []
                    attraction_type = ((classifications[0] if classifications else {}).get("type") or {}).get("name", "")
                    if attraction_type in ("Event Style", "Venue Based"):
                        continue

                    # Same filters as batch: exclude cancelled/postponed
                    status = (event.get("dates", {}).get("status") or {}).get("code", "unknown")
                    if status in ("cancelled", "postponed"):
                        continue

                    start = event.get("dates", {}).get("start") or {}
                    venue_info = (event.get("_embedded", {}).get("venues") or [{}])[0]

                    payload = {
                        "event_id": event_id,
                        "event_name": event.get("name"),
                        "artist_name": primary.get("name"),
                        "event_date": start.get("localDate"),
                        "event_status": status,
                        "venue_name": venue_info.get("name"),
                        "city": (venue_info.get("city") or {}).get("name"),
                        "country_code": (venue_info.get("country") or {}).get("countryCode"),
                        "observed_at": datetime.now(timezone.utc).isoformat(),
                        "is_new": event_id not in seen_event_ids,
                    }

                    producer.send(topic, payload)
                    published += 1
                    seen_event_ids.add(event_id)

            producer.flush()
            print(f"[producer] Published {published} events ({len(seen_event_ids)} total tracked)", flush=True)

        except error.HTTPError as exc:
            print(f"[producer] TM API error: HTTP {exc.code}", file=sys.stderr, flush=True)
        except Exception as exc:
            print(f"[producer] Error: {exc}", file=sys.stderr, flush=True)

        # Sleep in small increments so SIGTERM is responsive
        for _ in range(poll_interval):
            if not running:
                break
            time.sleep(1)

    producer.close()
    pid_path.unlink(missing_ok=True)
    print("[producer] Shut down cleanly", flush=True)


if __name__ == "__main__":
    main()
