"""Location name -> bounding box, via OpenStreetMap Nominatim.

Replaces the old Playwright-based coordinate discovery, which booted a whole
browser just to read '@lat,lon' out of a Google Maps URL. Nominatim returns a
bounding box directly, which is exactly what the scraper's -grid-bbox wants.
"""

import json
import time
import urllib.parse
import urllib.request

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Nominatim's usage policy requires a real identifying User-Agent.
USER_AGENT = "anti-gmaps-scraper/1.0 (+https://github.com/quackquavk)"

_last_request_at = 0.0


def _rate_limit():
    """Nominatim asks for max 1 request/second. Be a good citizen."""
    global _last_request_at
    elapsed = time.time() - _last_request_at
    if elapsed < 1.1:
        time.sleep(1.1 - elapsed)
    _last_request_at = time.time()


class GeocodeError(Exception):
    pass


def lookup(location, timeout=20):
    """Resolve a place name to (center, bbox).

    Returns:
        dict with keys:
          lat, lon      - center point (floats)
          bbox          - (min_lat, min_lon, max_lat, max_lon)
          display_name  - what Nominatim thinks this place is

    Raises GeocodeError if the location can't be resolved.
    """
    if not location or not location.strip():
        raise GeocodeError("empty location")

    params = urllib.parse.urlencode({
        "q": location.strip(),
        "format": "json",
        "limit": 1,
    })
    req = urllib.request.Request(
        f"{NOMINATIM_URL}?{params}",
        headers={"User-Agent": USER_AGENT},
    )

    _rate_limit()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        raise GeocodeError(f"Nominatim request failed: {e}") from e

    if not data:
        raise GeocodeError(f"no match for location '{location}'")

    hit = data[0]

    # Nominatim gives boundingbox as [min_lat, max_lat, min_lon, max_lon] strings.
    try:
        min_lat, max_lat, min_lon, max_lon = (float(v) for v in hit["boundingbox"])
    except (KeyError, ValueError, TypeError) as e:
        raise GeocodeError(f"malformed boundingbox in response: {e}") from e

    return {
        "lat": float(hit["lat"]),
        "lon": float(hit["lon"]),
        "bbox": (min_lat, min_lon, max_lat, max_lon),
        "display_name": hit.get("display_name", location),
    }


def bbox_dimensions_km(bbox):
    """Approximate (width_km, height_km) of a bbox. Good enough for cell sizing."""
    import math

    min_lat, min_lon, max_lat, max_lon = bbox
    mid_lat = (min_lat + max_lat) / 2.0

    height_km = (max_lat - min_lat) * 110.574
    width_km = (max_lon - min_lon) * 111.320 * math.cos(math.radians(mid_lat))

    return abs(width_km), abs(height_km)


def format_bbox(bbox):
    """Format a bbox for the scraper's -grid-bbox flag: minLat,minLon,maxLat,maxLon."""
    return ",".join(f"{v:.6f}" for v in bbox)


def point_in_bbox(lat, lon, bbox):
    """Grid results aren't strictly clipped to the bbox, so we clip them ourselves."""
    if lat is None or lon is None:
        return False
    min_lat, min_lon, max_lat, max_lon = bbox
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


# A longitude span this wide means the box wraps the antimeridian rather than
# describing a contiguous area. Countries with far-flung territories report one:
# "United States" comes back as -180..180 because of the Aleutians and American
# Samoa, so the box covers most of the planet and Frankfurt sits inside it.
DEGENERATE_LON_SPAN_DEG = 180.0


def spans_antimeridian(bbox):
    """True if this bbox wraps the globe and is useless as a search area."""
    _, min_lon, _, max_lon = bbox
    return (max_lon - min_lon) >= DEGENERATE_LON_SPAN_DEG


# Roughly how wide a Google Maps viewport is at each zoom level. Used to pick a
# zoom whose viewport covers the area we're aiming at, so the search isn't
# centred correctly but zoomed into one street.
_ZOOM_BY_SPAN_KM = (
    (3, 16),
    (8, 14),
    (25, 12),
    (80, 10),
    (250, 8),
    (800, 6),
    (2500, 4),
)

MIN_SEARCH_RADIUS_M = 2_000
MAX_SEARCH_RADIUS_M = 2_000_000


def zoom_for_span(span_km):
    """Pick a zoom level whose viewport roughly covers `span_km`."""
    for threshold, zoom in _ZOOM_BY_SPAN_KM:
        if span_km <= threshold:
            return zoom
    return 3


def radius_for_span(span_km):
    """Search radius in metres covering `span_km`, clamped to something sane."""
    radius = (span_km / 2.0) * 1000.0
    return int(max(MIN_SEARCH_RADIUS_M, min(MAX_SEARCH_RADIUS_M, radius)))
