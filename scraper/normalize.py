"""Map a gosom/google-maps-scraper JSON entry onto our flat result schema.

The old engine produced: name, category, address, website, email, phone,
location_link, raw_text. We keep every one of those keys so the existing UI and
any downstream CSV consumers keep working, and add the fields worth having now
that we get structured data instead of regex guesses.
"""


def _first(seq):
    if isinstance(seq, list) and seq:
        return seq[0]
    return None


def dedup_key(entry):
    """Stable identity for a place.

    place_id is Google's own identifier, so this is an exact match rather than
    the old fuzzy 'name|phone' key that produced duplicates whenever the phone
    regex misfired or a business turned up in two overlapping grid cells.
    """
    for field in ("place_id", "cid", "data_id"):
        value = (entry.get(field) or "").strip()
        if value:
            return f"{field}:{value}"

    # Last resort for entries missing every identifier.
    title = (entry.get("title") or "").lower().strip()
    address = (entry.get("address") or "").lower().strip()
    return f"fallback:{title}|{address}"


def normalize(entry):
    """gosom JSON entry -> our result dict."""
    emails = entry.get("emails") or []

    # The upstream struct misspells this as 'longtitude'; newer builds emit both.
    longitude = entry.get("longitude")
    if longitude is None:
        longitude = entry.get("longtitude")

    complete = entry.get("complete_address") or {}

    return {
        # --- original schema, kept for backwards compatibility ---
        "name": entry.get("title"),
        "category": entry.get("category"),
        "address": entry.get("address"),
        "website": entry.get("web_site") or None,
        "email": _first(emails),
        "phone": entry.get("phone") or None,
        "location_link": entry.get("link"),

        # --- new: structured identity, makes dedup exact ---
        "place_id": entry.get("place_id") or None,
        "cid": entry.get("cid") or None,

        # --- new: fields the old regex scraper could never get right ---
        "emails": emails,
        "categories": entry.get("categories") or [],
        "latitude": entry.get("latitude"),
        "longitude": longitude,
        "rating": entry.get("review_rating"),
        "review_count": entry.get("review_count"),
        "price_range": entry.get("price_range") or None,
        "status": entry.get("status") or None,
        "plus_code": entry.get("plus_code") or None,
        "timezone": entry.get("timezone") or None,
        "description": entry.get("description") or None,
        "thumbnail": entry.get("thumbnail") or None,
        "city": complete.get("city") or None,
        "state": complete.get("state") or None,
        "postal_code": complete.get("postal_code") or None,
        "country": complete.get("country") or None,
        "open_hours": entry.get("open_hours") or {},
    }


# Column order for CSV export -- the useful stuff first.
CSV_COLUMNS = [
    "name", "category", "phone", "email", "website", "address",
    "city", "state", "postal_code", "country",
    "rating", "review_count", "price_range", "status",
    "latitude", "longitude", "place_id", "cid", "location_link",
]
