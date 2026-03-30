from dataclasses import dataclass
from typing import Optional


@dataclass
class ResolveResult:
    website: Optional[str] = ""
    instagram: Optional[str] = ""
    facebook: Optional[str] = ""
    tiktok: Optional[str] = ""

    tiktok_present: bool = False
    menu_present: bool = False
    booking_present: bool = False
    delivery_present: bool = False
    data_capture_present: bool = False
    contact_present: bool = False

    confidence: float = 0.0
    source: str = ""
    status: str = "ok"
    needs_review: bool = False
    is_restaurant_match: bool = True
    non_restaurant_reason: str = ""
    evidence: str = ""


def resolve_one(name: str, city: str, country: str) -> ResolveResult:
    """
    Simple safe resolver (no recursion, no external dependency)
    """

    name = (name or "").lower()
    city = (city or "").lower()

    result = ResolveResult()

    # -----------------------
    # Basic validation
    # -----------------------
    if not name:
        result.needs_review = True
        result.is_restaurant_match = False
        result.non_restaurant_reason = "Missing name"
        result.evidence = "No name provided"
        return result

    # -----------------------
    # Fake matching logic (safe starter)
    # Replace later with your real logic
    # -----------------------

    # VERY IMPORTANT: city check to avoid wrong matches
    if city and city not in name:
        result.needs_review = True
        result.confidence = 0.3
        result.evidence = f"City '{city}' not found in name → possible mismatch"
    else:
        result.confidence = 0.8

    # Dummy links (replace later with real extraction)
    result.website = ""
    result.instagram = ""
    result.facebook = ""
    result.tiktok = ""

    # Flags
    result.tiktok_present = False
    result.menu_present = False
    result.booking_present = False
    result.delivery_present = False
    result.data_capture_present = False
    result.contact_present = False

    result.source = "basic_resolver"
    result.evidence += f" | Input: {name}, {city}, {country}"

    return result