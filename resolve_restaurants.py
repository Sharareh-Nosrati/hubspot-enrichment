from __future__ import annotations

import os
import re
import time
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple, Set
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from dotenv import load_dotenv

from search_provider_router import build_router, SearchResult

load_dotenv()


# -----------------------------
# Config
# -----------------------------
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
]

UA = "RestaurantResolver/1.0 (contact: you@example.com)"
TIMEOUT = 25
SLEEP_BETWEEN_REQUESTS_SEC = 1.0
CACHE_PATH = "resolver_cache.json"

SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")


# -----------------------------
# Domain patterns
# -----------------------------
DIRECTORY_DOMAIN_PATTERNS = [
    "google.com/maps",
    "maps.google.",
    "share.google",
    "goo.gl/maps",
    "maps.app.goo.gl",

    "justeat.",
    "deliveroo.",
    "thefork.",

    "tripadvisor.",
    "glovoapp.",
    "glovo.",

    "restaurantguru.",
    "opentable.",
    "quandoo.",
    "resmio.",

    "ubereats.",
    "paginegialle.it",
    "beverfood.com",
]

NON_OFFICIAL_WEBSITE_DOMAIN_PATTERNS = [
    "foodracers.com",
    "esserevegan.it",
    "tripadvisor.",
    "thefork.",
    "quandoo.",
    "opentable.",
    "resmio.",
    "justeat.",
    "deliveroo.",
    "ubereats.",
    "glovoapp.",
    "glovo.",
    "restaurantguru.",
    "beverfood.com",
    "paginegialle.it",

    "facebook.com",
    "instagram.com",
    "tiktok.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "youtu.be",
    "threads.net",
    "threads.com",
    "linktr.ee",
    "uqr.to",

    "tinyurl.com",
    "bit.ly",
    "t.co",
    "short.gy",
    "shorturl.at",
    "cutt.ly",
    "rb.gy",
    "share.google",
    "goo.gl",
    "maps.app.goo.gl",
]


# -----------------------------
# Result models
# -----------------------------
@dataclass
class LinkCandidate:
    url: str
    score: float
    source: str
    reason: str = ""


@dataclass
class ResolveResult:
    name: str
    city: str
    country: str

    website: Optional[str] = None
    instagram: Optional[str] = None
    facebook: Optional[str] = None
    tiktok: Optional[str] = None
    threads: Optional[str] = None
    x: Optional[str] = None
    youtube: Optional[str] = None
    linktree: Optional[str] = None
    uqrto: Optional[str] = None

    google_maps_url: Optional[str] = None
    justeat_url: Optional[str] = None
    deliveroo_url: Optional[str] = None
    thefork_url: Optional[str] = None
    tripadvisor_url: Optional[str] = None
    glovo_url: Optional[str] = None
    restaurantguru_url: Optional[str] = None
    opentable_url: Optional[str] = None
    quandoo_url: Optional[str] = None

    google_reviews_count: Optional[str] = "needs_api"
    google_rating_average: Optional[str] = "needs_api"
    instagram_bio_website: Optional[str] = None

    website_creator: Optional[str] = None
    website_creator_type: Optional[str] = None
    website_creator_confidence: Optional[str] = None
    website_creator_source: Optional[str] = None
    website_platform: Optional[str] = None

    website_score: float = 0.0
    website_validated: bool = False
    website_validation_score: float = 0.0
    website_validation_reason: str = ""

    instagram_score: float = 0.0
    facebook_score: float = 0.0
    tiktok_score: float = 0.0
    threads_score: float = 0.0
    x_score: float = 0.0
    youtube_score: float = 0.0
    linktree_score: float = 0.0
    uqrto_score: float = 0.0

    website_match_reason: str = ""
    instagram_match_reason: str = ""
    facebook_match_reason: str = ""
    tiktok_match_reason: str = ""
    threads_match_reason: str = ""
    x_match_reason: str = ""
    youtube_match_reason: str = ""
    linktree_match_reason: str = ""
    uqrto_match_reason: str = ""

    website_found_from: str = ""
    instagram_found_from: str = ""
    facebook_found_from: str = ""
    tiktok_found_from: str = ""
    threads_found_from: str = ""
    x_found_from: str = ""
    youtube_found_from: str = ""
    linktree_found_from: str = ""
    uqrto_found_from: str = ""

    instagram_bio_links_json: str = "[]"
    facebook_bio_links_json: str = "[]"
    instagram_primary_external_link: Optional[str] = None
    facebook_primary_external_link: Optional[str] = None

    directory_links_json: str = "[]"
    official_website_candidates_json: str = "[]"
    has_directory_profile: bool = False

    has_google_maps: bool = False
    has_justeat: bool = False
    has_deliveroo: bool = False
    has_thefork: bool = False
    has_tripadvisor: bool = False
    has_glovo: bool = False
    has_restaurantguru: bool = False
    has_opentable: bool = False
    has_quandoo: bool = False
    has_facebook_page: bool = False
    has_instagram_page: bool = False

    social_content_quality_label: str = "unknown"
    social_content_quality_score: float = 0.0
    social_identity_signal_label: str = "unknown"
    social_identity_signal_score: float = 0.0
    
    fb_website_link_present: bool = False
    fb_description_present: bool = False
    fb_hours_present: bool = False
    fb_bio_website_link_present: bool = False
    fb_bio_instagram_link_present: bool = False

    ig_website_link_present: bool = False
    ig_description_present: bool = False
    ig_bio_address_present: bool = False

    confidence: float = 0.0
    source: str = "none"
    evidence: str = ""
    needs_review: bool = True

    menu_present: bool = False
    booking_present: bool = False
    delivery_present: bool = False
    data_capture_present: bool = False
    contact_present: bool = False

    website_type: Optional[str] = None
    directions_present: bool = False
    reviews_visible: bool = False
    offers_promos_present: bool = False
    events_present: bool = False
    menu_quality: Optional[str] = None
    unique_value_present: bool = False
    unique_value_examples_json: str = "[]"
    website_completeness_score: float = 0.0
    website_strengths_json: str = "[]"
    website_weaknesses_json: str = "[]"

    is_restaurant_match: bool = False
    non_restaurant_reason: str = ""
    restaurant_match_score: float = 0.0
    restaurant_match_percent: int = 0
    restaurant_match_label: str = "No"
    tiktok_present: bool = False


# -----------------------------
# Cache helpers
# -----------------------------
def load_cache() -> dict:
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache: dict):
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def cache_key(name: str, city: str, country: str) -> str:
    return f"{norm(name)}|{norm(city)}|{norm(country)}"


# -----------------------------
# Generic helpers
# -----------------------------
def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def extract_google_maps_from_text(text: str) -> List[str]:
    import re
    patterns = [
        r"https?://www\.google\.com/maps[^\s\"']+",
        r"https?://maps\.google\.com[^\s\"']+",
        r"https?://goo\.gl/maps[^\s\"']+",
    ]
    links = []
    for p in patterns:
        links.extend(re.findall(p, text))
    return list(dict.fromkeys(links))


def extract_possible_addresses(text: str) -> List[str]:
    candidates = []
    lines = text.split("\n")

    for line in lines:
        l = line.strip()
        if len(l) < 10:
            continue

        if any(x in l.lower() for x in ["via", "viale", "piazza", "street", "road"]):
            candidates.append(l)

    return candidates[:3]


def search_google_maps_by_address(router, name: str, address: str, city: str):
    query = f"{name} {address} {city} google maps"
    resp = router.search(query)

    for r in resp.results:
        if "google.com/maps" in r.url:
            return r.url

    return None


def resolve_google_maps(
    name: str,
    city: str,
    router,
    website_html: Optional[str],
    instagram_html: Optional[str],
    facebook_html: Optional[str],
) -> Tuple[Optional[str], str]:

    # 1. Direct extraction
    for source_name, html in [
        ("website", website_html),
        ("instagram", instagram_html),
        ("facebook", facebook_html),
    ]:
        if html:
            links = extract_google_maps_from_text(html)
            if links:
                return links[0], f"found in {source_name}"

    # 2. Address-based search
    if website_html:
        addresses = extract_possible_addresses(website_html)
        for addr in addresses:
            gmaps = search_google_maps_by_address(router, name, addr, city)
            if gmaps:
                return gmaps, "resolved via address"

    # 3. Router fallback
    query = f"{name} {city} google maps"
    resp = router.search(query)

    for r in resp.results:
        if "google.com/maps" in r.url:
            return r.url, "resolved via router search"

    return None, "not found"



def looks_like_address_text(text: str) -> bool:
    text_n = normalize_for_match(text)

    address_keywords = [
        "via ", "viale ", "piazza ", "corso ", "vicolo ", "largo ",
        "street ", "road ", "avenue ", "boulevard ",
        "avellino", "napoli", "roma", "milano", "torino", "firenze",
        "indirizzo", "address", "dove siamo", "location"
    ]

    number_pattern = re.search(r"\b\d{1,4}\b", text_n)
    return any(k in text_n for k in address_keywords) and bool(number_pattern)


def extract_visible_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    return soup.get_text(" ", strip=True)


def analyze_instagram_profile_signals(instagram_url: str) -> Dict[str, Any]:
    result = {
        "ig_website_link_present": False,
        "ig_description_present": False,
        "ig_bio_address_present": False,
    }

    html = fetch_url(instagram_url)
    if not html:
        return result

    text = extract_visible_text_from_html(html)
    text_n = normalize_for_match(text)

    external_links = extract_external_links_from_instagram_html(html)
    clean_links = [u for u in external_links if is_valid_external_link(u)]

    result["ig_website_link_present"] = len(clean_links) > 0

    # Simple bio/description presence heuristic
    bio_keywords = [
        "ristorante", "restaurant", "pizzeria", "trattoria", "osteria",
        "menu", "prenota", "booking", "delivery", "food", "cucina",
        "chef", "bar", "pub", "wine", "cocktail", "dessert", "dolci"
    ]
    result["ig_description_present"] = any(k in text_n for k in bio_keywords)

    result["ig_bio_address_present"] = looks_like_address_text(text)

    return result


def analyze_facebook_profile_signals(facebook_url: str) -> Dict[str, Any]:
    result = {
        "fb_website_link_present": False,
        "fb_description_present": False,
        "fb_hours_present": False,
        "fb_bio_website_link_present": False,
        "fb_bio_instagram_link_present": False,
    }

    html = fetch_url(facebook_url)
    if not html:
        return result

    text = extract_visible_text_from_html(html)
    text_n = normalize_for_match(text)

    external_links = extract_external_links_from_facebook_html(html)
    clean_links = [u for u in external_links if is_valid_external_link(u)]

    result["fb_website_link_present"] = len(clean_links) > 0
    result["fb_bio_website_link_present"] = len(clean_links) > 0

    result["fb_bio_instagram_link_present"] = any(
        "instagram.com" in (u or "").lower() for u in external_links
    )

    description_keywords = [
        "ristorante", "restaurant", "pizzeria", "trattoria", "osteria",
        "menu", "prenota", "booking", "delivery", "food", "cucina",
        "chef", "bar", "pub", "wine", "cocktail", "dessert", "dolci",
        "chi siamo", "about", "specialità", "specialita"
    ]
    result["fb_description_present"] = any(k in text_n for k in description_keywords)

    hours_keywords = [
        "open now", "hours", "opening hours", "orari", "oggi aperto",
        "chiuso", "apre alle", "closes", "lun", "mar", "mer", "gio", "ven", "sab", "dom",
        "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
    ]
    result["fb_hours_present"] = any(k in text_n for k in hours_keywords)

    return result


def normalize_for_match(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("&", " and ")

    # important city normalization
    s = s.replace("porto venere", "portovenere")
    s = s.replace("porto-venere", "portovenere")

    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokenize_name(s: str) -> List[str]:
    stop = {
        "ristorante", "pizzeria", "osteria", "trattoria", "bar", "cafe", "pub",
        "restaurant", "pizza", "the", "di", "da", "la", "il", "lo", "le", "i", "gli"
    }
    return [t for t in normalize_for_match(s).split() if t and t not in stop and len(t) > 1]


def ensure_http(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return url
    if url.startswith(("http://", "https://")):
        return url
    if url.startswith("//"):
        return "https:" + url
    return "https://" + url


def looks_like_possible_url(value: str) -> bool:
    if not value or not isinstance(value, str):
        return False

    value = value.strip()
    if not value:
        return False

    low = value.lower()

    if low.startswith(("http://", "https://", "//")):
        return True

    if any(x in low for x in [
        "www.", ".com", ".it", ".net", ".org", ".io", ".eu",
        "instagram.com", "facebook.com", "tiktok.com",
        "threads.net", "threads.com", "x.com", "twitter.com",
        "youtube.com", "youtu.be", "linktr.ee", "uqr.to",
        "google.com/maps", "maps.google.", "share.google",
        "goo.gl/maps", "maps.app.goo.gl"
    ]):
        return True

    return False


def base_domain(url: str) -> str:
    try:
        parsed = urlparse(ensure_http(url))
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def append_unique_json_list(existing_json: str, values: List[str]) -> str:
    try:
        current = json.loads(existing_json or "[]")
        if not isinstance(current, list):
            current = []
    except Exception:
        current = []

    seen = set()
    out = []

    for v in current:
        if not isinstance(v, str) or not v.strip():
            continue
        cleaned = ensure_http(v).rstrip("/")
        if cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)

    for v in values:
        if not v or not isinstance(v, str):
            continue
        cleaned = ensure_http(v).rstrip("/")
        if cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)

    return json.dumps(out, ensure_ascii=False)


def json_list(value: str) -> List[str]:
    try:
        arr = json.loads(value or "[]")
        if isinstance(arr, list):
            return [x for x in arr if isinstance(x, str)]
    except Exception:
        pass
    return []


def is_directory_domain(domain: str) -> bool:
    domain = (domain or "").lower()
    return any(x in domain for x in DIRECTORY_DOMAIN_PATTERNS)


def is_non_official_website_domain(domain: str) -> bool:
    domain = (domain or "").lower()
    return any(x in domain for x in NON_OFFICIAL_WEBSITE_DOMAIN_PATTERNS)


def choose_best_candidate(candidates: List[LinkCandidate], min_score: float = 0.45) -> Optional[LinkCandidate]:
    if not candidates:
        return None
    best = sorted(candidates, key=lambda x: x.score, reverse=True)[0]
    return best if best.score >= min_score else None


def keep_best_candidate(current: Optional[LinkCandidate], candidate: Optional[LinkCandidate]) -> Optional[LinkCandidate]:
    if candidate is None:
        return current
    if current is None:
        return candidate
    return candidate if candidate.score > current.score else current


def extract_platform_links(directory_links_json: str) -> Dict[str, Optional[str]]:
    items = json_list(directory_links_json)

    result = {
        "google_maps": None,
        "justeat": None,
        "deliveroo": None,
        "thefork": None,
        "tripadvisor": None,
        "glovo": None,
        "restaurantguru": None,
        "opentable": None,
        "quandoo": None,
    }

    for url in items:
        u = url.lower()
        if is_google_maps_like(url) and not result["google_maps"]:
            result["google_maps"] = url
        elif "justeat" in u and not result["justeat"]:
            result["justeat"] = url
        elif "deliveroo" in u and not result["deliveroo"]:
            result["deliveroo"] = url
        elif "thefork" in u and not result["thefork"]:
            result["thefork"] = url
        elif "tripadvisor" in u and not result["tripadvisor"]:
            result["tripadvisor"] = url
        elif "glovo" in u and not result["glovo"]:
            result["glovo"] = url
        elif "restaurantguru" in u and not result["restaurantguru"]:
            result["restaurantguru"] = url
        elif "opentable" in u and not result["opentable"]:
            result["opentable"] = url
        elif "quandoo" in u and not result["quandoo"]:
            result["quandoo"] = url

    return result


# -----------------------------
# Name helpers
# -----------------------------
def clean_input_name(raw_name: str) -> str:
    name = (raw_name or "").strip()
    if not name:
        return ""

    name = re.sub(r"\b(snc|srl|sas|spa|srls)\b", " ", name, flags=re.IGNORECASE)
    name = name.replace("_", " ").replace("-", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name




def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        key = normalize_for_match(v)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def strip_city_from_name(raw_name: str, city: str) -> str:
    name = clean_input_name(raw_name)
    if not name:
        return ""

    for cv in city_variants(city):
        # remove "... Portovenere" at end
        pattern_end = rf"(\s+|\s*-\s*)(?:{re.escape(cv)})$"
        name = re.sub(pattern_end, "", name, flags=re.IGNORECASE).strip()

        # remove "a Portovenere", "in Portovenere", "di Portovenere"
        pattern_with_prep = rf"\b(?:a|in|di)\s+{re.escape(cv)}\b"
        name = re.sub(pattern_with_prep, "", name, flags=re.IGNORECASE).strip()

    name = re.sub(r"\s+", " ", name).strip(" -")
    return name


def split_name_on_separators(name: str) -> List[str]:
    parts = re.split(r"\s+-\s+|\s+\|\s+|/|,|\(|\)", name)
    out = []
    for p in parts:
        p = re.sub(r"\s+", " ", p).strip(" -")
        if p:
            out.append(p)
    return out


def build_search_name_set(raw_name: str, city: str) -> List[str]:
    names = extract_candidate_business_names(raw_name, city)
    extras: List[str] = []

    for n in names:
        n_clean = strip_city_from_name(n, city)
        if n_clean:
            extras.append(n_clean)

        if " - " in n:
            extras.extend([p for p in split_name_on_separators(n) if p])

    all_names = dedupe_preserve_order(names + extras)

    scored = []
    for n in all_names:
        tokens = tokenize_name(n)
        score = len(tokens)
        n_norm = normalize_for_match(n)

        if any(w in n_norm for w in ["ristorante", "pizzeria", "osteria", "trattoria"]):
            score += 2
        if n_norm == "il timone":
            score += 3

        scored.append((score, n))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [n for _, n in scored]




def extract_candidate_business_names(raw_name: str, city: str = "") -> List[str]:
    raw = clean_input_name(raw_name)
    if not raw:
        return []

    candidates: List[str] = []
    seen = set()

    def add_candidate(value: str):
        value = re.sub(r"\s+", " ", (value or "")).strip(" -")
        key = normalize_for_match(value)
        if value and key and key not in seen:
            seen.add(key)
            candidates.append(value)

    city_cleaned = strip_city_from_name(raw, city) if city else raw

    add_candidate(raw)
    add_candidate(city_cleaned)

    for part in split_name_on_separators(raw):
        add_candidate(part)

    for part in split_name_on_separators(city_cleaned):
        add_candidate(part)

    restaurant_words = ["ristorante", "pizzeria", "osteria", "trattoria", "bar", "cafe", "pub"]
    raw_lower = raw.lower()

    for word in restaurant_words:
        idx = raw_lower.find(word)
        if idx != -1:
            add_candidate(raw[idx:])
            add_candidate(strip_city_from_name(raw[idx:], city))

    words = city_cleaned.split()
    for n in [1, 2, 3, 4]:
        if len(words) >= n:
            add_candidate(" ".join(words[:n]))

    raw_n = normalize_for_match(city_cleaned)
    if " il timone " in f" {raw_n} " or raw_n.startswith("il timone") or raw_n.endswith("il timone"):
        add_candidate("Il Timone")

    add_candidate(city_cleaned.replace(" ", ""))
    return candidates


def generate_name_variants(name: str) -> List[str]:
    n = norm(name)
    variants = {n}

    removable_words = [
        "ristorante", "pizzeria", "osteria", "trattoria", "bar", "cafe", "rist", "pizza"
    ]

    words = n.split()
    filtered = [w for w in words if w not in removable_words]
    if filtered:
        variants.add(" ".join(filtered))

    variants.add(n.replace("'", ""))
    variants.add(n.replace("-", " "))
    variants.add(n.replace("&", "and"))

    return list(variants)


# -----------------------------
# Match helpers
# -----------------------------
def domain_slug(domain: str) -> str:
    domain = (domain or "").lower()
    domain = domain.replace("www.", "")
    domain = domain.split(":")[0]
    domain = domain.split(".")[0]
    return re.sub(r"[^a-z0-9]", "", domain)



def count_restaurant_evidence_signals(text: str) -> Dict[str, Any]:
    text_n = normalize_for_match(text)

    positive_groups = {
        "identity": [
            "restaurant", "ristorante", "pizzeria", "trattoria", "osteria", "pub", "bar", "cafe", "bistrot"
        ],
        "menu_food": [
            "menu", "menù", "food", "cucina", "chef", "pizza", "pasta", "dolci", "dessert", "wine", "vino",
            "cocktail", "drinks", "beer", "birra", "antipasti", "primi", "secondi", "ingredienti"
        ],
        "booking_service": [
            "prenota", "prenotazione", "booking", "reservation", "book a table",
            "delivery", "takeaway", "asporto", "ordina", "ordine online", "a domicilio"
        ],
        "contact_location": [
            "contatti", "contact", "telefono", "phone", "indirizzo", "address", "dove siamo", "location"
        ],
        "branding_story": [
            "specialità", "specialita", "tradizione", "homemade", "fatto in casa", "nostra storia", "chi siamo"
        ],
    }

    negative_terms = [
        "marketing agency", "digital agency", "web agency", "creative studio",
        "blog", "magazine", "news", "directory", "listing", "all restaurants",
        "job", "careers", "supplier", "wholesale", "catalog", "marketplace",
        "software", "saas", "platform", "template", "theme"
    ]

    matched_groups = {}
    total_positive = 0

    for group_name, words in positive_groups.items():
        hits = [w for w in words if w in text_n]
        matched_groups[group_name] = hits
        total_positive += len(hits)

    negatives = [w for w in negative_terms if w in text_n]

    return {
        "total_positive": total_positive,
        "matched_groups": matched_groups,
        "negative_hits": negatives,
        "group_count": sum(1 for _, hits in matched_groups.items() if hits),
    }



def normalized_name_slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", normalize_for_match(name))


def count_name_token_hits(name: str, text: str) -> int:
    tokens = tokenize_name(name)
    text_n = normalize_for_match(text)
    return sum(1 for t in tokens if t in text_n)


def exact_name_phrase_in_text(name: str, text: str) -> bool:
    return normalize_for_match(name) in normalize_for_match(text)


def looks_like_restaurant_context(text: str) -> int:
    text_n = normalize_for_match(text)

    keywords = [
        # generic restaurant
        "restaurant", "ristorante", "pizzeria", "trattoria", "osteria", "pub", "bar", "cafe", "bistrot",
        # food and menu
        "menu", "menù", "carta", "food", "piatti", "cucina", "kitchen", "chef", "antipasti",
        "primi", "secondi", "dolci", "dessert", "desserts", "wine", "vino", "cocktail", "drinks", "bevande",
        "birra", "beer", "aperitivo", "aperitivi", "pizza", "pasta", "carne", "pesce", "seafood", "grill",
        # operations
        "prenota", "prenotazione", "reservation", "booking", "book a table",
        "delivery", "takeaway", "asporto", "ordina", "ordine online", "a domicilio",
        # contact/location
        "contatti", "contact", "telefono", "phone", "indirizzo", "address", "dove siamo", "location",
        # service / identity
        "specialità", "specialita", "tradizione", "homemade", "fatto in casa", "ingredienti", "ingredients",
    ]

    return sum(1 for k in keywords if k in text_n)


def city_variants(city: str) -> List[str]:
    c = normalize_for_match(city)
    variants = {c}

    aliases = {
        "napoli": {"naples"},
        "naples": {"napoli"},
        "venezia": {"venice", "mestre"},
        "venice": {"venezia", "mestre"},
        "mestre": {"venezia", "venice"},
        "roma": {"rome"},
        "rome": {"roma"},
        "milano": {"milan"},
        "milan": {"milano"},
        "firenze": {"florence"},
        "florence": {"firenze"},
        "torino": {"turin"},
        "turin": {"torino"},
        "genova": {"genoa"},
        "genoa": {"genova"},
        "portovenere": {"porto venere", "porto-venere"},
        "porto venere": {"portovenere", "porto-venere"},
    }

    if c in aliases:
        variants.update(aliases[c])

    return [x for x in variants if x]


def count_city_mentions(text: str, city: str) -> int:
    text_n = normalize_for_match(text)
    count = 0
    for v in city_variants(city):
        count += len(re.findall(rf"\b{re.escape(v)}\b", text_n))
    return count


def detect_conflicting_city(text: str, target_city: str) -> Tuple[bool, str, int]:
    text_n = normalize_for_match(text)
    target_variants = set(city_variants(target_city))

    city_groups = {
        "napoli": {"napoli", "naples"},
        "venezia": {"venezia", "venice", "mestre"},
        "roma": {"roma", "rome"},
        "milano": {"milano", "milan"},
        "firenze": {"firenze", "florence"},
        "torino": {"torino", "turin"},
        "bologna": {"bologna"},
        "genova": {"genova", "genoa"},
        "palermo": {"palermo"},
        "verona": {"verona"},
        "padova": {"padova", "padua"},
        "mestre": {"mestre", "venezia", "venice"},
    }

    best_city = ""
    best_count = 0

    for label, variants in city_groups.items():
        if variants & target_variants:
            continue

        mentions = 0
        for v in variants:
            mentions += len(re.findall(rf"\b{re.escape(v)}\b", text_n))

        if mentions > best_count:
            best_count = mentions
            best_city = label

    if best_city and best_count >= 2 and count_city_mentions(text, target_city) == 0:
        return True, best_city, best_count

    return False, "", 0


def is_strong_official_domain(name: str, url: str) -> bool:
    dslug = domain_slug(base_domain(url))
    nslug = normalized_name_slug(name)
    if not dslug or not nslug:
        return False
    return dslug == nslug or dslug in nslug or nslug in dslug


def is_google_maps_like(url: str) -> bool:
    u = (url or "").lower()
    return (
        "google.com/maps" in u
        or "maps.google." in u
        or "share.google/" in u
        or "goo.gl/maps" in u
        or "maps.app.goo.gl" in u
    )


# -----------------------------
# URL cleaning
# -----------------------------
def _strip_tracking_params(u: str) -> str:
    u = re.sub(r"([?&])(utm_[^=&]+|fbclid|gclid)=[^&#]*", "", u, flags=re.IGNORECASE)
    u = re.sub(r"[?&]+$", "", u)
    return u


def clean_social_url(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None

    u = u.strip()
    if not u:
        return None

    if u.startswith("//"):
        u = "https:" + u
    elif not u.startswith(("http://", "https://")):
        if "instagram.com" in u or "facebook.com" in u:
            u = "https://" + u

    low = u.lower()
    bad_patterns = [
        "facebook.com/sharer",
        "facebook.com/share",
        "l.facebook.com",
        "m.facebook.com/sharer",
        "instagram.com/p/",
        "instagram.com/reel/",
        "instagram.com/stories/",
        "instagram.com/explore/",
        "instagram.com/accounts/",
        "instagram.com/direct/",
        "instagram.com/challenge/",
        "api.whatsapp.com",
        "wa.me/",
        "javascript:",
        "mailto:",
        "#",
    ]
    if any(b in low for b in bad_patterns):
        return None

    u = _strip_tracking_params(u)
    low = u.lower()

    if "instagram.com/" in low:
        m = re.search(r"(https?://(?:www\.)?instagram\.com/([A-Za-z0-9._]+))/?", u, flags=re.IGNORECASE)
        if not m:
            return None

        handle = (m.group(2) or "").lower().strip()
        bad_handles = {
            "explore", "reel", "reels", "stories", "p", "tv", "about",
            "developer", "directory", "accounts", "challenge", "popular"
        }
        if not handle or handle in bad_handles:
            return None

        return m.group(1) + "/"

    if "facebook.com/" in low:
        if any(x in low for x in ["/posts/", "/photo.php", "/photos/", "/media/", "/mentions/", "/videos/"]):
            return None

        m = re.search(r"(https?://(?:www\.)?facebook\.com/[^?#]+)", u, flags=re.IGNORECASE)
        if not m:
            return None

        cleaned = m.group(1).rstrip("/")
        path = cleaned.split("facebook.com/")[-1].strip("/").lower()
        first_part = path.split("/")[0] if path else ""

        bad_fb_paths = {"groups", "events", "watch", "share", "marketplace", "gaming"}
        if not first_part or first_part in bad_fb_paths:
            return None

        return cleaned

    return None


def clean_tiktok_url(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None

    u = u.strip()
    if not u:
        return None

    if u.startswith("//"):
        u = "https:" + u
    elif not u.startswith(("http://", "https://")) and "tiktok.com" in u:
        u = "https://" + u

    low = u.lower()
    bad_patterns = [
        "/video/", "/tag/", "/discover/", "/search", "vm.tiktok.com", "tiktok.com/t/",
        "javascript:", "mailto:", "#",
    ]
    if any(b in low for b in bad_patterns):
        return None

    u = _strip_tracking_params(u)
    m = re.search(r"(https?://(?:www\.)?tiktok\.com/@[^/?#]+)", u, flags=re.IGNORECASE)
    return m.group(1) if m else None


def clean_threads_url(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None

    u = u.strip()
    if not u:
        return None

    if u.startswith("//"):
        u = "https:" + u
    elif not u.startswith(("http://", "https://")) and ("threads.net" in u or "threads.com" in u):
        u = "https://" + u

    low = u.lower()
    if any(x in low for x in ["javascript:", "mailto:", "#"]):
        return None

    u = _strip_tracking_params(u)
    m = re.search(r"(https?://(?:www\.)?(?:threads\.net|threads\.com)/@?[^/?#]+)", u, flags=re.IGNORECASE)
    return m.group(1) if m else None


def clean_x_url(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None

    u = u.strip()
    if not u:
        return None

    if u.startswith("//"):
        u = "https:" + u
    elif not u.startswith(("http://", "https://")) and ("twitter.com" in u or "x.com" in u):
        u = "https://" + u

    low = u.lower()
    bad_patterns = [
        "javascript:", "mailto:", "#", "/search", "/hashtag/", "/explore", "/home", "/i/", "/intent/", "/share"
    ]
    if any(x in low for x in bad_patterns):
        return None

    m = re.search(r"(https?://(?:www\.)?(?:x\.com|twitter\.com)/[A-Za-z0-9_]{1,20})/?", u, flags=re.IGNORECASE)
    return m.group(1) if m else None


def clean_youtube_url(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None

    u = u.strip()
    if not u:
        return None

    if u.startswith("//"):
        u = "https:" + u
    elif not u.startswith(("http://", "https://")) and ("youtube.com" in u or "youtu.be" in u):
        u = "https://" + u

    low = u.lower()
    bad_patterns = ["javascript:", "mailto:", "#", "/watch?", "/playlist?", "/results?", "/shorts/", "youtu.be/"]
    if any(x in low for x in bad_patterns):
        return None

    patterns = [
        r"(https?://(?:www\.)?youtube\.com/@[A-Za-z0-9_.-]+)",
        r"(https?://(?:www\.)?youtube\.com/c/[A-Za-z0-9_.-]+)",
        r"(https?://(?:www\.)?youtube\.com/channel/[A-Za-z0-9_-]+)",
        r"(https?://(?:www\.)?youtube\.com/user/[A-Za-z0-9_.-]+)",
    ]
    for p in patterns:
        m = re.search(p, u, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def clean_linktree_url(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None
    u = ensure_http(u.strip())
    low = u.lower()
    if "linktr.ee/" not in low or any(x in low for x in ["javascript:", "mailto:", "#"]):
        return None
    m = re.search(r"(https?://(?:www\.)?linktr\.ee/[A-Za-z0-9_.-]+)", u, flags=re.IGNORECASE)
    return m.group(1) if m else None


def clean_uqrto_url(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None
    u = ensure_http(u.strip())
    low = u.lower()
    if "uqr.to/" not in low or any(x in low for x in ["javascript:", "mailto:", "#"]):
        return None
    m = re.search(r"(https?://(?:www\.)?uqr\.to/[A-Za-z0-9_-]+)", u, flags=re.IGNORECASE)
    return m.group(1) if m else None


def clean_google_maps_url(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None

    u = u.strip()
    if not u:
        return None

    if not looks_like_possible_url(u):
        return None

    u = ensure_http(u)
    u = _strip_tracking_params(u)

    if not is_google_maps_like(u):
        return None

    return u


# -----------------------------
# Fetch / redirect
# -----------------------------
def fetch_url(url: str) -> Optional[str]:
    try:
        safe_url = ensure_http(url).strip()
        if not looks_like_possible_url(safe_url):
            return None

        r = requests.get(
            safe_url,
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
            allow_redirects=True,
        )
        if 200 <= r.status_code < 300 and r.text:
            return r.text
    except requests.RequestException:
        return None
    except Exception:
        return None
    return None


def resolve_redirect_url(url: str) -> str:
    try:
        safe_url = ensure_http(url).strip()
        if not looks_like_possible_url(safe_url):
            return url

        r = requests.get(
            safe_url,
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
            allow_redirects=True,
        )
        return r.url or url
    except Exception:
        return url


def resolve_external_links(urls: List[str], max_items: int = 20) -> List[str]:
    resolved: List[str] = []
    seen: Set[str] = set()

    for u in urls[:max_items]:
        if not isinstance(u, str) or not u.strip():
            continue
        if not looks_like_possible_url(u):
            continue

        final_url = resolve_redirect_url(u).strip()
        final_url = ensure_http(final_url).rstrip("/")
        if final_url and final_url not in seen:
            seen.add(final_url)
            resolved.append(final_url)

    return resolved


# -----------------------------
# Validation / scoring
# -----------------------------
def validate_business_page_content(
    name: str,
    city: str,
    url: str,
    html: Optional[str] = None,
) -> Tuple[bool, float, str]:
    domain = base_domain(url)
    if is_non_official_website_domain(domain):
        return False, 0.0, f"non-official domain: {domain}"

    if html is None:
        html = fetch_url(url)

    if not html:
        return False, 0.0, "page could not be fetched"

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    text_n = normalize_for_match(text)
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    title_n = normalize_for_match(title)
    combined = f"{title_n} {text_n}"

    score = 0.0
    reasons = []

    token_hits = count_name_token_hits(name, combined)
    total_tokens = max(len(tokenize_name(name)), 1)
    token_ratio = token_hits / total_tokens

    exact_title = exact_name_phrase_in_text(name, title)
    exact_combined = exact_name_phrase_in_text(name, combined)

    if exact_title:
        score += 0.32
    elif exact_combined:
        score += 0.26
    elif token_ratio >= 0.8:
        score += 0.18
    elif token_ratio >= 0.6:
        score += 0.10
    else:
        reasons.append("weak name match")

    fuzzy_title = fuzz.token_set_ratio(normalize_for_match(name), title_n) / 100.0
    fuzzy_text = fuzz.token_set_ratio(normalize_for_match(name), text_n[:4000]) / 100.0
    fuzzy_best = max(fuzzy_title, fuzzy_text)
    score += min(0.14, fuzzy_best * 0.14)

    target_city_mentions = count_city_mentions(combined, city)
    if target_city_mentions >= 1:
        score += 0.14
    if target_city_mentions >= 2:
        score += 0.06

    restaurant_info = count_restaurant_evidence_signals(combined)
    restaurant_signal_count = restaurant_info["total_positive"]
    restaurant_group_count = restaurant_info["group_count"]
    negative_hits = restaurant_info["negative_hits"]

    if restaurant_group_count >= 2:
        score += 0.14
    if restaurant_group_count >= 3:
        score += 0.08
    if restaurant_signal_count >= 8:
        score += 0.08

    if is_strong_official_domain(name, url):
        score += 0.14

    phone_found = bool(re.search(r"\+?\d[\d\-\s()]{6,}", text))
    email_found = bool(re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text))
    if phone_found:
        score += 0.04
    if email_found:
        score += 0.03

    bad_listing_terms = [
        "directory", "listing", "all restaurants", "trova locali", "navigator",
        "discover restaurants", "food delivery marketplace", "ristoranti vicino",
        "local guide", "scheda locale"
    ]
    if any(x in combined for x in bad_listing_terms):
        score -= 0.28
        reasons.append("page looks like listing/aggregator")

    if negative_hits:
        score -= min(0.25, 0.07 * len(negative_hits))
        reasons.append(f"negative business signals: {', '.join(negative_hits[:4])}")

    has_conflict, conflict_city, conflict_mentions = detect_conflicting_city(combined, city)
    if has_conflict:
        score -= 0.45
        reasons.append(f"page strongly points to another city: {conflict_city} ({conflict_mentions} mentions)")

    # hard rejections
    if token_ratio < 0.45 and not exact_combined and fuzzy_best < 0.70:
        score = min(score, 0.48)
        reasons.append("name match too weak")

    if restaurant_group_count < 2 and restaurant_signal_count < 5:
        score = min(score, 0.52)
        reasons.append("restaurant evidence too weak")

    if has_conflict and target_city_mentions == 0:
        score = min(score, 0.35)

    score = max(0.0, min(score, 1.0))
    final_ok = (
        score >= 0.68
        and (exact_combined or token_ratio >= 0.55 or fuzzy_best >= 0.78 or is_strong_official_domain(name, url))
        and (restaurant_group_count >= 2 or restaurant_signal_count >= 6)
    )

    if final_ok:
        return True, round(score, 3), "official website candidate looks strong"
    return False, round(score, 3), "; ".join(dict.fromkeys(reasons)) or "page too weak"

def website_validation_details(name: str, city: str, website_url: str) -> Dict[str, Any]:
    result = {
        "is_valid": False,
        "score": 0.0,
        "reason": "",
        "name_match": False,
        "city_match": False,
        "other_city_penalty": False,
        "conflicting_city": "",
        "conflicting_city_mentions": 0,
    }

    domain = base_domain(website_url)
    if is_non_official_website_domain(domain):
        result["reason"] = f"Non-official website domain: {domain}"
        return result

    html = fetch_url(website_url)
    if not html:
        result["reason"] = "Website could not be fetched"
        return result

    ok, score, reason = validate_business_page_content(name, city, website_url, html)
    result["score"] = score
    result["reason"] = reason

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    combined = f"{title} {text}"

    result["name_match"] = (
        exact_name_phrase_in_text(name, combined)
        or count_name_token_hits(name, combined) / max(len(tokenize_name(name)), 1) >= 0.7
    )
    result["city_match"] = count_city_mentions(combined, city) > 0

    has_conflict, conflict_city, conflict_mentions = detect_conflicting_city(combined, city)
    result["other_city_penalty"] = has_conflict
    result["conflicting_city"] = conflict_city
    result["conflicting_city_mentions"] = conflict_mentions

    result["is_valid"] = ok
    return result


def apply_website_validation(res: ResolveResult, name: str, city: str, evidence: List[str]) -> None:
    if not res.website:
        return

    details = website_validation_details(name, city, res.website)
    res.website_validated = details["is_valid"]
    res.website_validation_score = details["score"]
    res.website_validation_reason = details["reason"]

    evidence.append(
        f"Website validation: valid={res.website_validated}, "
        f"score={res.website_validation_score}, "
        f"reason={res.website_validation_reason}"
    )

    if details.get("other_city_penalty") and not details.get("city_match"):
        evidence.append(
            f"Website rejected due to conflicting city: {details.get('conflicting_city')} "
            f"({details.get('conflicting_city_mentions')} mentions)"
        )
        res.website_validated = False
        res.website_validation_score = 0.0
        res.website_validation_reason = "Rejected due to conflicting city"

    if not res.website_validated:
        res.official_website_candidates_json = append_unique_json_list(
            res.official_website_candidates_json,
            [res.website],
        )
        res.website = None
        res.website_score = 0.0
        res.website_match_reason = ""
        res.website_found_from = ""


def looks_like_non_business_instagram(url: str, title: str = "", snippet: str = "") -> bool:
    text = f"{url} {title} {snippet}".lower()
    bad_terms = [
        "fanclub", "fan club", "fans", "supporters", "community", "tribute",
        "official fan", "napoli fan", "milan fan", "milano fan", "club"
    ]
    return any(term in text for term in bad_terms)


def looks_like_non_business_tiktok(url: str, title: str = "", snippet: str = "") -> bool:
    text = f"{url} {title} {snippet}".lower()
    bad_terms = ["fan", "fanclub", "tribute", "community", "edit", "edits"]
    return any(term in text for term in bad_terms)


def score_text_candidate(name: str, city: str, title: str = "", snippet: str = "", url: str = "") -> float:
    title_n = normalize_for_match(title)
    snippet_n = normalize_for_match(snippet)
    url_n = normalize_for_match(url)
    combined = f"{title_n} {snippet_n} {url_n}"

    score = 0.0

    token_hits = count_name_token_hits(name, combined)
    total_tokens = max(len(tokenize_name(name)), 1)
    token_ratio = token_hits / total_tokens

    if exact_name_phrase_in_text(name, title):
        score += 0.38
    elif exact_name_phrase_in_text(name, combined):
        score += 0.30
    else:
        score += min(0.28, token_ratio * 0.30)

    fuzzy_title = fuzz.token_set_ratio(normalize_for_match(name), title_n) / 100.0
    fuzzy_combined = fuzz.token_set_ratio(normalize_for_match(name), combined) / 100.0
    score += max(fuzzy_title, fuzzy_combined) * 0.18

    city_hits = count_city_mentions(combined, city)
    if city_hits >= 1:
        score += 0.14
    if city_hits >= 2:
        score += 0.06

    restaurant_hits = looks_like_restaurant_context(combined)
    if restaurant_hits >= 1:
        score += 0.08
    if restaurant_hits >= 3:
        score += 0.08

    if is_strong_official_domain(name, url):
        score += 0.18

    has_conflict, _, _ = detect_conflicting_city(combined, city)
    if has_conflict and city_hits == 0:
        score -= 0.30

    domain = base_domain(url)
    if is_directory_domain(domain):
        score -= 0.18
    if is_non_official_website_domain(domain):
        score -= 0.35

    return max(0.0, min(score, 1.0))


def score_social_candidate(name: str, city: str, title: str = "", snippet: str = "", url: str = "") -> float:
    score = score_text_candidate(name, city, title, snippet, url)

    ul = (url or "").lower()
    if any(x in ul for x in [
        "instagram.com", "facebook.com", "tiktok.com",
        "x.com", "twitter.com", "linktr.ee", "uqr.to"
    ]):
        score += 0.10

    if "/pages/" in ul:
        score -= 0.10

    if looks_like_non_business_instagram(url, title, snippet):
        score -= 0.30

    if looks_like_non_business_tiktok(url, title, snippet):
        score -= 0.25

    return max(0.0, min(score, 1.0))


# -----------------------------
# Website creator detection
# -----------------------------
def detect_website_creator(html: str, website_url: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    html_lower = html.lower()
    text_lower = text.lower()

    result = {
        "website_creator": "",
        "website_creator_type": "unknown",
        "website_creator_confidence": "low",
        "website_creator_source": "",
        "website_platform": "",
    }

    text_patterns = [
        r"(?:developed by|created by|made by|designed by|site by|website by|powered by|realizzato da|sviluppato da)\s+([A-Za-z0-9&.,'() \-]{2,80})",
        r"(?:web design by|design by|crafted by)\s+([A-Za-z0-9&.,'() \-]{2,80})",
    ]

    for pattern in text_patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            creator = m.group(1).strip(" -|•:").strip()
            creator = re.sub(r"\s+", " ", creator)
            if creator:
                low = creator.lower()
                bad_terms = [
                    "wordpress", "wix", "squarespace", "shopify", "cookie",
                    "privacy", "reserved", "all rights"
                ]
                if not any(x in low for x in bad_terms):
                    result["website_creator"] = creator
                    result["website_creator_type"] = "agency"
                    result["website_creator_confidence"] = "high"
                    result["website_creator_source"] = "visible_text"
                    return result

    for a in soup.find_all("a", href=True):
        label = (a.get_text(" ", strip=True) or "").lower()
        href = a.get("href", "")
        domain = base_domain(href)

        if any(x in label for x in ["developed by", "created by", "designed by", "site by", "powered by"]):
            creator = a.get_text(" ", strip=True)
            if creator:
                result["website_creator"] = creator
                result["website_creator_type"] = "agency"
                result["website_creator_confidence"] = "high"
                result["website_creator_source"] = "footer_link"
                return result

        if domain and domain not in {base_domain(website_url), ""}:
            if any(x in label for x in ["agency", "studio", "digital", "web", "creative"]):
                result["website_creator"] = domain
                result["website_creator_type"] = "agency"
                result["website_creator_confidence"] = "medium"
                result["website_creator_source"] = "footer_external_link"
                return result

    gen = soup.find("meta", attrs={"name": re.compile(r"generator", re.I)})
    if gen and gen.get("content"):
        gen_content = gen.get("content", "").strip()
        result["website_platform"] = gen_content
        low = gen_content.lower()

        for platform in ["wordpress", "wix", "squarespace", "shopify", "joomla"]:
            if platform in low:
                result["website_creator"] = platform.capitalize() if platform != "wordpress" else "WordPress"
                result["website_creator_type"] = "platform"
                result["website_creator_confidence"] = "medium"
                result["website_creator_source"] = "meta_generator"
                return result

    fingerprints = [
        ("WordPress", ["wp-content", "wp-includes", "wordpress"]),
        ("Elementor", ["elementor"]),
        ("WooCommerce", ["woocommerce"]),
        ("Wix", ["static.wixstatic.com", "wixsite", "wix-image"]),
        ("Squarespace", ["static.squarespace.com", "squarespace"]),
        ("Shopify", ["cdn.shopify.com", "shopify"]),
        ("Webflow", ["webflow", "assets.website-files.com"]),
        ("Framer", ["framer", "framerusercontent"]),
        ("PrestaShop", ["prestashop"]),
    ]

    for platform, signals in fingerprints:
        if any(sig in html_lower or sig in website_url.lower() for sig in signals):
            result["website_creator"] = platform
            result["website_platform"] = platform
            result["website_creator_type"] = "platform"
            result["website_creator_confidence"] = "medium"
            result["website_creator_source"] = "technical_fingerprint"
            return result

    if "powered by" in text_lower:
        m = re.search(r"powered by\s+([A-Za-z0-9&.,'() \-]{2,80})", text, flags=re.IGNORECASE)
        if m:
            creator = m.group(1).strip(" -|•:").strip()
            if creator:
                result["website_creator"] = creator
                result["website_creator_type"] = "platform"
                result["website_creator_confidence"] = "medium"
                result["website_creator_source"] = "visible_text"
                return result

    return result


# -----------------------------
# HTML / profile parsing
# -----------------------------
def extract_links_from_html(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    found_urls: List[str] = []

    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if isinstance(href, str) and looks_like_possible_url(href):
            found_urls.append(href)

    for meta in soup.find_all(["meta", "link"]):
        for attr in ["content", "href"]:
            val = meta.get(attr)
            if val and isinstance(val, str) and looks_like_possible_url(val):
                found_urls.append(val)

    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.text
        if not raw:
            continue
        try:
            data = json.loads(raw)
            blocks = data if isinstance(data, list) else [data]
            for block in blocks:
                if isinstance(block, dict):
                    same_as = block.get("sameAs")
                    if isinstance(same_as, list):
                        found_urls.extend([x for x in same_as if isinstance(x, str) and looks_like_possible_url(x)])
                    elif isinstance(same_as, str) and looks_like_possible_url(same_as):
                        found_urls.append(same_as)
        except Exception:
            pass

    instagram = None
    facebook = None
    tiktok = None
    threads = None
    x_url = None
    youtube = None
    linktree = None
    uqrto = None
    google_maps = None
    directory_links: List[str] = []
    official_website_candidates: List[str] = []

    seen_dirs = set()
    seen_websites = set()

    for u in found_urls:
        if not isinstance(u, str):
            continue

        cu_social = clean_social_url(u)
        cu_tiktok = clean_tiktok_url(u)
        cu_threads = clean_threads_url(u)
        cu_x = clean_x_url(u)
        cu_youtube = clean_youtube_url(u)
        cu_linktree = clean_linktree_url(u)
        cu_uqrto = clean_uqrto_url(u)
        cu_gmaps = clean_google_maps_url(u)

        if not instagram and cu_social and "instagram.com" in cu_social.lower():
            instagram = cu_social
        if not facebook and cu_social and "facebook.com" in cu_social.lower():
            facebook = cu_social
        if not tiktok and cu_tiktok:
            tiktok = cu_tiktok
        if not threads and cu_threads:
            threads = cu_threads
        if not x_url and cu_x:
            x_url = cu_x
        if not youtube and cu_youtube:
            youtube = cu_youtube
        if not linktree and cu_linktree:
            linktree = cu_linktree
        if not uqrto and cu_uqrto:
            uqrto = cu_uqrto
        if not google_maps and cu_gmaps:
            google_maps = cu_gmaps

        if not looks_like_possible_url(u):
            continue

        full = ensure_http(u)
        domain = base_domain(full)

        if domain:
            if is_directory_domain(domain):
                cleaned = clean_google_maps_url(full) if is_google_maps_like(full) else ensure_http(full)
                if cleaned and cleaned not in seen_dirs:
                    seen_dirs.add(cleaned)
                    directory_links.append(cleaned)
            elif not is_non_official_website_domain(domain):
                cleaned = ensure_http(full)
                if cleaned not in seen_websites:
                    seen_websites.add(cleaned)
                    official_website_candidates.append(cleaned)

    return {
        "instagram": instagram,
        "facebook": facebook,
        "tiktok": tiktok,
        "threads": threads,
        "x": x_url,
        "youtube": youtube,
        "linktree": linktree,
        "uqrto": uqrto,
        "google_maps": google_maps,
        "directory_links": directory_links,
        "official_website_candidates": official_website_candidates,
    }


def is_valid_external_link(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False

    u = url.lower().strip()
    domain = base_domain(u)

    blocked_patterns = [
        "static.xx.fbcdn.net",
        "fbcdn.net",
        "apple.com",
        "youtube.com/watch",
        "youtu.be/",
        "vip",
        "bet",
        "casino",
        "porn",
        "adult",
        "register",
        "login",
        "signup",
        "track",
        "click",
        "redirect",
        "tinyurl.com",
        "bit.ly",
        "short.gy",
        "tg-need",
        "sinarabadi",
        "heylink.me",
    ]
    if any(x in u for x in blocked_patterns):
        return False

    if any(u.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".svg", ".ico", ".gif", ".webp"]):
        return False

    if any(x in u for x in [
        "instagram.com",
        "facebook.com",
        "tiktok.com",
        "threads.net",
        "threads.com",
        "wa.me",
        "whatsapp.com",
    ]):
        return False

    if domain in {"apple.com", "www.apple.com"}:
        return False

    return True


def extract_external_links_from_instagram_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    found: List[str] = []
    seen = set()
    candidate_values: List[str] = []

    for a in soup.find_all("a", href=True):
        candidate_values.append(a.get("href"))

    for meta in soup.find_all(["meta", "link"]):
        for attr in ["content", "href"]:
            val = meta.get(attr)
            if val and isinstance(val, str):
                candidate_values.append(val)

    for script in soup.find_all("script"):
        raw = script.string or script.text
        if raw and isinstance(raw, str):
            candidate_values.append(raw)
            json_like_urls = re.findall(r'https?:\\\\?/\\\\?/[^"\']+', raw)
            candidate_values.extend([u.replace("\\/", "/") for u in json_like_urls])

    for value in candidate_values:
        if not value or not isinstance(value, str):
            continue

        urls = re.findall(r'https?://[^\s"\'<>\\]+', value)
        for u in urls:
            u = u.strip().rstrip(".,);]")
            if not is_valid_external_link(u):
                continue
            if u not in seen:
                seen.add(u)
                found.append(u)

    preferred_order = []
    for u in found:
        low = u.lower()
        if any(x in low for x in ["linktr.ee", "uqr.to", "beacons.ai", "lnk.bio", "taplink", "bio.site"]):
            preferred_order.append((0, u))
        else:
            preferred_order.append((1, u))

    preferred_order.sort(key=lambda x: x[0])
    return [u for _, u in preferred_order]


def analyze_instagram_external_links(instagram_url: str) -> Dict[str, Any]:
    result = {
        "instagram_primary_external_link": None,
        "instagram_bio_links_json": "[]",
        "instagram_bio_website": None,
    }

    html = fetch_url(instagram_url)
    if not html:
        return result

    raw_links = extract_external_links_from_instagram_html(html)
    resolved_links = resolve_external_links(raw_links, max_items=20)
    clean_resolved = [u for u in resolved_links if is_valid_external_link(u)]

    if clean_resolved:
        result["instagram_primary_external_link"] = clean_resolved[0]
        result["instagram_bio_links_json"] = json.dumps(clean_resolved, ensure_ascii=False)

        for u in clean_resolved:
            domain = base_domain(u)
            if not is_non_official_website_domain(domain) and not is_directory_domain(domain):
                result["instagram_bio_website"] = u
                break

    return result


def extract_external_links_from_facebook_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    found: List[str] = []
    seen = set()
    candidate_values: List[str] = []

    for a in soup.find_all("a", href=True):
        candidate_values.append(a.get("href"))

    for meta in soup.find_all(["meta", "link"]):
        for attr in ["content", "href"]:
            val = meta.get(attr)
            if val and isinstance(val, str):
                candidate_values.append(val)

    for script in soup.find_all("script"):
        raw = script.string or script.text
        if raw and isinstance(raw, str):
            candidate_values.append(raw)

    for value in candidate_values:
        if not value or not isinstance(value, str):
            continue

        urls = re.findall(r'https?://[^\s"\'<>\\]+', value)
        for u in urls:
            u = u.strip().rstrip(".,);]")
            if not is_valid_external_link(u):
                continue
            if u not in seen:
                seen.add(u)
                found.append(u)

    preferred_order = []
    for u in found:
        low = u.lower()
        if any(x in low for x in ["linktr.ee", "uqr.to", "beacons.ai", "lnk.bio", "taplink", "bio.site"]):
            preferred_order.append((0, u))
        else:
            preferred_order.append((1, u))

    preferred_order.sort(key=lambda x: x[0])
    return [u for _, u in preferred_order]


def analyze_facebook_external_links(facebook_url: str) -> Dict[str, Any]:
    result = {
        "facebook_primary_external_link": None,
        "facebook_bio_links_json": "[]",
    }

    html = fetch_url(facebook_url)
    if not html:
        return result

    raw_links = extract_external_links_from_facebook_html(html)
    resolved_links = resolve_external_links(raw_links, max_items=20)
    clean_resolved = [u for u in resolved_links if is_valid_external_link(u)]

    if clean_resolved:
        result["facebook_primary_external_link"] = clean_resolved[0]
        result["facebook_bio_links_json"] = json.dumps(clean_resolved, ensure_ascii=False)

    return result


# -----------------------------
# Search result extraction
# -----------------------------
def extract_business_profile_candidates_from_results(
    results: List[SearchResult],
    name: str,
    city: str,
    provider_name: str,
) -> Dict[str, List[LinkCandidate]]:
    website_candidates: List[LinkCandidate] = []
    facebook_candidates: List[LinkCandidate] = []
    instagram_candidates: List[LinkCandidate] = []
    tiktok_candidates: List[LinkCandidate] = []
    threads_candidates: List[LinkCandidate] = []
    x_candidates: List[LinkCandidate] = []
    youtube_candidates: List[LinkCandidate] = []
    linktree_candidates: List[LinkCandidate] = []
    uqrto_candidates: List[LinkCandidate] = []
    directory_candidates: List[LinkCandidate] = []

    for r in results:
        raw_url = getattr(r, "url", "") or ""
        url = ensure_http(raw_url.split("?")[0].rstrip("/"))
        title = getattr(r, "title", "") or ""
        snippet = getattr(r, "snippet", "") or ""
        domain = base_domain(url)

        if not url or not domain:
            continue

        cleaned_gmaps = clean_google_maps_url(url)
        if cleaned_gmaps:
            score = score_text_candidate(name, city, title, snippet, cleaned_gmaps)
            directory_candidates.append(
                LinkCandidate(
                    url=cleaned_gmaps,
                    score=min(1.0, score + 0.18),
                    source=f"search_router:{provider_name}",
                    reason=f"google maps result ({provider_name}): title='{title}'",
                )
            )
            continue

        if "instagram.com" in domain:
            cleaned = clean_social_url(url)
            if cleaned and not looks_like_non_business_instagram(cleaned, title, snippet):
                score = score_social_candidate(name, city, title, snippet, cleaned)
                instagram_candidates.append(
                    LinkCandidate(cleaned, score, f"search_router:{provider_name}", f"instagram result ({provider_name}): title='{title}'")
                )
            continue

        if "facebook.com" in domain:
            cleaned = clean_social_url(url)
            if cleaned:
                score = score_social_candidate(name, city, title, snippet, cleaned)
                facebook_candidates.append(
                    LinkCandidate(cleaned, score, f"search_router:{provider_name}", f"facebook result ({provider_name}): title='{title}'")
                )
            continue

        if "tiktok.com" in domain:
            cleaned = clean_tiktok_url(url)
            if cleaned and not looks_like_non_business_tiktok(cleaned, title, snippet):
                score = score_social_candidate(name, city, title, snippet, cleaned)
                tiktok_candidates.append(
                    LinkCandidate(cleaned, score, f"search_router:{provider_name}", f"tiktok result ({provider_name}): title='{title}'")
                )
            continue

        if "threads.net" in domain or "threads.com" in domain:
            cleaned = clean_threads_url(url)
            if cleaned:
                score = score_social_candidate(name, city, title, snippet, cleaned)
                threads_candidates.append(
                    LinkCandidate(cleaned, score, f"search_router:{provider_name}", f"threads result ({provider_name}): title='{title}'")
                )
            continue

        if "x.com" in domain or "twitter.com" in domain:
            cleaned = clean_x_url(url)
            if cleaned:
                score = score_social_candidate(name, city, title, snippet, cleaned)
                x_candidates.append(
                    LinkCandidate(cleaned, score, f"search_router:{provider_name}", f"x result ({provider_name}): title='{title}'")
                )
            continue

        if "youtube.com" in domain or "youtu.be" in domain:
            cleaned = clean_youtube_url(url)
            if cleaned:
                score = score_social_candidate(name, city, title, snippet, cleaned)
                youtube_candidates.append(
                    LinkCandidate(cleaned, score, f"search_router:{provider_name}", f"youtube result ({provider_name}): title='{title}'")
                )
            continue

        if "linktr.ee" in domain:
            cleaned = clean_linktree_url(url)
            if cleaned:
                score = score_social_candidate(name, city, title, snippet, cleaned)
                linktree_candidates.append(
                    LinkCandidate(cleaned, score, f"search_router:{provider_name}", f"linktree result ({provider_name}): title='{title}'")
                )
            continue

        if "uqr.to" in domain:
            cleaned = clean_uqrto_url(url)
            if cleaned:
                score = score_social_candidate(name, city, title, snippet, cleaned)
                uqrto_candidates.append(
                    LinkCandidate(cleaned, score, f"search_router:{provider_name}", f"uqr.to result ({provider_name}): title='{title}'")
                )
            continue

        if is_directory_domain(domain):
            score = score_text_candidate(name, city, title, snippet, url)
            directory_candidates.append(
                LinkCandidate(url, score, f"search_router:{provider_name}", f"directory result ({provider_name}): title='{title}'")
            )
            continue

        if is_non_official_website_domain(domain):
            continue

        score = score_text_candidate(name, city, title, snippet, url)
        if is_strong_official_domain(name, url):
            score += 0.15

        website_candidates.append(
            LinkCandidate(url, min(1.0, score), f"search_router:{provider_name}", f"website result ({provider_name}): title='{title}'")
        )

    return {
        "website": website_candidates,
        "facebook": facebook_candidates,
        "instagram": instagram_candidates,
        "tiktok": tiktok_candidates,
        "threads": threads_candidates,
        "x": x_candidates,
        "youtube": youtube_candidates,
        "linktree": linktree_candidates,
        "uqrto": uqrto_candidates,
        "directory": directory_candidates,
    }


# -----------------------------
# Search router fallbacks
# -----------------------------
def find_profiles_via_search_router(
    name: str,
    city: str,
    country: str,
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    evidence: List[str] = []
    providers_tried: List[str] = []

    try:
        router = build_router()
    except Exception as e:
        return {
            "website": None,
            "facebook": None,
            "instagram": None,
            "tiktok": None,
            "threads": None,
            "x": None,
            "youtube": None,
            "linktree": None,
            "uqrto": None,
            "directory": [],
        }, [f"Router init failed: {e}"], providers_tried

    provider_order = ["serpapi", "serper", "tavily"]

    best_website = None
    best_facebook = None
    best_instagram = None
    best_tiktok = None
    best_threads = None
    best_x = None
    best_youtube = None
    best_linktree = None
    best_uqrto = None
    all_directory_candidates: List[LinkCandidate] = []

    search_names = build_search_name_set(name, city)[:6]

    queries = []
    for n in search_names:
        queries.extend([
            f"{n} {city} restaurant ristorante pizzeria official website facebook instagram tiktok google maps",
            f'"{n}" "{city}" official website instagram facebook google maps',
            f"{n} {city} menu prenota delivery instagram facebook maps",
            f'"{n}" "{city}" tripadvisor',
            f'"{n}" "{city}" thefork',
        ])

    queries = dedupe_preserve_order(queries)

    for provider_name in provider_order:
        providers_tried.append(provider_name)

        for query in queries:
            response = router.search(
                query,
                country="IT",
                language="it",
                count=10,
                providers_order=[provider_name],
            )

            if not response.ok:
                evidence.append(f"Provider {provider_name} failed for '{query}': {response.error_message}")
                continue

            for n in search_names[:4]:
                candidates = extract_business_profile_candidates_from_results(
                    response.results,
                    name=n,
                    city=city,
                    provider_name=provider_name,
                )

                all_directory_candidates.extend(candidates.get("directory", []))

                best_website = keep_best_candidate(best_website, choose_best_candidate(candidates["website"], 0.38))
                best_facebook = keep_best_candidate(best_facebook, choose_best_candidate(candidates["facebook"], 0.28))
                best_instagram = keep_best_candidate(best_instagram, choose_best_candidate(candidates["instagram"], 0.24))
                best_tiktok = keep_best_candidate(best_tiktok, choose_best_candidate(candidates["tiktok"], 0.28))
                best_threads = keep_best_candidate(best_threads, choose_best_candidate(candidates["threads"], 0.25))
                best_x = keep_best_candidate(best_x, choose_best_candidate(candidates["x"], 0.25))
                best_youtube = keep_best_candidate(best_youtube, choose_best_candidate(candidates["youtube"], 0.25))
                best_linktree = keep_best_candidate(best_linktree, choose_best_candidate(candidates["linktree"], 0.20))
                best_uqrto = keep_best_candidate(best_uqrto, choose_best_candidate(candidates["uqrto"], 0.20))

        evidence.append(
            f"Provider {provider_name} cumulative best: "
            f"website={round(best_website.score, 3) if best_website else None}, "
            f"facebook={round(best_facebook.score, 3) if best_facebook else None}, "
            f"instagram={round(best_instagram.score, 3) if best_instagram else None}, "
            f"tiktok={round(best_tiktok.score, 3) if best_tiktok else None}, "
            f"directory_count={len(all_directory_candidates)}"
        )

    return {
        "website": best_website,
        "facebook": best_facebook,
        "instagram": best_instagram,
        "tiktok": best_tiktok,
        "threads": best_threads,
        "x": best_x,
        "youtube": best_youtube,
        "linktree": best_linktree,
        "uqrto": best_uqrto,
        "directory": all_directory_candidates,
    }, evidence, providers_tried


def find_instagram_via_search_router(name: str, city: str, country: str):
    return _find_single_social_via_search_router(name, city, country, "instagram", 0.25)


def find_facebook_via_search_router(name: str, city: str, country: str):
    return _find_single_social_via_search_router(name, city, country, "facebook", 0.30)


def find_tiktok_via_search_router(name: str, city: str, country: str):
    return _find_single_social_via_search_router(name, city, country, "tiktok", 0.30)


def find_google_maps_via_search_router(
    name: str,
    city: str,
    country: str,
) -> Tuple[Optional[LinkCandidate], List[str], List[str]]:
    evidence: List[str] = []
    providers_tried: List[str] = []

    try:
        router = build_router()
    except Exception as e:
        return None, [f"Router init failed for Google Maps search: {e}"], providers_tried

    search_names = build_search_name_set(name, city)[:6]

    queries = []
    for n in search_names:
        queries.extend([
            f"{n} {city} google maps",
            f"{n} {city} maps",
            f'"{n}" "{city}" "google maps"',
            f'"{n}" "{city}" "share.google"',
            f'"{n}" "{city}" "maps.app.goo.gl"',
        ])

    queries = dedupe_preserve_order(queries)
    provider_order = ["serpapi", "serper", "tavily"]
    best_candidate = None

    for provider_name in provider_order:
        providers_tried.append(provider_name)

        for query in queries:
            response = router.search(
                query,
                country="IT",
                language="it",
                count=10,
                providers_order=[provider_name],
            )

            if not response.ok:
                evidence.append(f"Google Maps {provider_name} failed for '{query}': {response.error_message}")
                continue

            candidates: List[LinkCandidate] = []

            for r in response.results:
                url = ensure_http((getattr(r, "url", "") or "").split("?")[0].rstrip("/"))
                title = getattr(r, "title", "") or ""
                snippet = getattr(r, "snippet", "") or ""

                cleaned_gmaps = clean_google_maps_url(url)
                if not cleaned_gmaps:
                    continue

                best_score = 0.0
                best_name = ""
                for n in search_names[:4]:
                    score = score_text_candidate(n, city, title, snippet, url)
                    if "share.google" in cleaned_gmaps.lower() or "maps.app.goo.gl" in cleaned_gmaps.lower():
                        score += 0.08
                    if "maps" in title.lower():
                        score += 0.05
                    if score > best_score:
                        best_score = score
                        best_name = n

                candidates.append(
                    LinkCandidate(
                        url=cleaned_gmaps,
                        score=min(1.0, best_score + 0.15),
                        source=f"search_router_google_maps:{provider_name}",
                        reason=f"Google Maps query='{query}' via {provider_name}, matched_name='{best_name}', title='{title}'",
                    )
                )

            provider_best = choose_best_candidate(candidates, min_score=0.30)
            best_candidate = keep_best_candidate(best_candidate, provider_best)

        evidence.append(
            f"Google Maps provider {provider_name} best={round(best_candidate.score, 3) if best_candidate else None}"
        )

    return best_candidate, evidence, providers_tried

def find_website_via_search_router(
    name: str,
    city: str,
    country: str,
) -> Tuple[Optional[LinkCandidate], List[str], List[str]]:
    evidence: List[str] = []
    providers_tried: List[str] = []

    try:
        router = build_router()
    except Exception as e:
        return None, [f"Router init failed for website search: {e}"], providers_tried

    search_names = build_search_name_set(name, city)[:6]

    queries = []
    for n in search_names:
        queries.extend([
            f"{n} {city} official website",
            f"{n} {city} ristorante official website",
            f"{n} {city} pizzeria official website",
            f'"{n}" "{city}" official website',
            f'"{n}" "{city}" sito ufficiale',
            f'"{n}" "{city}" menu',
        ])

    queries = dedupe_preserve_order(queries)
    provider_order = ["serpapi", "serper", "tavily"]
    best_candidate = None

    for provider_name in provider_order:
        providers_tried.append(provider_name)

        for query in queries:
            response = router.search(
                query,
                country="IT",
                language="it",
                count=10,
                providers_order=[provider_name],
            )

            if not response.ok:
                evidence.append(f"Website {provider_name} failed for '{query}': {response.error_message}")
                continue

            candidates: List[LinkCandidate] = []
            for r in response.results:
                url = ensure_http((getattr(r, "url", "") or "").split("?")[0].rstrip("/"))
                domain = base_domain(url)
                if not domain or is_non_official_website_domain(domain) or is_directory_domain(domain):
                    continue

                best_score = 0.0
                best_name = ""
                for n in search_names[:4]:
                    score = score_text_candidate(n, city, getattr(r, "title", ""), getattr(r, "snippet", ""), url)
                    if score > best_score:
                        best_score = score
                        best_name = n

                candidates.append(
                    LinkCandidate(
                        url=url,
                        score=best_score,
                        source=f"search_router_website:{provider_name}",
                        reason=f"Website query='{query}' via {provider_name}, matched_name='{best_name}', title='{getattr(r, 'title', '')}'",
                    )
                )

            provider_best = choose_best_candidate(candidates, min_score=0.38)
            best_candidate = keep_best_candidate(best_candidate, provider_best)

        evidence.append(
            f"Website provider {provider_name} best={round(best_candidate.score, 3) if best_candidate else None}"
        )

    return best_candidate, evidence, providers_tried


def find_directory_platforms_via_search_router(
    name: str,
    city: str,
    country: str,
) -> Tuple[List[LinkCandidate], List[str], List[str]]:
    evidence: List[str] = []
    providers_tried: List[str] = []

    try:
        router = build_router()
    except Exception as e:
        return [], [f"Router init failed for directory search: {e}"], providers_tried

    search_names = build_search_name_set(name, city)[:6]
    provider_order = ["serpapi", "serper", "tavily"]
    directories = ["tripadvisor", "thefork", "restaurantguru", "google maps"]

    all_candidates: List[LinkCandidate] = []

    for provider_name in provider_order:
        providers_tried.append(provider_name)

        for n in search_names:
            for d in directories:
                query = f'"{n}" "{city}" "{d}"'
                response = router.search(
                    query,
                    country="IT",
                    language="it",
                    count=10,
                    providers_order=[provider_name],
                )

                if not response.ok:
                    evidence.append(f"Directory {provider_name} failed for '{query}': {response.error_message}")
                    continue

                candidates = extract_business_profile_candidates_from_results(
                    response.results,
                    name=n,
                    city=city,
                    provider_name=provider_name,
                )
                all_candidates.extend(candidates.get("directory", []))

    evidence.append(f"Dedicated directory search produced {len(all_candidates)} candidates")
    return all_candidates, evidence, providers_tried


def _find_single_social_via_search_router(
    name: str,
    city: str,
    country: str,
    platform: str,
    min_score: float,
) -> Tuple[Optional[LinkCandidate], List[str], List[str]]:
    evidence: List[str] = []
    providers_tried: List[str] = []

    try:
        router = build_router()
    except Exception as e:
        return None, [f"Router init failed for {platform}: {e}"], providers_tried

    search_names = build_search_name_set(name, city)[:6]

    queries = []
    for n in search_names:
        queries.extend([
            f"{n} {city} {platform}",
            f"{n} {city} restaurant {platform}",
            f'"{n}" "{city}" {platform}',
        ])

    queries = dedupe_preserve_order(queries)
    provider_order = ["serpapi", "serper", "tavily"]
    best_candidate = None

    for provider_name in provider_order:
        providers_tried.append(provider_name)

        for query in queries:
            response = router.search(
                query,
                country="IT",
                language="it",
                count=10,
                providers_order=[provider_name],
            )

            if not response.ok:
                evidence.append(f"{platform} {provider_name} failed for '{query}': {response.error_message}")
                continue

            candidates: List[LinkCandidate] = []
            for r in response.results:
                raw_url = getattr(r, "url", "") or ""
                title = getattr(r, "title", "") or ""
                snippet = getattr(r, "snippet", "") or ""

                cleaned = None
                if platform == "instagram":
                    cleaned = clean_social_url(raw_url)
                    if cleaned and "instagram.com" not in cleaned.lower():
                        cleaned = None
                    if cleaned and looks_like_non_business_instagram(cleaned, title, snippet):
                        cleaned = None
                elif platform == "facebook":
                    cleaned = clean_social_url(raw_url)
                    if cleaned and "facebook.com" not in cleaned.lower():
                        cleaned = None
                elif platform == "tiktok":
                    cleaned = clean_tiktok_url(raw_url)
                    if cleaned and looks_like_non_business_tiktok(cleaned, title, snippet):
                        cleaned = None

                if not cleaned:
                    continue

                best_score = 0.0
                best_name = ""
                for n in search_names[:4]:
                    score = score_social_candidate(n, city, title, snippet, raw_url)
                    if score > best_score:
                        best_score = score
                        best_name = n

                candidates.append(
                    LinkCandidate(
                        url=cleaned,
                        score=best_score,
                        source=f"search_router_{platform}:{provider_name}",
                        reason=f"{platform} query='{query}' via {provider_name}, matched_name='{best_name}', title='{title}'",
                    )
                )

            provider_best = choose_best_candidate(candidates, min_score=min_score)
            best_candidate = keep_best_candidate(best_candidate, provider_best)

        evidence.append(
            f"{platform} provider {provider_name} best={round(best_candidate.score, 3) if best_candidate else None}"
        )

    return best_candidate, evidence, providers_tried


# -----------------------------
# Website feature analysis
# -----------------------------
def get_internal_candidate_links(base_url: str, html: str, max_links: int = 8) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    base_host = base_domain(base_url)
    links = []
    seen = set()

    interesting_keywords = [
        "menu", "menù", "carta", "food", "book", "booking", "reserve", "reservation",
        "prenota", "prenotazione", "delivery", "takeaway", "asporto", "ordina",
        "ordine", "contact", "contatti", "about", "newsletter", "event", "evento",
        "offers", "offerte", "promo", "recensioni", "reviews", "dove-siamo", "location"
    ]

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href:
            continue

        full = ensure_http(urljoin(base_url, href))
        host = base_domain(full)

        if host != base_host:
            continue

        low = full.lower()
        text = (a.get_text(" ", strip=True) or "").lower()
        combined = low + " " + text

        if any(k in combined for k in interesting_keywords):
            if full not in seen:
                seen.add(full)
                links.append(full)

        if len(links) >= max_links:
            break

    return links


def analyze_single_html(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True).lower()
    links = [a.get("href", "").lower() for a in soup.find_all("a", href=True)]
    buttons = [b.get_text(" ", strip=True).lower() for b in soup.find_all(["button", "a"])]
    forms = soup.find_all("form")
    iframes = [i.get("src", "").lower() for i in soup.find_all("iframe", src=True)]

    all_content = " ".join([text, " ".join(links), " ".join(buttons), " ".join(iframes)])

    def has_any(keywords: List[str]) -> bool:
        return any(k in all_content for k in keywords)

    menu_keywords = ["menu", "menù", "our-menu", "food-menu", "carta", "scarica il menu"]
    booking_keywords = ["book", "booking", "reserve", "reservation", "prenota", "prenotazione", "thefork", "opentable", "quandoo", "resmio"]
    delivery_keywords = ["delivery", "takeaway", "asporto", "consegna", "a domicilio", "deliveroo", "ubereats", "justeat", "glovo", "ordina online"]
    directions_keywords = ["dove siamo", "come raggiungerci", "get directions", "directions", "maps", "mappa", "google maps"]
    reviews_keywords = ["reviews", "recensioni", "tripadvisor", "google reviews", "cosa dicono", "dicono di noi", "testimonials", "testimonial"]
    offers_keywords = ["offerta", "offerte", "promo", "promozione", "promotions", "special offer", "discount", "sconto"]
    events_keywords = ["eventi", "evento", "events", "live music", "serata", "serate", "calendar", "upcoming"]
    uniqueness_keywords = [
        "tradizione", "tradizionale", "ingredienti freschi", "ingredienti locali",
        "chef", "since", "dal", "specialità", "specialita", "signature",
        "family run", "artigianale", "homemade", "fatto in casa", "territorio",
        "nostra storia", "about us", "chi siamo", "filosofia", "unique", "experience"
    ]

    contact_present = (
        has_any(["contact", "contatti", "tel:", "mailto:", "whatsapp", "dove siamo"])
        or bool(re.search(r"\+?\d[\d\-\s()]{6,}", text))
        or bool(re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text))
    )

    data_capture_present = (
        len(forms) > 0
        or has_any(["newsletter", "subscribe", "sign up", "join", "iscriviti", "contact form", "book now", "prenota ora"])
    )

    menu_present = has_any(menu_keywords) or ".pdf" in all_content
    menu_quality = "missing"
    if menu_present:
        descriptive_menu_signals = [
            "ingredienti", "ingredients", "allergeni", "allergens", "descrizione",
            "description", "chef", "specialità", "specialita", "grams", "€"
        ]
        if sum(1 for s in descriptive_menu_signals if s in all_content) >= 2:
            menu_quality = "described"
        else:
            menu_quality = "basic_list"

    unique_hits = [k for k in uniqueness_keywords if k in all_content]

    return {
        "menu_present": menu_present,
        "menu_quality": menu_quality,
        "booking_present": has_any(booking_keywords),
        "delivery_present": has_any(delivery_keywords),
        "data_capture_present": data_capture_present,
        "contact_present": contact_present,
        "directions_present": has_any(directions_keywords),
        "reviews_visible": has_any(reviews_keywords),
        "offers_promos_present": has_any(offers_keywords),
        "events_present": has_any(events_keywords),
        "unique_value_present": len(unique_hits) >= 2,
        "unique_value_examples": unique_hits[:8],
    }


def analyze_website_features(website_url: str) -> Dict[str, Any]:
    result = {
        "menu_present": False,
        "menu_quality": "missing",
        "booking_present": False,
        "delivery_present": False,
        "data_capture_present": False,
        "contact_present": False,
        "directions_present": False,
        "reviews_visible": False,
        "offers_promos_present": False,
        "events_present": False,
        "unique_value_present": False,
        "unique_value_examples": [],
    }

    homepage_html = fetch_url(website_url)
    if not homepage_html:
        return result

    homepage_result = analyze_single_html(homepage_html)

    for k in [
        "booking_present", "delivery_present", "data_capture_present", "contact_present",
        "directions_present", "reviews_visible", "offers_promos_present", "events_present",
        "unique_value_present"
    ]:
        result[k] = result[k] or homepage_result[k]

    if homepage_result["menu_present"]:
        result["menu_present"] = True
        result["menu_quality"] = homepage_result["menu_quality"]

    result["unique_value_examples"] = list(homepage_result.get("unique_value_examples", []))

    candidate_links = get_internal_candidate_links(website_url, homepage_html, max_links=10)
    visited: Set[str] = set()

    for link in candidate_links:
        if link in visited:
            continue
        visited.add(link)

        html = fetch_url(link)
        time.sleep(0.3)
        if not html:
            continue

        sub_result = analyze_single_html(html)

        for k in [
            "booking_present", "delivery_present", "data_capture_present", "contact_present",
            "directions_present", "reviews_visible", "offers_promos_present", "events_present",
            "unique_value_present"
        ]:
            result[k] = result[k] or sub_result[k]

        if sub_result["menu_present"]:
            result["menu_present"] = True
            if result["menu_quality"] != "described":
                result["menu_quality"] = sub_result["menu_quality"]

        for item in sub_result.get("unique_value_examples", []):
            if item not in result["unique_value_examples"]:
                result["unique_value_examples"].append(item)

    return result


def classify_website_type(features: Dict[str, Any]) -> Tuple[str, float]:
    score = 0.0

    if features.get("menu_present"):
        score += 1.0
    if features.get("booking_present"):
        score += 1.0
    if features.get("delivery_present"):
        score += 1.0
    if features.get("data_capture_present"):
        score += 1.0
    if features.get("contact_present"):
        score += 1.0
    if features.get("directions_present"):
        score += 0.5
    if features.get("reviews_visible"):
        score += 0.5
    if features.get("offers_promos_present"):
        score += 0.5
    if features.get("events_present"):
        score += 0.5
    if features.get("unique_value_present"):
        score += 1.0
    if features.get("menu_quality") == "described":
        score += 1.0

    completeness_score = round(min(score / 8.0, 1.0), 3)

    if completeness_score >= 0.75:
        return "complete", completeness_score
    elif completeness_score >= 0.40:
        return "showcase_plus", completeness_score
    return "basic_showcase", completeness_score


def build_website_strengths_weaknesses(features: Dict[str, Any], creator: str = "") -> Tuple[List[str], List[str]]:
    strengths = []
    weaknesses = []

    if features.get("menu_present"):
        if features.get("menu_quality") == "described":
            strengths.append("Menu is present and described")
        else:
            strengths.append("Menu is present")
    else:
        weaknesses.append("No menu found")

    if features.get("booking_present"):
        strengths.append("Online booking available")
    else:
        weaknesses.append("No online booking found")

    if features.get("delivery_present"):
        strengths.append("Delivery or takeaway visible")
    else:
        weaknesses.append("No delivery signal found")

    if features.get("data_capture_present"):
        strengths.append("Customer data capture is present")
    else:
        weaknesses.append("No customer data capture found")

    if features.get("contact_present"):
        strengths.append("Contact information is visible")
    else:
        weaknesses.append("Contact information is weak or missing")

    if features.get("directions_present"):
        strengths.append("Directions/location info is visible")

    if features.get("reviews_visible"):
        strengths.append("Reviews/testimonials are visible")

    if features.get("offers_promos_present"):
        strengths.append("Offers or promotions are visible")

    if features.get("events_present"):
        strengths.append("Events or special activities are visible")

    if features.get("unique_value_present"):
        strengths.append("Website communicates unique identity")
    else:
        weaknesses.append("Unique restaurant identity is not clearly explained")

    if creator:
        strengths.append(f"Website creator/platform identified: {creator}")

    return strengths[:8], weaknesses[:8]


# -----------------------------
# OSM
# -----------------------------
def overpass_search(name: str, city: str, country: str) -> Dict[str, Any]:
    name_escaped = name.replace('"', '\\"')
    city_escaped = city.replace('"', '\\"')
    country_escaped = country.replace('"', '\\"')

    query = f"""
    [out:json][timeout:30];
    area["name"="{country_escaped}"]["boundary"="administrative"]->.country;
    (
      area["name"="{city_escaped}"]["boundary"="administrative"](area.country)->.cityArea;
    );
    (
      node(area.cityArea)["name"~"{name_escaped}", i]["amenity"];
      way(area.cityArea)["name"~"{name_escaped}", i]["amenity"];
      relation(area.cityArea)["name"~"{name_escaped}", i]["amenity"];
      node(area.cityArea)["name"~"{name_escaped}", i]["shop"];
      way(area.cityArea)["name"~"{name_escaped}", i]["shop"];
      relation(area.cityArea)["name"~"{name_escaped}", i]["shop"];
    );
    out tags center 50;
    """

    errors = []

    for base_url in OVERPASS_URLS:
        for attempt in range(2):
            try:
                r = requests.post(
                    base_url,
                    data=query.encode("utf-8"),
                    headers={"User-Agent": UA},
                    timeout=45,
                )
                r.raise_for_status()
                data = r.json()
                if data.get("elements"):
                    return data
                return {"elements": [], "error": "OSM no matches"}

            except requests.exceptions.Timeout as e:
                errors.append(f"{base_url} timeout attempt {attempt + 1}: {str(e)}")
            except requests.exceptions.RequestException as e:
                errors.append(f"{base_url} request error attempt {attempt + 1}: {str(e)}")
            except Exception as e:
                errors.append(f"{base_url} unknown error attempt {attempt + 1}: {str(e)}")

            time.sleep(2)

    return {"elements": [], "error": " | ".join(errors)}


def is_probably_restaurant(tags: Dict[str, str]) -> bool:
    amenity = (tags.get("amenity") or "").lower()
    shop = (tags.get("shop") or "").lower()
    tourism = (tags.get("tourism") or "").lower()
    return amenity in {"restaurant", "fast_food", "cafe", "bar", "pub"} or shop == "bakery" or tourism == "hotel"


def pick_best_osm_candidate(
    name: str,
    city: str,
    elements: List[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], float, List[str]]:
    target = norm(name)
    evidence = []
    best = None
    best_score = 0.0

    for el in elements:
        tags = el.get("tags", {}) or {}
        el_name = tags.get("name") or ""
        if not el_name:
            continue

        sim = fuzz.token_set_ratio(norm(el_name), target)
        boost = 0

        if tags.get("addr:city") and norm(tags.get("addr:city")) == norm(city):
            boost += 8
        if tags.get("phone") or tags.get("contact:phone"):
            boost += 3
        if tags.get("website") or tags.get("contact:website"):
            boost += 6
        if tags.get("facebook") or tags.get("contact:facebook") or tags.get("instagram") or tags.get("contact:instagram"):
            boost += 4

        if not is_probably_restaurant(tags):
            sim *= 0.85

        score = (sim + boost) / 112.0

        if score > best_score:
            best_score = score
            best = el

    if best:
        tags = best.get("tags", {}) or {}
        evidence.append(f"OSM best match name='{tags.get('name', '')}', score={best_score:.2f}")
        if tags.get("addr:city"):
            evidence.append(f"OSM addr:city='{tags.get('addr:city')}'")

    return best, best_score, evidence


# -----------------------------
# Domain guessing
# -----------------------------
def guess_possible_domains(name: str, country: str = "Italy") -> List[str]:
    candidates = []
    tlds = [".it", ".com"]

    for variant in generate_name_variants(name):
        cleaned = re.sub(r"[^a-z0-9\s-]", "", variant)
        words = cleaned.split()
        if not words:
            continue

        joined = "".join(words)
        dashed = "-".join(words)

        for tld in tlds:
            candidates.append(f"https://www.{joined}{tld}")
            candidates.append(f"https://{joined}{tld}")
            candidates.append(f"https://www.{dashed}{tld}")
            candidates.append(f"https://{dashed}{tld}")

    seen = set()
    unique = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    return unique


def find_working_domain(name: str, city: str, country: str) -> Tuple[Optional[str], List[str]]:
    evidence = []
    domains = guess_possible_domains(name, country)

    best_url = None
    best_score = -1.0
    best_reason = ""

    for url in domains:
        html = fetch_url(url)
        time.sleep(0.5)

        if not html:
            continue

        domain = base_domain(url)
        if is_non_official_website_domain(domain):
            continue

        ok, score, reason = validate_business_page_content(name, city, url, html)

        if ok and score >= 0.80:
            evidence.append(
                f"Guessed working domain accepted: {url} "
                f"(score={score}, reason={reason})"
            )
            return url, evidence

        if score > best_score:
            best_score = score
            best_url = url
            best_reason = reason

        evidence.append(
            f"Guessed domain rejected: {url} "
            f"(score={score}, reason={reason})"
        )

    if best_url:
        evidence.append(
            f"Best guessed domain rejected overall: {best_url} "
            f"(score={best_score}, reason={best_reason})"
        )

    return None, evidence or ["No guessed domain worked"]


# -----------------------------
# Entity validation
# -----------------------------

def compute_restaurant_match_score(
    name: str,
    city: str,
    website: Optional[str],
    instagram: Optional[str],
    facebook: Optional[str],
    tiktok: Optional[str] = None,
    x: Optional[str] = None,
) -> Tuple[float, str]:
    score = 0.0
    reasons: List[str] = []

    restaurant_keywords = [
        "restaurant", "ristorante", "pizzeria", "trattoria", "osteria",
        "cucina", "menu", "menù", "prenota", "reservation", "booking",
        "food", "delivery", "takeaway", "asporto", "bar", "cafe", "pub",
        "pizza", "pasta", "wine", "vino", "chef", "dolci", "dessert"
    ]

    negative_keywords = [
        "makeup", "hair stylist", "nails", "photographer", "blogger",
        "fashion", "real estate", "lawyer", "fitness", "gym", "dentist",
        "clinic", "beauty", "cosmetics", "wedding planner"
    ]

    if website:
        html = fetch_url(website)
        time.sleep(0.3)

        if html:
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ", strip=True).lower()
            title = soup.title.get_text(" ", strip=True).lower() if soup.title else ""
            combined = f"{title} {text}"

            positive_hits = sum(1 for k in restaurant_keywords if k in combined)
            negative_hits = sum(1 for k in negative_keywords if k in combined)

            if positive_hits >= 3:
                score += 0.35
                reasons.append("Website contains strong restaurant keywords")
            elif positive_hits >= 1:
                score += 0.20
                reasons.append("Website contains some restaurant keywords")

            if "menu" in combined or "menù" in combined:
                score += 0.15
                reasons.append("Website mentions menu")

            if any(k in combined for k in ["prenota", "reservation", "booking", "book a table"]):
                score += 0.10
                reasons.append("Website mentions booking")

            if any(k in combined for k in ["delivery", "takeaway", "asporto", "a domicilio"]):
                score += 0.10
                reasons.append("Website mentions delivery/takeaway")

            if negative_hits >= 2:
                score -= 0.30
                reasons.append("Website contains non-restaurant business signals")

    if instagram:
        html = fetch_url(instagram)
        time.sleep(0.2)

        if html:
            text = extract_visible_text_from_html(html).lower()
            positive_hits = sum(1 for k in restaurant_keywords if k in text)
            negative_hits = sum(1 for k in negative_keywords if k in text)

            if positive_hits >= 2:
                score += 0.15
                reasons.append("Instagram contains restaurant-related terms")

            if looks_like_address_text(text):
                score += 0.05
                reasons.append("Instagram bio looks like business address")

            if negative_hits >= 2:
                score -= 0.15
                reasons.append("Instagram looks like non-restaurant profile")

    if facebook:
        html = fetch_url(facebook)
        time.sleep(0.2)

        if html:
            text = extract_visible_text_from_html(html).lower()
            positive_hits = sum(1 for k in restaurant_keywords if k in text)
            negative_hits = sum(1 for k in negative_keywords if k in text)

            if positive_hits >= 2:
                score += 0.15
                reasons.append("Facebook contains restaurant-related terms")

            if any(k in text for k in ["orari", "hours", "open now", "oggi aperto"]):
                score += 0.05
                reasons.append("Facebook has business opening hours")

            if negative_hits >= 2:
                score -= 0.15
                reasons.append("Facebook looks like non-restaurant page")

    if tiktok:
        score += 0.03
        reasons.append("TikTok profile exists")

    if x:
        score += 0.02
        reasons.append("X profile exists")

    name_n = normalize_for_match(name)
    if any(w in name_n for w in ["ristorante", "pizzeria", "trattoria", "osteria", "pub", "bar"]):
        score += 0.15
        reasons.append("Business name contains restaurant-related word")

    score = max(0.0, min(score, 1.0))
    return score, " | ".join(dict.fromkeys(reasons))



def analyze_basic_social_kpis(res: ResolveResult) -> Dict[str, Any]:
    """
    Builds easy social KPIs that do not require Meta API permissions.
    Uses already-collected profile signals and visible-text heuristics only.
    """
    score = 0.0
    reasons: List[str] = []

    has_fb = bool(res.facebook)
    has_ig = bool(res.instagram)

    if has_fb:
        score += 0.15
        reasons.append("Facebook page found")

    if has_ig:
        score += 0.15
        reasons.append("Instagram page found")

    if res.fb_description_present:
        score += 0.10
        reasons.append("Facebook description present")

    if res.fb_hours_present:
        score += 0.08
        reasons.append("Facebook hours present")

    if res.fb_bio_website_link_present:
        score += 0.08
        reasons.append("Facebook bio website link present")

    if res.fb_bio_instagram_link_present:
        score += 0.05
        reasons.append("Facebook links to Instagram")

    if res.ig_description_present:
        score += 0.10
        reasons.append("Instagram description present")

    if res.ig_website_link_present:
        score += 0.08
        reasons.append("Instagram website link present")

    if res.ig_bio_address_present:
        score += 0.08
        reasons.append("Instagram bio address present")

    if res.menu_present:
        score += 0.07
        reasons.append("Menu found")

    if res.booking_present:
        score += 0.03
        reasons.append("Booking found")

    if res.delivery_present:
        score += 0.03
        reasons.append("Delivery/takeaway found")

    if res.unique_value_present:
        score += 0.08
        reasons.append("Identity/uniqueness visible")

    score = max(0.0, min(score, 1.0))

    if score >= 0.70:
        content_quality_label = "good"
    elif score >= 0.40:
        content_quality_label = "basic"
    else:
        content_quality_label = "weak"

    identity_score = 0.0
    if res.unique_value_present:
        identity_score += 0.45
    if res.menu_present:
        identity_score += 0.20
    if res.ig_description_present or res.fb_description_present:
        identity_score += 0.20
    if res.website and res.website_validated:
        identity_score += 0.15

    identity_score = max(0.0, min(identity_score, 1.0))

    if identity_score >= 0.70:
        identity_label = "strong"
    elif identity_score >= 0.40:
        identity_label = "basic"
    else:
        identity_label = "weak"

    return {
        "has_facebook_page": has_fb,
        "has_instagram_page": has_ig,
        "social_content_quality_score": round(score, 3),
        "social_content_quality_label": content_quality_label,
        "social_identity_signal_score": round(identity_score, 3),
        "social_identity_signal_label": identity_label,
        "social_kpi_reason": " | ".join(dict.fromkeys(reasons)),
    }



def validate_restaurant_match(
    name: str,
    city: str,
    website: Optional[str],
    instagram: Optional[str],
    facebook: Optional[str],
    x: Optional[str] = None,
) -> Tuple[bool, str]:
    target_name = norm(name)

    restaurant_keywords = [
        "restaurant", "ristorante", "pizzeria", "trattoria", "osteria",
        "cucina", "menu", "menù", "prenota", "reservation", "booking",
        "food", "delivery", "takeaway", "asporto", "bar", "cafe", "pub",
    ]

    reasons = []
    positive_signals = 0.0

    if website:
        html = fetch_url(website)
        time.sleep(0.3)

        if html:
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ", strip=True).lower()
            title = soup.title.get_text(" ", strip=True).lower() if soup.title else ""
            combined = f"{title} {text}"

            name_in_page = target_name in combined
            city_hits = count_city_mentions(combined, city)
            restaurant_kw = any(k in combined for k in restaurant_keywords)
            conflict, conflict_city, _ = detect_conflicting_city(combined, city)

            if name_in_page:
                positive_signals += 1
            if city_hits > 0:
                positive_signals += 1
            if restaurant_kw:
                positive_signals += 1
            if name_in_page and restaurant_kw and not conflict:
                positive_signals += 1

            if not name_in_page:
                reasons.append("Website does not clearly mention the restaurant name")
            if city_hits == 0:
                reasons.append("Website does not clearly mention the target city")
            if conflict and city_hits == 0:
                reasons.append(f"Website points to another city: {conflict_city}")
            if not restaurant_kw:
                reasons.append("Website does not look like a restaurant website")
        else:
            reasons.append("Website could not be fetched for validation")

    if instagram:
        ig_url = instagram.lower()
        if any(x in ig_url for x in target_name.replace("'", "").split()):
            positive_signals += 1
        else:
            reasons.append("Instagram handle does not match restaurant name")

    if facebook:
        fb_url = facebook.lower()
        if any(x in fb_url for x in target_name.replace("'", "").split()):
            positive_signals += 1
        else:
            reasons.append("Facebook page does not match restaurant name")

    if x:
        x_url = x.lower()
        if any(tok in x_url for tok in target_name.replace("'", "").split() if len(tok) > 2):
            positive_signals += 0.5

    is_match = positive_signals >= 2

    if not is_match and not reasons:
        reasons.append("Found links do not provide enough evidence of being the restaurant")

    return is_match, " | ".join(dict.fromkeys(reasons))


# -----------------------------
# Resolver helpers
# -----------------------------
def assign_social_from_candidate(
    res: ResolveResult,
    field: str,
    score_field: str,
    reason_field: str,
    found_from_field: str,
    candidate: Optional[LinkCandidate],
):
    if not candidate:
        return
    if getattr(res, field):
        return

    setattr(res, field, ensure_http(candidate.url))
    setattr(res, score_field, candidate.score)
    setattr(res, reason_field, candidate.reason)
    setattr(res, found_from_field, candidate.source)


def enrich_from_website_html(res: ResolveResult, html: str, source_label: str, evidence: List[str]) -> None:
    links_data = extract_links_from_html(html)

    def assign_if_missing(
        field: str,
        score_field: str,
        reason_field: str,
        found_from_field: str,
        value: Optional[str],
        label: str,
    ):
        if not value:
            return

        current_value = getattr(res, field, None)
        if current_value:
            return

        setattr(res, field, value)
        setattr(res, score_field, 0.90)
        setattr(res, reason_field, f"Found directly in {source_label} HTML")
        setattr(res, found_from_field, source_label)
        evidence.append(f"{label} extracted from {source_label}")

    assign_if_missing("instagram", "instagram_score", "instagram_match_reason", "instagram_found_from", links_data.get("instagram"), "Instagram")
    assign_if_missing("facebook", "facebook_score", "facebook_match_reason", "facebook_found_from", links_data.get("facebook"), "Facebook")
    assign_if_missing("tiktok", "tiktok_score", "tiktok_match_reason", "tiktok_found_from", links_data.get("tiktok"), "TikTok")
    assign_if_missing("threads", "threads_score", "threads_match_reason", "threads_found_from", links_data.get("threads"), "Threads")
    assign_if_missing("x", "x_score", "x_match_reason", "x_found_from", links_data.get("x"), "X")
    assign_if_missing("youtube", "youtube_score", "youtube_match_reason", "youtube_found_from", links_data.get("youtube"), "YouTube")
    assign_if_missing("linktree", "linktree_score", "linktree_match_reason", "linktree_found_from", links_data.get("linktree"), "Linktree")
    assign_if_missing("uqrto", "uqrto_score", "uqrto_match_reason", "uqrto_found_from", links_data.get("uqrto"), "uqr.to")

    if res.tiktok:
        res.tiktok_present = True

    if links_data.get("google_maps") and not res.google_maps_url:
        res.google_maps_url = links_data["google_maps"]
        evidence.append(f"Google Maps extracted from {source_label}")

    res.directory_links_json = append_unique_json_list(
        res.directory_links_json,
        links_data.get("directory_links", []),
    )

    res.official_website_candidates_json = append_unique_json_list(
        res.official_website_candidates_json,
        links_data.get("official_website_candidates", []),
    )


def apply_platform_flags(res: ResolveResult) -> None:
    platforms = extract_platform_links(res.directory_links_json)

    if not res.google_maps_url:
        res.google_maps_url = platforms.get("google_maps")
    if not res.justeat_url:
        res.justeat_url = platforms.get("justeat")
    if not res.deliveroo_url:
        res.deliveroo_url = platforms.get("deliveroo")
    if not res.thefork_url:
        res.thefork_url = platforms.get("thefork")
    if not res.tripadvisor_url:
        res.tripadvisor_url = platforms.get("tripadvisor")
    if not res.glovo_url:
        res.glovo_url = platforms.get("glovo")
    if not res.restaurantguru_url:
        res.restaurantguru_url = platforms.get("restaurantguru")
    if not res.opentable_url:
        res.opentable_url = platforms.get("opentable")
    if not res.quandoo_url:
        res.quandoo_url = platforms.get("quandoo")

    res.has_google_maps = bool(res.google_maps_url)
    res.has_justeat = bool(res.justeat_url)
    res.has_deliveroo = bool(res.deliveroo_url)
    res.has_thefork = bool(res.thefork_url)
    res.has_tripadvisor = bool(res.tripadvisor_url)
    res.has_glovo = bool(res.glovo_url)
    res.has_restaurantguru = bool(res.restaurantguru_url)
    res.has_opentable = bool(res.opentable_url)
    res.has_quandoo = bool(res.quandoo_url)


def absorb_external_links(
    res: ResolveResult,
    links: List[str],
    source_label: str,
    name: str,
    city: str,
    evidence: List[str],
) -> None:
    for link in links:
        if not is_valid_external_link(link):
            continue

        domain = base_domain(link)

        cleaned_gmaps = clean_google_maps_url(link)
        if cleaned_gmaps:
            if not res.google_maps_url:
                res.google_maps_url = cleaned_gmaps
                evidence.append(f"Google Maps found in {source_label}")
            res.directory_links_json = append_unique_json_list(res.directory_links_json, [cleaned_gmaps])
            continue

        cleaned_threads = clean_threads_url(link)
        if cleaned_threads and not res.threads:
            res.threads = cleaned_threads
            res.threads_score = 0.90
            res.threads_match_reason = f"Found in {source_label}"
            res.threads_found_from = source_label
            evidence.append(f"Threads found in {source_label}")
            continue

        cleaned_x = clean_x_url(link)
        if cleaned_x and not res.x:
            res.x = cleaned_x
            res.x_score = 0.90
            res.x_match_reason = f"Found in {source_label}"
            res.x_found_from = source_label
            evidence.append(f"X found in {source_label}")
            continue

        cleaned_youtube = clean_youtube_url(link)
        if cleaned_youtube and not res.youtube:
            res.youtube = cleaned_youtube
            res.youtube_score = 0.90
            res.youtube_match_reason = f"Found in {source_label}"
            res.youtube_found_from = source_label
            evidence.append(f"YouTube found in {source_label}")
            continue

        cleaned_linktree = clean_linktree_url(link)
        if cleaned_linktree and not res.linktree:
            res.linktree = cleaned_linktree
            res.linktree_score = 0.90
            res.linktree_match_reason = f"Found in {source_label}"
            res.linktree_found_from = source_label
            evidence.append(f"Linktree found in {source_label}")
            continue

        cleaned_uqrto = clean_uqrto_url(link)
        if cleaned_uqrto and not res.uqrto:
            res.uqrto = cleaned_uqrto
            res.uqrto_score = 0.90
            res.uqrto_match_reason = f"Found in {source_label}"
            res.uqrto_found_from = source_label
            evidence.append(f"uqr.to found in {source_label}")
            continue

        cleaned_social = clean_social_url(link)
        if cleaned_social:
            low = cleaned_social.lower()
            if "instagram.com" in low and not res.instagram:
                res.instagram = cleaned_social
                res.instagram_score = 0.90
                res.instagram_match_reason = f"Found in {source_label}"
                res.instagram_found_from = source_label
                evidence.append(f"Instagram found in {source_label}")
                continue
            if "facebook.com" in low and not res.facebook:
                res.facebook = cleaned_social
                res.facebook_score = 0.90
                res.facebook_match_reason = f"Found in {source_label}"
                res.facebook_found_from = source_label
                evidence.append(f"Facebook found in {source_label}")
                continue

        cleaned_tiktok = clean_tiktok_url(link)
        if cleaned_tiktok and not res.tiktok:
            res.tiktok = cleaned_tiktok
            res.tiktok_score = 0.90
            res.tiktok_match_reason = f"Found in {source_label}"
            res.tiktok_found_from = source_label
            res.tiktok_present = True
            evidence.append(f"TikTok found in {source_label}")
            continue

        if is_directory_domain(domain):
            res.directory_links_json = append_unique_json_list(res.directory_links_json, [link])
            evidence.append(f"Directory/platform link stored from {source_label}: {domain}")
            continue

        if not res.website and not is_non_official_website_domain(domain):
            temp_result = ResolveResult(name=name, city=city, country=res.country)
            temp_result.website = ensure_http(link)
            apply_website_validation(temp_result, name, city, evidence)

            if temp_result.website_validated and temp_result.website_validation_score >= 0.70:
                res.website = ensure_http(link)
                res.website_score = max(res.website_score, 0.91)
                res.website_match_reason = f"Found in {source_label}"
                res.website_found_from = source_label
                res.website_validated = True
                res.website_validation_score = temp_result.website_validation_score
                res.website_validation_reason = temp_result.website_validation_reason
                evidence.append(f"Website assigned from {source_label}")
            else:
                res.official_website_candidates_json = append_unique_json_list(
                    res.official_website_candidates_json,
                    [ensure_http(link)],
                )
                evidence.append(f"Rejected website candidate from {source_label}: {link}")


def apply_website_creator_detection(res: ResolveResult, html: str, evidence: List[str]) -> None:
    creator_info = detect_website_creator(html, res.website or "")
    res.website_creator = creator_info["website_creator"] or None
    res.website_creator_type = creator_info["website_creator_type"] or None
    res.website_creator_confidence = creator_info["website_creator_confidence"] or None
    res.website_creator_source = creator_info["website_creator_source"] or None
    res.website_platform = creator_info["website_platform"] or None

    if res.website_creator or res.website_platform:
        evidence.append(
            f"Website creator detection: creator={res.website_creator}, "
            f"type={res.website_creator_type}, "
            f"confidence={res.website_creator_confidence}, "
            f"source={res.website_creator_source}, "
            f"platform={res.website_platform}"
        )


def enrich_from_valid_website(res: ResolveResult, evidence: List[str], source_label: str) -> None:
    if not res.website or not res.website_validated:
        return

    html = fetch_url(res.website)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)
    if not html:
        return

    enrich_from_website_html(res, html, source_label, evidence)
    apply_website_creator_detection(res, html, evidence)


def try_upgrade_website_from_candidates(res: ResolveResult, name: str, city: str, evidence: List[str]) -> None:
    if res.website and res.website_validated and res.website_validation_score >= 0.72:
        return

    candidates = json_list(res.official_website_candidates_json)
    if not candidates:
        return

    best_url = None
    best_score = -1.0
    best_reason = ""

    for url in candidates:
        details = website_validation_details(name, city, url)
        score = details.get("score", 0.0)

        if details.get("is_valid") and score > best_score:
            best_url = url
            best_score = score
            best_reason = details.get("reason", "")

    if best_url and best_score >= 0.74:
        res.website = best_url
        res.website_score = max(res.website_score, best_score)
        res.website_validated = True
        res.website_validation_score = best_score
        res.website_validation_reason = best_reason
        res.website_match_reason = "Upgraded from website candidates"
        res.website_found_from = "website_candidates"
        evidence.append(f"Website upgraded from candidates: {best_url} (score={best_score})")

# -----------------------------
# Main resolver
# -----------------------------
def resolve_one(name: str, city: str, country: str) -> ResolveResult:
    res = ResolveResult(name=name, city=city, country=country)
    evidence: List[str] = []

    # NEW: router instance for smarter Google Maps resolution
    try:
        router = build_router()
    except Exception as e:
        router = None
        evidence.append(f"Router init failed inside resolve_one: {e}")

    search_names = extract_candidate_business_names(name, city)
    restaurant_priority_words = ["ristorante", "pizzeria", "osteria", "trattoria", "bar", "cafe", "pub"]
    primary_search_name = search_names[0] if search_names else name

    for candidate in search_names:
        c_low = candidate.lower()
        if any(word in c_low for word in restaurant_priority_words):
            primary_search_name = candidate
            break

    evidence.append(f"Input name='{name}'")
    evidence.append(f"Primary search name='{primary_search_name}'")
    if len(search_names) > 1:
        evidence.append(f"Search candidates={search_names}")

    osm = overpass_search(primary_search_name, city, country)
    elements = osm.get("elements", []) or []

    if elements:
        best, score, ev = pick_best_osm_candidate(primary_search_name, city, elements)
        evidence += ev

        if best and score >= 0.55:
            tags = best.get("tags", {}) or {}
            website = tags.get("contact:website") or tags.get("website")
            instagram = tags.get("contact:instagram") or tags.get("instagram")
            facebook = tags.get("contact:facebook") or tags.get("facebook")
            gmaps = tags.get("google_maps") or tags.get("contact:google_maps")

            if website:
                res.website = ensure_http(website)
                res.website_score = max(res.website_score, 0.95)
                res.website_match_reason = "Found in OSM tags"
                res.website_found_from = "osm"
                apply_website_validation(res, name, city, evidence)

            if instagram:
                res.instagram = ensure_http(instagram)
                res.instagram_score = max(res.instagram_score, 0.95)
                res.instagram_match_reason = "Found in OSM tags"
                res.instagram_found_from = "osm"

            if facebook:
                res.facebook = ensure_http(facebook)
                res.facebook_score = max(res.facebook_score, 0.95)
                res.facebook_match_reason = "Found in OSM tags"
                res.facebook_found_from = "osm"

            if gmaps:
                cleaned = clean_google_maps_url(gmaps)
                if cleaned:
                    res.google_maps_url = cleaned
                    res.directory_links_json = append_unique_json_list(res.directory_links_json, [cleaned])
                    evidence.append(f"Google Maps found in OSM tags: {cleaned}")

            res.confidence = max(res.confidence, score)
            res.source = "osm"
        else:
            evidence.append("OSM candidates found but confidence below threshold")
    else:
        evidence.append(f"OSM error: {osm['error']}" if osm.get("error") else "OSM no matches")

    if res.website and res.website_validated:
        enrich_from_valid_website(res, evidence, "website_html")
        if res.source == "osm":
            res.source = "osm+site"
            res.confidence = min(1.0, res.confidence + 0.10)



    if (not res.website) or (not res.instagram) or (not res.facebook) or (not res.tiktok):
        profiles, ev, providers_tried = find_profiles_via_search_router(primary_search_name, city, country)
        evidence += ev
        evidence.append(f"Providers tried for profile search: {providers_tried}")

        directory_candidates = profiles.get("directory", []) or []
        if directory_candidates:
            res.directory_links_json = append_unique_json_list(
                res.directory_links_json,
                [c.url for c in directory_candidates]
            )
            evidence.append(f"Stored {len(directory_candidates)} directory links from search results")

        website_candidate = profiles.get("website")
        if website_candidate and not res.website:
            candidate_website = ensure_http(website_candidate.url)

            temp_result = ResolveResult(name=name, city=city, country=country)
            temp_result.website = candidate_website
            apply_website_validation(temp_result, name, city, evidence)

            if temp_result.website_validated:
                res.website = candidate_website
                res.website_score = website_candidate.score
                res.website_match_reason = website_candidate.reason + " | accepted after website validation"
                res.website_found_from = website_candidate.source
                res.website_validated = True
                res.website_validation_score = temp_result.website_validation_score
                res.website_validation_reason = temp_result.website_validation_reason
                if res.source == "none":
                    res.source = website_candidate.source
                res.confidence = max(res.confidence, 0.72)
                evidence.append(f"Accepted router website: {candidate_website} (score={website_candidate.score:.3f})")
                enrich_from_valid_website(res, evidence, "router_website_html")
            else:
                res.official_website_candidates_json = append_unique_json_list(
                    res.official_website_candidates_json,
                    [candidate_website]
                )
                evidence.append(f"Rejected router website: {candidate_website}")

        assign_social_from_candidate(res, "instagram", "instagram_score", "instagram_match_reason", "instagram_found_from", profiles.get("instagram"))
        assign_social_from_candidate(res, "facebook", "facebook_score", "facebook_match_reason", "facebook_found_from", profiles.get("facebook"))
        assign_social_from_candidate(res, "tiktok", "tiktok_score", "tiktok_match_reason", "tiktok_found_from", profiles.get("tiktok"))
        assign_social_from_candidate(res, "threads", "threads_score", "threads_match_reason", "threads_found_from", profiles.get("threads"))
        assign_social_from_candidate(res, "x", "x_score", "x_match_reason", "x_found_from", profiles.get("x"))
        assign_social_from_candidate(res, "youtube", "youtube_score", "youtube_match_reason", "youtube_found_from", profiles.get("youtube"))
        assign_social_from_candidate(res, "linktree", "linktree_score", "linktree_match_reason", "linktree_found_from", profiles.get("linktree"))
        assign_social_from_candidate(res, "uqrto", "uqrto_score", "uqrto_match_reason", "uqrto_found_from", profiles.get("uqrto"))

        if res.tiktok:
            res.tiktok_present = True

        if any([profiles.get("instagram"), profiles.get("facebook"), profiles.get("tiktok")]) and res.source == "none":
            for key in ["instagram", "facebook", "tiktok"]:
                cand = profiles.get(key)
                if cand:
                    res.source = cand.source
                    break

        if any([profiles.get("instagram"), profiles.get("facebook"), profiles.get("tiktok")]):
            res.confidence = max(res.confidence, 0.72)

    if not res.google_maps_url or not res.tripadvisor_url or not res.thefork_url:
        directory_candidates, ev, providers_tried = find_directory_platforms_via_search_router(primary_search_name, city, country)
        evidence += ev
        evidence.append(f"Providers tried for dedicated directory search: {providers_tried}")

        if directory_candidates:
            res.directory_links_json = append_unique_json_list(
                res.directory_links_json,
                [c.url for c in directory_candidates]
            )
            evidence.append(f"Stored {len(directory_candidates)} dedicated directory links")

    if not res.instagram:
        instagram_candidate, ev, providers_tried = find_instagram_via_search_router(primary_search_name, city, country)
        evidence += ev
        evidence.append(f"Providers tried for Instagram search: {providers_tried}")

        if instagram_candidate:
            assign_social_from_candidate(
                res, "instagram", "instagram_score", "instagram_match_reason", "instagram_found_from", instagram_candidate
            )
            evidence.append(f"Instagram profile found via dedicated Instagram search (score={instagram_candidate.score:.3f})")
            if res.source == "none":
                res.source = instagram_candidate.source
            res.confidence = max(res.confidence, 0.68)

    if res.instagram:
        ig_links = analyze_instagram_external_links(res.instagram)
        res.instagram_bio_links_json = ig_links["instagram_bio_links_json"]
        res.instagram_bio_website = ig_links.get("instagram_bio_website")

        bio_links = json_list(res.instagram_bio_links_json)

        absorb_external_links(
            res=res,
            links=bio_links,
            source_label="instagram_bio",
            name=name,
            city=city,
            evidence=evidence,
        )

        ig_primary_link = ig_links["instagram_primary_external_link"]
        if ig_primary_link and is_valid_external_link(ig_primary_link):
            res.instagram_primary_external_link = ig_primary_link
            evidence.append(f"Instagram primary external link found: {ig_primary_link}")

        ig_profile_signals = analyze_instagram_profile_signals(res.instagram)
        res.ig_website_link_present = ig_profile_signals["ig_website_link_present"]
        res.ig_description_present = ig_profile_signals["ig_description_present"]
        res.ig_bio_address_present = ig_profile_signals["ig_bio_address_present"]

        evidence.append(
            "Instagram profile signals: "
            f"website_link_present={res.ig_website_link_present}, "
            f"description_present={res.ig_description_present}, "
            f"bio_address_present={res.ig_bio_address_present}"
        )

    if not res.facebook:
        facebook_candidate, ev, providers_tried = find_facebook_via_search_router(primary_search_name, city, country)
        evidence += ev
        evidence.append(f"Providers tried for Facebook search: {providers_tried}")

        if facebook_candidate:
            assign_social_from_candidate(
                res, "facebook", "facebook_score", "facebook_match_reason", "facebook_found_from", facebook_candidate
            )
            evidence.append(f"Facebook profile found via dedicated Facebook search (score={facebook_candidate.score:.3f})")
            if res.source == "none":
                res.source = facebook_candidate.source
            res.confidence = max(res.confidence, 0.68)

    if res.facebook:
        fb_links = analyze_facebook_external_links(res.facebook)
        res.facebook_bio_links_json = fb_links["facebook_bio_links_json"]

        bio_links = json_list(res.facebook_bio_links_json)

        absorb_external_links(
            res=res,
            links=bio_links,
            source_label="facebook_bio",
            name=name,
            city=city,
            evidence=evidence,
        )

        fb_primary_link = fb_links["facebook_primary_external_link"]
        if fb_primary_link and is_valid_external_link(fb_primary_link):
            res.facebook_primary_external_link = fb_primary_link
            evidence.append(f"Facebook primary external link found: {fb_primary_link}")

        fb_profile_signals = analyze_facebook_profile_signals(res.facebook)
        res.fb_website_link_present = fb_profile_signals["fb_website_link_present"]
        res.fb_description_present = fb_profile_signals["fb_description_present"]
        res.fb_hours_present = fb_profile_signals["fb_hours_present"]
        res.fb_bio_website_link_present = fb_profile_signals["fb_bio_website_link_present"]
        res.fb_bio_instagram_link_present = fb_profile_signals["fb_bio_instagram_link_present"]

        evidence.append(
            "Facebook profile signals: "
            f"website_link_present={res.fb_website_link_present}, "
            f"description_present={res.fb_description_present}, "
            f"hours_present={res.fb_hours_present}, "
            f"bio_website_link_present={res.fb_bio_website_link_present}, "
            f"bio_instagram_link_present={res.fb_bio_instagram_link_present}"
        )

    if not res.tiktok:
        tiktok_candidate, ev, providers_tried = find_tiktok_via_search_router(primary_search_name, city, country)
        evidence += ev
        evidence.append(f"Providers tried for TikTok search: {providers_tried}")

        if tiktok_candidate and tiktok_candidate.score >= 0.75:
            assign_social_from_candidate(
                res, "tiktok", "tiktok_score", "tiktok_match_reason", "tiktok_found_from", tiktok_candidate
            )
            res.tiktok_present = True
            evidence.append(f"TikTok profile found via dedicated TikTok search (score={tiktok_candidate.score:.3f})")
            if res.source == "none":
                res.source = tiktok_candidate.source
            res.confidence = max(res.confidence, 0.68)
        else:
            res.tiktok_present = False

    if not res.website:
        website_candidate, ev, providers_tried = find_website_via_search_router(primary_search_name, city, country)
        evidence += ev
        evidence.append(f"Providers tried for dedicated website search: {providers_tried}")

        if website_candidate:
            candidate_website = ensure_http(website_candidate.url)

            temp_result = ResolveResult(name=name, city=city, country=country)
            temp_result.website = candidate_website
            apply_website_validation(temp_result, name, city, evidence)

            if temp_result.website_validated:
                res.website = candidate_website
                res.website_score = max(res.website_score, website_candidate.score)
                res.website_match_reason = website_candidate.reason + " | accepted via dedicated website search"
                res.website_found_from = website_candidate.source
                res.website_validated = True
                res.website_validation_score = temp_result.website_validation_score
                res.website_validation_reason = temp_result.website_validation_reason
                evidence.append(f"Website found via dedicated website search: {candidate_website}")
                if res.source == "none":
                    res.source = website_candidate.source
                res.confidence = max(res.confidence, 0.74)

                enrich_from_valid_website(res, evidence, "dedicated_website_html")
                
        # ---------------------------------------
        # LAST PRIORITY: Guess domain
        # ---------------------------------------
    if not res.website:
        guessed_website, ev = find_working_domain(primary_search_name, city, country)
        evidence += ev

        if guessed_website:
            temp_result = ResolveResult(name=name, city=city, country=country)
            temp_result.website = guessed_website

            apply_website_validation(temp_result, name, city, evidence)

            if temp_result.website_validated:
                res.website = guessed_website
                res.website_score = max(res.website_score, 0.60)
                res.website_match_reason = "Guessed domain (fallback after router)"
                res.website_found_from = "guessed_domain"

                if res.source == "none":
                    res.source = "guessed_domain"

                res.confidence = max(res.confidence, 0.55)

                evidence.append(f"Accepted guessed domain (fallback): {guessed_website}")

                enrich_from_valid_website(res, evidence, "guessed_website_html")
            else:
                evidence.append(f"Rejected guessed domain (failed validation): {guessed_website}")

    if res.uqrto:
        resolved_uqrto = resolve_redirect_url(res.uqrto)
        evidence.append(f"Resolved uqr.to redirect: {res.uqrto} -> {resolved_uqrto}")

        absorb_external_links(
            res=res,
            links=[resolved_uqrto],
            source_label="uqrto_redirect",
            name=name,
            city=city,
            evidence=evidence,
        )

    # NEW SMART GOOGLE MAPS RESOLUTION
    # Replaces the old "if not res.google_maps_url: find_google_maps_via_search_router(...)"
    if not res.google_maps_url:
        website_html_for_gmaps = None
        instagram_html_for_gmaps = None
        facebook_html_for_gmaps = None

        if res.website:
            website_html_for_gmaps = fetch_url(res.website)
            if website_html_for_gmaps:
                evidence.append("Fetched website HTML for Google Maps resolution")

        if res.instagram:
            instagram_html_for_gmaps = fetch_url(res.instagram)
            if instagram_html_for_gmaps:
                evidence.append("Fetched Instagram HTML for Google Maps resolution")

        if res.facebook:
            facebook_html_for_gmaps = fetch_url(res.facebook)
            if facebook_html_for_gmaps:
                evidence.append("Fetched Facebook HTML for Google Maps resolution")

        if router:
            gmaps_url, gmaps_reason = resolve_google_maps(
                name=name,
                city=city,
                router=router,
                website_html=website_html_for_gmaps,
                instagram_html=instagram_html_for_gmaps,
                facebook_html=facebook_html_for_gmaps,
            )

            if gmaps_url:
                cleaned_gmaps = clean_google_maps_url(gmaps_url)
                if cleaned_gmaps:
                    res.google_maps_url = cleaned_gmaps
                    res.directory_links_json = append_unique_json_list(
                        res.directory_links_json,
                        [cleaned_gmaps]
                    )
                    evidence.append(f"Google Maps resolved smartly: {cleaned_gmaps} ({gmaps_reason})")
            else:
                evidence.append(f"Google Maps smart resolution failed: {gmaps_reason}")
        else:
            evidence.append("Google Maps smart resolution skipped because router is unavailable")

    try_upgrade_website_from_candidates(res, name, city, evidence)

    if res.website and res.website_validated and not res.website_creator:
        html = fetch_url(res.website)
        if html:
            apply_website_creator_detection(res, html, evidence)

    if res.website and res.website_validated:
        features = analyze_website_features(res.website)

        res.menu_present = features["menu_present"]
        res.menu_quality = features["menu_quality"]
        res.booking_present = features["booking_present"]
        res.delivery_present = features["delivery_present"]
        res.data_capture_present = features["data_capture_present"]
        res.contact_present = features["contact_present"]
        res.directions_present = features["directions_present"]
        res.reviews_visible = features["reviews_visible"]
        res.offers_promos_present = features["offers_promos_present"]
        res.events_present = features["events_present"]
        res.unique_value_present = features["unique_value_present"]
        res.unique_value_examples_json = json.dumps(features.get("unique_value_examples", []), ensure_ascii=False)

        res.website_type, res.website_completeness_score = classify_website_type(features)

        creator_label = res.website_creator or res.website_platform or ""
        strengths, weaknesses = build_website_strengths_weaknesses(features, creator_label)
        res.website_strengths_json = json.dumps(strengths, ensure_ascii=False)
        res.website_weaknesses_json = json.dumps(weaknesses, ensure_ascii=False)

        evidence.append(
            "Website features analyzed: "
            f"menu={res.menu_present}, "
            f"menu_quality={res.menu_quality}, "
            f"booking={res.booking_present}, "
            f"delivery={res.delivery_present}, "
            f"data_capture={res.data_capture_present}, "
            f"contact={res.contact_present}, "
            f"directions={res.directions_present}, "
            f"reviews={res.reviews_visible}, "
            f"offers={res.offers_promos_present}, "
            f"events={res.events_present}, "
            f"unique_value={res.unique_value_present}, "
            f"website_type={res.website_type}, "
            f"website_completeness_score={res.website_completeness_score}"
        )

    _, legacy_restaurant_reason = validate_restaurant_match(
        name=name,
        city=city,
        website=res.website,
        instagram=res.instagram,
        facebook=res.facebook,
        x=res.x,
    )

    res.restaurant_match_score, restaurant_score_reason = compute_restaurant_match_score(
        name=name,
        city=city,
        website=res.website,
        instagram=res.instagram,
        facebook=res.facebook,
        tiktok=res.tiktok,
        x=res.x,
    )

    res.restaurant_match_percent = int(round(res.restaurant_match_score * 100))

    if res.restaurant_match_score >= 0.75:
        res.restaurant_match_label = "Yes"
        res.is_restaurant_match = True
    elif res.restaurant_match_score >= 0.50:
        res.restaurant_match_label = "Unclear"
        res.is_restaurant_match = False
    else:
        res.restaurant_match_label = "No"
        res.is_restaurant_match = False

    res.non_restaurant_reason = restaurant_score_reason or legacy_restaurant_reason

    evidence.append(
        f"Restaurant score: label={res.restaurant_match_label}, "
        f"score={res.restaurant_match_percent}%, "
        f"reason={restaurant_score_reason}"
    )

    social_kpis = analyze_basic_social_kpis(res)

    res.has_facebook_page = social_kpis["has_facebook_page"]
    res.has_instagram_page = social_kpis["has_instagram_page"]
    res.social_content_quality_score = social_kpis["social_content_quality_score"]
    res.social_content_quality_label = social_kpis["social_content_quality_label"]
    res.social_identity_signal_score = social_kpis["social_identity_signal_score"]
    res.social_identity_signal_label = social_kpis["social_identity_signal_label"]

    evidence.append(
        "Basic social KPIs: "
        f"has_facebook_page={res.has_facebook_page}, "
        f"has_instagram_page={res.has_instagram_page}, "
        f"social_content_quality_label={res.social_content_quality_label}, "
        f"social_content_quality_score={int(round(res.social_content_quality_score * 100))}%, "
        f"social_identity_signal_label={res.social_identity_signal_label}, "
        f"social_identity_signal_score={int(round(res.social_identity_signal_score * 100))}%, "
        f"reason={social_kpis['social_kpi_reason']}"
    )

    if not res.is_restaurant_match:
        evidence.append(f"Non-restaurant or weak entity match: {res.non_restaurant_reason}")
        res.needs_review = True
        res.confidence = min(res.confidence, 0.45)

    if res.website and (res.instagram or res.facebook or res.tiktok):
        res.confidence = min(1.0, res.confidence + 0.08)
    elif res.website:
        res.confidence = min(1.0, res.confidence + 0.03)

    real_links_found = sum([
        1 if res.website else 0,
        1 if res.instagram else 0,
        1 if res.facebook else 0,
        1 if res.tiktok else 0,
    ])

    if res.source == "guessed_domain":
        if real_links_found == 1:
            res.confidence = min(res.confidence, 0.55)
        elif real_links_found == 2:
            res.confidence = min(res.confidence, 0.75)
        elif real_links_found == 3:
            res.confidence = min(res.confidence, 0.88)
        else:
            res.confidence = min(res.confidence, 0.92)

    if real_links_found == 0:
        res.confidence = min(res.confidence, 0.25)
    elif real_links_found == 1:
        if res.website and res.website_validated and res.website_validation_score >= 0.80:
            res.confidence = min(res.confidence, 0.75)
        else:
            res.confidence = min(res.confidence, 0.55)
    elif real_links_found == 2:
        res.confidence = min(res.confidence, 0.80)
    elif real_links_found == 3:
        res.confidence = min(res.confidence, 0.92)
    else:
        res.confidence = min(res.confidence, 1.00)

    res.confidence = min(res.confidence, 0.95)

    try:
        dir_links = json_list(res.directory_links_json)
        res.has_directory_profile = len(dir_links) > 0
    except Exception:
        res.has_directory_profile = False

    if not res.google_reviews_count:
        res.google_reviews_count = "needs_api"
    if not res.google_rating_average:
        res.google_rating_average = "needs_api"

    apply_platform_flags(res)

    res.needs_review = (
        res.needs_review
        or (res.confidence < 0.65)
        or (not res.website and not res.instagram and not res.facebook and not res.tiktok)
    )

    res.tiktok_present = bool(res.tiktok)
    res.evidence = " | ".join(evidence)
    return res