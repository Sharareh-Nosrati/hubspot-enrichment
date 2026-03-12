import os
import re
import time
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple, Set
from urllib.parse import urlparse, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

from search_provider_router import build_router, SearchResult
from dotenv import load_dotenv
load_dotenv()


# -----------------------------
# Config
# -----------------------------
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter"
]
UA = "RestaurantResolver/1.0 (contact: you@example.com)"

TIMEOUT = 25
SLEEP_BETWEEN_REQUESTS_SEC = 1.0
CACHE_PATH = "resolver_cache.json"

SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")


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
# Helpers
# -----------------------------
def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def generate_name_variants(name: str) -> List[str]:
    n = norm(name)
    variants = {n}

    removable_words = [
        "ristorante", "pizzeria", "osteria", "trattoria",
        "bar", "cafe", "rist", "pizza"
    ]

    words = n.split()
    filtered = [w for w in words if w not in removable_words]
    if filtered:
        variants.add(" ".join(filtered))

    variants.add(n.replace("'", ""))
    variants.add(n.replace("-", " "))
    variants.add(n.replace("&", "and"))

    return list(variants)

def ensure_http(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return url
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("//"):
        return "https:" + url
    return "https://" + url

def base_domain(url: str) -> str:
    try:
        parsed = urlparse(ensure_http(url))
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def clean_social_url(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None

    u = u.strip()
    if not u:
        return None

    if u.startswith("//"):
        u = "https:" + u
    elif not u.startswith("http://") and not u.startswith("https://"):
        if "instagram.com" in u or "facebook.com" in u:
            u = "https://" + u

    u_lower = u.lower()

    bad_patterns = [
        "facebook.com/sharer",
        "facebook.com/share",
        "l.facebook.com",
        "m.facebook.com/sharer",
        "instagram.com/p/",
        "instagram.com/reel/",
        "instagram.com/stories/",
        "instagram.com/explore/",
        "api.whatsapp.com",
        "wa.me/",
        "javascript:",
        "mailto:",
        "#"
    ]
    if any(b in u_lower for b in bad_patterns):
        return None

    u = re.sub(r"([?&])(utm_[^=&]+|fbclid|gclid)=[^&#]*", "", u, flags=re.IGNORECASE)
    u = re.sub(r"[?&]+$", "", u)

    u_lower = u.lower()

    if "instagram.com/" in u_lower:
        m = re.search(r"(https?://(?:www\.)?instagram\.com/[^/?#]+)/?", u, flags=re.IGNORECASE)
        if m:
            u = m.group(1) + "/"

    if "facebook.com/" in u_lower:
        u = u.split("&")[0]

    return u

def fetch_url(url: str) -> Optional[str]:
    try:
        r = requests.get(
            ensure_http(url),
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
            allow_redirects=True
        )
        if 200 <= r.status_code < 300 and r.text:
            return r.text
    except requests.RequestException:
        return None
    return None

def is_probably_restaurant(tags: Dict[str, str]) -> bool:
    amenity = (tags.get("amenity") or "").lower()
    shop = (tags.get("shop") or "").lower()
    tourism = (tags.get("tourism") or "").lower()
    return amenity in {"restaurant", "fast_food", "cafe", "bar", "pub"} or shop == "bakery" or tourism == "hotel"


# -----------------------------
# Candidate scoring
# -----------------------------
def score_text_candidate(name: str, city: str, title: str = "", snippet: str = "", url: str = "") -> float:
    target_name = norm(name)
    title_n = norm(title)
    snippet_n = norm(snippet)
    url_n = norm(url)
    city_n = norm(city)

    score = 0.0

    name_sim = fuzz.token_set_ratio(target_name, title_n) / 100.0
    score += name_sim * 0.55

    if city_n and city_n in title_n:
        score += 0.15
    if city_n and city_n in snippet_n:
        score += 0.12
    if city_n and city_n in url_n:
        score += 0.08

    if target_name in snippet_n:
        score += 0.10

    return min(score, 1.0)

def score_social_candidate(name: str, city: str, title: str = "", snippet: str = "", url: str = "") -> float:
    score = score_text_candidate(name, city, title, snippet, url)

    ul = url.lower()
    if "instagram.com" in ul or "facebook.com" in ul:
        score += 0.10

    if "/pages/" in ul:
        score -= 0.08

    return max(0.0, min(score, 1.0))


# -----------------------------
# HTML parsing
# -----------------------------
def extract_socials_from_html(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "lxml")
    found_urls = []

    for a in soup.find_all("a", href=True):
        found_urls.append(a.get("href"))

    for meta in soup.find_all(["meta", "link"]):
        for attr in ["content", "href"]:
            val = meta.get(attr)
            if val and isinstance(val, str):
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
                        found_urls.extend([x for x in same_as if isinstance(x, str)])
                    elif isinstance(same_as, str):
                        found_urls.append(same_as)
        except Exception:
            pass

    cleaned = []
    seen = set()
    for u in found_urls:
        cu = clean_social_url(u)
        if cu and cu not in seen:
            seen.add(cu)
            cleaned.append(cu)

    ig = None
    fb = None

    for u in cleaned:
        ul = u.lower()
        if not ig and "instagram.com" in ul:
            ig = u
        if not fb and "facebook.com" in ul:
            fb = u
        if ig and fb:
            break

    return {"instagram": ig, "facebook": fb}

def extract_business_profiles_from_results(results: List[SearchResult]) -> Dict[str, Optional[str]]:
    website = None
    facebook = None
    instagram = None

    bad_facebook_parts = ["/posts/", "/photo.php", "/photos/", "/media/", "/mentions/"]
    bad_instagram_parts = ["/p/", "/reel/", "/stories/"]

    for r in results:
        url = r.url.split("?")[0].rstrip("/")
        domain = r.domain.lower()

        if "facebook.com" in domain:
            if not any(part in url for part in bad_facebook_parts):
                if not facebook:
                    facebook = url + "/"
            continue

        if "instagram.com" in domain:
            if not any(part in url for part in bad_instagram_parts):
                if not instagram:
                    instagram = url + "/"
            continue

        if domain and all(x not in domain for x in [
            "facebook.com", "instagram.com", "linkedin.com", "youtube.com",
            "tripadvisor.", "thefork.", "ubereats.", "justeat.", "glovoapp.", "deliveroo."
        ]):
            if not website:
                website = url + "/"

    return {
        "website": website,
        "facebook": facebook,
        "instagram": instagram,
    }


def find_profiles_via_search_router(
    name: str, city: str, country: str
) -> Tuple[Dict[str, Optional[str]], List[str], Optional[str]]:
    evidence = []

    try:
        router = build_router()
    except Exception as e:
        return {"website": None, "facebook": None, "instagram": None}, [f"Router init failed: {e}"], None

    query = f"{name} {city} restaurant ristorante pizzeria official website facebook instagram"
    response = router.search(query, country="IT", language="it", count=10)

    if not response.ok:
        return {"website": None, "facebook": None, "instagram": None}, [
            f"Router search failed: {response.error_message}"
        ], None

    profiles = extract_business_profiles_from_results(response.results)

    evidence.append(f"Router provider used: {response.provider}")
    evidence.append(
        f"Router profiles found: website={bool(profiles['website'])}, "
        f"facebook={bool(profiles['facebook'])}, instagram={bool(profiles['instagram'])}"
    )

    return profiles, evidence, response.provider


# -----------------------------
# Website feature analysis
# -----------------------------
def get_internal_candidate_links(base_url: str, html: str, max_links: int = 8) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    base_host = base_domain(base_url)
    links = []
    seen = set()

    interesting_keywords = [
        "menu", "menù", "carta", "food",
        "book", "booking", "reserve", "reservation", "prenota", "prenotazione",
        "delivery", "takeaway", "asporto", "ordina", "ordine",
        "contact", "contatti", "about", "newsletter"
    ]

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href:
            continue

        full = urljoin(base_url, href)
        full = ensure_http(full)
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

def analyze_single_html(html: str) -> Dict[str, bool]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True).lower()
    links = [a.get("href", "").lower() for a in soup.find_all("a", href=True)]
    buttons = [b.get_text(" ", strip=True).lower() for b in soup.find_all(["button", "a"])]
    forms = soup.find_all("form")
    iframes = [i.get("src", "").lower() for i in soup.find_all("iframe", src=True)]

    all_content = " ".join([
        text,
        " ".join(links),
        " ".join(buttons),
        " ".join(iframes)
    ])

    def has_any(keywords: List[str]) -> bool:
        return any(k in all_content for k in keywords)

    menu_present = has_any([
        "menu", "menù", "our-menu", "food-menu", "carta", "scarica il menu",
        ".pdf", "menu degustazione"
    ])

    booking_present = has_any([
        "book", "booking", "reserve", "reservation",
        "prenota", "prenotazione", "thefork", "opentable",
        "quandoo", "resmio", "book a table", "table booking"
    ])

    delivery_present = has_any([
        "delivery", "takeaway", "asporto", "consegna", "a domicilio",
        "deliveroo", "ubereats", "just eat", "justeat", "glovo", "ordina online"
    ])

    data_capture_present = (
        len(forms) > 0
        or has_any([
            "newsletter", "subscribe", "sign up", "join",
            "iscriviti", "lascia i tuoi dati", "contattaci",
            "request info", "contact form"
        ])
    )

    contact_present = (
        has_any(["contact", "contatti", "tel:", "mailto:", "whatsapp", "dove siamo"])
        or bool(re.search(r"\+?\d[\d\-\s()]{6,}", text))
        or bool(re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text))
    )

    return {
        "menu_present": menu_present,
        "booking_present": booking_present,
        "delivery_present": delivery_present,
        "data_capture_present": data_capture_present,
        "contact_present": contact_present
    }

def analyze_website_features(website_url: str) -> Dict[str, bool]:
    result = {
        "menu_present": False,
        "booking_present": False,
        "delivery_present": False,
        "data_capture_present": False,
        "contact_present": False
    }

    homepage_html = fetch_url(website_url)
    if not homepage_html:
        return result

    homepage_result = analyze_single_html(homepage_html)
    for k, v in homepage_result.items():
        result[k] = result[k] or v

    candidate_links = get_internal_candidate_links(website_url, homepage_html, max_links=8)

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
        for k, v in sub_result.items():
            result[k] = result[k] or v

    return result


# -----------------------------
# Overpass
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
                    timeout=45
                )
                r.raise_for_status()
                data = r.json()
                if data.get("elements"):
                    return data
                else:
                    return {"elements": [], "error": "OSM no matches"}

            except requests.exceptions.Timeout as e:
                errors.append(f"{base_url} timeout attempt {attempt + 1}: {str(e)}")
            except requests.exceptions.RequestException as e:
                errors.append(f"{base_url} request error attempt {attempt + 1}: {str(e)}")
            except Exception as e:
                errors.append(f"{base_url} unknown error attempt {attempt + 1}: {str(e)}")

            time.sleep(2)

    return {"elements": [], "error": " | ".join(errors)}

def pick_best_osm_candidate(name: str, city: str, elements: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], float, List[str]]:
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
# Free guessed-domain fallback
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
    generic_names = {"la pergola", "ristorante", "pizzeria", "osteria", "trattoria"}

    for url in domains:
        html = fetch_url(url)
        time.sleep(0.5)

        if not html:
            continue

        text = BeautifulSoup(html, "lxml").get_text(" ", strip=True).lower()
        name_match = norm(name) in text
        city_match = norm(city) in text

        if name_match and city_match:
            evidence.append(f"Guessed working domain: {url}")
            return url, evidence

        if (
            name_match
            and len(norm(name).split()) >= 2
            and norm(name) not in generic_names
        ):
            evidence.append(f"Guessed domain matched name strongly: {url}")
            return url, evidence

    return None, ["No guessed domain worked"]



# -----------------------------
# Optional: Google Places
# -----------------------------
def google_places_website(name: str, city: str, country: str) -> Optional[str]:
    if not GOOGLE_PLACES_API_KEY:
        return None

    try:
        text = f"{name} {city} {country}"

        ts = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": text, "key": GOOGLE_PLACES_API_KEY},
            timeout=TIMEOUT
        )
        ts.raise_for_status()
        ts_data = ts.json()
        results = ts_data.get("results", [])
        if not results:
            return None

        place_id = results[0].get("place_id")
        if not place_id:
            return None

        pd_resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params={
                "place_id": place_id,
                "fields": "website,url,name,formatted_address,international_phone_number",
                "key": GOOGLE_PLACES_API_KEY
            },
            timeout=TIMEOUT
        )
        pd_resp.raise_for_status()
        det = pd_resp.json().get("result", {})
        return det.get("website")
    except requests.RequestException:
        return None


# -----------------------------
# Result model
# -----------------------------
@dataclass
class ResolveResult:
    name: str
    city: str
    country: str
    website: Optional[str] = None
    instagram: Optional[str] = None
    facebook: Optional[str] = None
    confidence: float = 0.0
    source: str = "none"
    evidence: str = ""
    needs_review: bool = True
    menu_present: bool = False
    booking_present: bool = False
    delivery_present: bool = False
    data_capture_present: bool = False
    contact_present: bool = False
    is_restaurant_match: bool = False
    non_restaurant_reason: str = ""



def validate_restaurant_match(
    name: str,
    city: str,
    website: Optional[str],
    instagram: Optional[str],
    facebook: Optional[str]
) -> Tuple[bool, str]:
    target_name = norm(name)
    target_city = norm(city)

    restaurant_keywords = [
        "restaurant", "ristorante", "pizzeria", "trattoria", "osteria",
        "cucina", "menu", "menù", "prenota", "reservation", "booking",
        "food", "delivery", "takeaway", "asporto", "bar", "cafe", "pub"
    ]

    reasons = []
    positive_signals = 0

    # --- Website validation ---
    if website:
        html = fetch_url(website)
        time.sleep(0.3)

        if html:
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ", strip=True).lower()
            title = (soup.title.get_text(" ", strip=True).lower() if soup.title else "")
            combined = f"{title} {text}"

            name_in_page = target_name in combined
            city_in_page = target_city in combined if target_city else False
            restaurant_kw = any(k in combined for k in restaurant_keywords)

            if name_in_page:
                positive_signals += 1
            if city_in_page:
                positive_signals += 1
            if restaurant_kw:
                positive_signals += 1

            if not name_in_page:
                reasons.append("Website does not clearly mention the restaurant name")
            if not restaurant_kw:
                reasons.append("Website does not look like a restaurant website")
        else:
            reasons.append("Website could not be fetched for validation")

    # --- Instagram validation ---
    if instagram:
        ig_url = instagram.lower()
        handle_match = any(x in ig_url for x in target_name.replace("'", "").split())
        if handle_match:
            positive_signals += 1
        else:
            reasons.append("Instagram handle does not match restaurant name")

    # --- Facebook validation ---
    if facebook:
        fb_url = facebook.lower()
        handle_match = any(x in fb_url for x in target_name.replace("'", "").split())
        if handle_match:
            positive_signals += 1
        else:
            reasons.append("Facebook page does not match restaurant name")

    is_match = positive_signals >= 2

    if not is_match and not reasons:
        reasons.append("Found links do not provide enough evidence of being the restaurant")

    return is_match, " | ".join(dict.fromkeys(reasons))



# -----------------------------
# Main resolver
# -----------------------------
def resolve_one(name: str, city: str, country: str) -> ResolveResult:
    res = ResolveResult(name=name, city=city, country=country)
    evidence: List[str] = []

    # 1) OSM
    osm = overpass_search(name, city, country)
    elements = osm.get("elements", []) or []

    if elements:
        best, score, ev = pick_best_osm_candidate(name, city, elements)
        evidence += ev

        if best and score >= 0.55:
            tags = best.get("tags", {}) or {}
            website = tags.get("contact:website") or tags.get("website")
            instagram = tags.get("contact:instagram") or tags.get("instagram")
            facebook = tags.get("contact:facebook") or tags.get("facebook")

            if website:
                res.website = ensure_http(website)
            if instagram:
                res.instagram = ensure_http(instagram)
            if facebook:
                res.facebook = ensure_http(facebook)

            res.confidence = max(res.confidence, score)
            res.source = "osm"
        else:
            evidence.append("OSM candidates found but confidence below threshold")
    else:
        if osm.get("error"):
            evidence.append(f"OSM error: {osm['error']}")
        else:
            evidence.append("OSM no matches")

    # 2) If website found, scrape for socials
    if res.website and (not res.instagram or not res.facebook):
        html = fetch_url(res.website)
        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

        if html:
            socials = extract_socials_from_html(html)
            if not res.instagram and socials.get("instagram"):
                res.instagram = socials["instagram"]
                evidence.append("Instagram extracted from website HTML")
            if not res.facebook and socials.get("facebook"):
                res.facebook = socials["facebook"]
                evidence.append("Facebook extracted from website HTML")

            if res.source == "osm":
                res.source = "osm+site"
                res.confidence = min(1.0, res.confidence + 0.10)
        else:
            evidence.append("Website scrape failed or empty HTML")

    # 3) Free fallback: guess likely website domains
    if not res.website:
        guessed_website, ev = find_working_domain(name, city, country)
        evidence += ev

        if guessed_website:
            res.website = guessed_website
            res.source = "guessed_domain"
            res.confidence = max(res.confidence, 0.50)

            html = fetch_url(res.website)
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

            if html:
                socials = extract_socials_from_html(html)
                if not res.instagram and socials.get("instagram"):
                    res.instagram = socials["instagram"]
                    evidence.append("Instagram extracted from guessed website")
                if not res.facebook and socials.get("facebook"):
                    res.facebook = socials["facebook"]
                    evidence.append("Facebook extracted from guessed website")

    # 4) Search API fallback via router
    if (not res.website) or (not res.instagram) or (not res.facebook):
        profiles, ev, router_provider = find_profiles_via_search_router(name, city, country)
        evidence += ev

        if profiles.get("website") and not res.website:
            res.website = ensure_http(profiles["website"])
            if res.source == "none":
                res.source = f"search_router:{router_provider}" if router_provider else "search_router"
            res.confidence = max(res.confidence, 0.72)
        
        if profiles.get("instagram") and not res.instagram:
            res.instagram = ensure_http(profiles["instagram"])
            if res.source == "none":
                res.source = f"search_router:{router_provider}" if router_provider else "search_router"
            res.confidence = max(res.confidence, 0.72)

        if profiles.get("facebook") and not res.facebook:
            res.facebook = ensure_http(profiles["facebook"])
            if res.source == "none":
                res.source = f"search_router:{router_provider}" if router_provider else "search_router"
            res.confidence = max(res.confidence, 0.72)

        # If website came from router, scrape it too
        if res.website:
            html = fetch_url(res.website)
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

            if html:
                socials = extract_socials_from_html(html)
                if not res.instagram and socials.get("instagram"):
                    res.instagram = socials["instagram"]
                    evidence.append("Instagram extracted from router-found website")
                if not res.facebook and socials.get("facebook"):
                    res.facebook = socials["facebook"]
                    evidence.append("Facebook extracted from router-found website")
                    
    # 6) Optional Google Places fallback
    if not res.website and GOOGLE_PLACES_API_KEY:
        wp = google_places_website(name, city, country)
        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

        if wp:
            res.website = ensure_http(wp)
            res.source = "google_places"
            res.confidence = max(res.confidence, 0.78)
            evidence.append(f"Website from Google Places: {wp}")

            html = fetch_url(res.website)
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

            if html:
                socials = extract_socials_from_html(html)
                if not res.instagram and socials.get("instagram"):
                    res.instagram = socials["instagram"]
                    evidence.append("Instagram extracted from Places website")
                if not res.facebook and socials.get("facebook"):
                    res.facebook = socials["facebook"]
                    evidence.append("Facebook extracted from Places website")

    # 6.5) Analyze website features
    if res.website:
        features = analyze_website_features(res.website)
        res.menu_present = features["menu_present"]
        res.booking_present = features["booking_present"]
        res.delivery_present = features["delivery_present"]
        res.data_capture_present = features["data_capture_present"]
        res.contact_present = features["contact_present"]
        evidence.append(
            "Website features analyzed: "
            f"menu={res.menu_present}, "
            f"booking={res.booking_present}, "
            f"delivery={res.delivery_present}, "
            f"data_capture={res.data_capture_present}, "
            f"contact={res.contact_present}"
        )
        
    # 6.6) Validate that found links really belong to the restaurant
    res.is_restaurant_match, res.non_restaurant_reason = validate_restaurant_match(
        name=name,
        city=city,
        website=res.website,
        instagram=res.instagram,
        facebook=res.facebook
    )

    if not res.is_restaurant_match:
        evidence.append(f"Non-restaurant or weak entity match: {res.non_restaurant_reason}")
        res.needs_review = True
        res.confidence = min(res.confidence, 0.45)

    # 7) Final confidence adjustment
    if res.website and (res.instagram or res.facebook):
        res.confidence = min(1.0, res.confidence + 0.08)
    elif res.website:
        res.confidence = min(1.0, res.confidence + 0.03)

    real_links_found = sum([
        1 if res.website else 0,
        1 if res.instagram else 0,
        1 if res.facebook else 0
    ])

    if res.source == "guessed_domain":
        if real_links_found == 1:
            res.confidence = min(res.confidence, 0.55)
        elif real_links_found == 2:
            res.confidence = min(res.confidence, 0.75)
        elif real_links_found == 3:
            res.confidence = min(res.confidence, 0.88)

    if real_links_found == 0:
        res.confidence = min(res.confidence, 0.25)
    elif real_links_found == 1:
        res.confidence = min(res.confidence, 0.55)
    elif real_links_found == 2:
        res.confidence = min(res.confidence, 0.80)
    else:
        res.confidence = min(res.confidence, 1.00)

    res.confidence = min(res.confidence, 0.95)

    res.needs_review = (
        (res.confidence < 0.65)
        or (not res.website and not res.instagram and not res.facebook)
    )

    res.evidence = " | ".join(evidence)
    return res


# -----------------------------
# Batch processing
# -----------------------------
def resolve_excel(input_path: str, output_path: str):
    df = pd.read_excel(input_path)

    required = {"name", "city", "country"}
    missing = required - set(df.columns.str.lower())
    if missing:
        raise ValueError(f"Excel must contain columns: {sorted(required)} (case-insensitive). Missing: {sorted(missing)}")

    df.rename(columns={c: c.lower() for c in df.columns}, inplace=True)

    cache = load_cache()
    out_rows = []

    for _, row in df.iterrows():
        name = str(row.get("name", "")).strip()
        city = str(row.get("city", "")).strip()
        country = str(row.get("country", "")).strip()

        if not name or not city or not country:
            out_rows.append({
                **row.to_dict(),
                "website": None,
                "instagram": None,
                "facebook": None,
                "confidence": 0.0,
                "source": "skipped",
                "evidence": "Missing name/city/country",
                "needs_review": True,
                "status": "error",
                "menu_present": False,
                "booking_present": False,
                "delivery_present": False,
                "data_capture_present": False,
                "contact_present": False,
                "is_restaurant_match": False,
                "non_restaurant_reason": "Missing name/city/country",
            })
            continue

        key = cache_key(name, city, country)
        if key in cache:
            out_rows.append({**row.to_dict(), **cache[key]})
            continue

        result = resolve_one(name, city, country)

        status = "ok"
        if result.needs_review:
            status = "needs_review"

        if (
            ("timeout" in result.evidence.lower()) or
            ("request error" in result.evidence.lower())
        ) and not (result.website or result.instagram or result.facebook):
            status = "error"

        record = {
            "website": result.website,
            "instagram": result.instagram,
            "facebook": result.facebook,
            "confidence": round(result.confidence, 3),
            "source": result.source,
            "evidence": result.evidence,
            "needs_review": result.needs_review,
            "status": status,
            "menu_present": result.menu_present,
            "booking_present": result.booking_present,
            "delivery_present": result.delivery_present,
            "data_capture_present": result.data_capture_present,
            "contact_present": result.contact_present,
            "is_restaurant_match": result.is_restaurant_match,
            "non_restaurant_reason": result.non_restaurant_reason
        }

        cache[key] = record
        save_cache(cache)

        out_rows.append({**row.to_dict(), **record})
        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

    out_df = pd.DataFrame(out_rows)
    out_df.to_excel(output_path, index=False)
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to input Excel (.xlsx)")
    ap.add_argument("--output", required=True, help="Path to output Excel (.xlsx)")
    args = ap.parse_args()

    resolve_excel(args.input, args.output)