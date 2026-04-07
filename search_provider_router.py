from __future__ import annotations

import os
import time
import json
import hashlib
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Tuple
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# Config
# ============================================================

DEFAULT_TIMEOUT_SEC = 8
DEFAULT_CONNECT_TIMEOUT_SEC = 3
CACHE_TTL_SEC = 60 * 60 * 6  # 6 hours
HEALTH_FAIL_COOLDOWN_SEC = 60 * 5
USER_AGENT = "SearchRouter/1.0"


# ============================================================
# Core models
# ============================================================

@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str = ""
    rank: int = 0
    source_type: str = "organic"
    provider: str = ""
    domain: str = ""
    score: float = 0.0
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchResponse:
    provider: str
    query: str
    ok: bool
    results: List[SearchResult] = field(default_factory=list)
    latency_ms: int = 0
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProviderPolicy:
    min_results: int = 1
    min_top_score: float = 0.35
    timeout_sec: int = DEFAULT_TIMEOUT_SEC


@dataclass
class RouterConfig:
    providers_order: List[str] = field(
        default_factory=lambda: ["serpapi", "serper", "tavily"]
    )
    policy: ProviderPolicy = field(default_factory=ProviderPolicy)
    cache_ttl_sec: int = CACHE_TTL_SEC
    cooldown_sec: int = HEALTH_FAIL_COOLDOWN_SEC
    country: str = "IT"
    language: str = "it"
    dedupe: bool = True


# ============================================================
# Provider protocol
# ============================================================

class SearchProvider(Protocol):
    name: str

    def search(self, query: str, **kwargs: Any) -> SearchResponse:
        ...


# ============================================================
# Utilities
# ============================================================

class SearchError(Exception):
    pass


class InMemoryCache:
    def __init__(self) -> None:
        self._store: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        item = self._store.get(key)
        if not item:
            return None
        expires_at, value = item
        if time.time() > expires_at:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl_sec: int) -> None:
        self._store[key] = (time.time() + ttl_sec, value)


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url.strip())
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc.lower().replace("www.", "")
        path = parsed.path.rstrip("/")
        return f"{scheme}://{netloc}{path}"
    except Exception:
        return url.strip()


def extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def query_cache_key(provider: str, query: str, kwargs: Dict[str, Any]) -> str:
    payload = json.dumps(
        {"provider": provider, "query": query, "kwargs": kwargs},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def score_result(result: SearchResult, query: str) -> float:
    query_terms = {t.lower() for t in query.split() if t.strip()}
    hay = f"{result.title} {result.snippet} {result.domain}".lower()
    overlap = sum(1 for t in query_terms if t in hay)
    overlap_score = overlap / max(1, len(query_terms))

    brand_bonus = 0.0
    if result.domain and any(t in result.domain for t in query_terms):
        brand_bonus += 0.2

    homepage_bonus = 0.1 if result.url.rstrip("/").count("/") <= 2 else 0.0
    result.score = min(1.0, overlap_score + brand_bonus + homepage_bonus)
    return result.score


def dedupe_results(results: List[SearchResult]) -> List[SearchResult]:
    seen = set()
    out: List[SearchResult] = []
    for r in results:
        key = normalize_url(r.url)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


# ============================================================
# Base HTTP provider
# ============================================================

class BaseHTTPProvider:
    name = "base"

    def __init__(self, api_key: str, timeout_sec: int = DEFAULT_TIMEOUT_SEC) -> None:
        self.api_key = api_key
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def _timed_request(
        self, method: str, url: str, **kwargs: Any
    ) -> Tuple[requests.Response, int]:
        start = now_ms()
        try:
            response = self.session.request(
                method,
                url,
                timeout=(DEFAULT_CONNECT_TIMEOUT_SEC, self.timeout_sec),
                **kwargs,
            )
            latency = now_ms() - start
            return response, latency
        except requests.Timeout as e:
            raise SearchError(f"timeout: {e}") from e
        except requests.RequestException as e:
            raise SearchError(f"request_failed: {e}") from e


# ============================================================
# Serper
# ============================================================

class SerperProvider(BaseHTTPProvider):
    name = "serper"
    endpoint = "https://google.serper.dev/search"

    def search(self, query: str, **kwargs: Any) -> SearchResponse:
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }
        payload = {
            "q": query,
            "gl": kwargs.get("country", "IT").lower(),
            "hl": kwargs.get("language", "it").lower(),
            "num": kwargs.get("count", 10),
        }
        try:
            resp, latency = self._timed_request(
                "POST",
                self.endpoint,
                json=payload,
                headers=headers,
            )

            if not resp.ok:
                try:
                    error_body = json.dumps(resp.json(), ensure_ascii=False)
                except Exception:
                    error_body = resp.text

                return SearchResponse(
                    provider=self.name,
                    query=query,
                    ok=False,
                    latency_ms=latency,
                    error_type=f"http_{resp.status_code}",
                    error_message=f"Serper {resp.status_code}: {error_body}",
                    meta={"request_payload": payload},
                )

            data = resp.json()
            items = data.get("organic", [])

            results: List[SearchResult] = []
            for i, item in enumerate(items, start=1):
                url = item.get("link", "")
                result = SearchResult(
                    title=item.get("title", ""),
                    url=url,
                    snippet=item.get("snippet", ""),
                    rank=i,
                    source_type="organic",
                    provider=self.name,
                    domain=extract_domain(url),
                    raw=item,
                )
                score_result(result, query)
                results.append(result)

            return SearchResponse(
                provider=self.name,
                query=query,
                ok=True,
                results=results,
                latency_ms=latency,
                meta={"raw_count": len(items), "request_payload": payload},
            )
        except Exception as e:
            return SearchResponse(
                provider=self.name,
                query=query,
                ok=False,
                latency_ms=0,
                error_type="provider_error",
                error_message=str(e),
                meta={"request_payload": payload},
            )


# ============================================================
# SerpAPI
# ============================================================

class SerpApiProvider(BaseHTTPProvider):
    name = "serpapi"
    endpoint = "https://serpapi.com/search.json"

    def search(self, query: str, **kwargs: Any) -> SearchResponse:
        params = {
            "engine": "google",
            "q": query,
            "api_key": self.api_key,
            "gl": kwargs.get("country", "IT").lower(),
            "hl": kwargs.get("language", "it").lower(),
            "num": kwargs.get("count", 10),
        }
        try:
            resp, latency = self._timed_request("GET", self.endpoint, params=params)

            if not resp.ok:
                try:
                    error_body = json.dumps(resp.json(), ensure_ascii=False)
                except Exception:
                    error_body = resp.text

                return SearchResponse(
                    provider=self.name,
                    query=query,
                    ok=False,
                    latency_ms=latency,
                    error_type=f"http_{resp.status_code}",
                    error_message=f"SerpAPI {resp.status_code}: {error_body}",
                    meta={"request_params": params},
                )

            data = resp.json()
            items = data.get("organic_results", [])

            results: List[SearchResult] = []
            for i, item in enumerate(items, start=1):
                url = item.get("link", "")
                result = SearchResult(
                    title=item.get("title", ""),
                    url=url,
                    snippet=item.get("snippet", ""),
                    rank=i,
                    source_type="organic",
                    provider=self.name,
                    domain=extract_domain(url),
                    raw=item,
                )
                score_result(result, query)
                results.append(result)

            return SearchResponse(
                provider=self.name,
                query=query,
                ok=True,
                results=results,
                latency_ms=latency,
                meta={"raw_count": len(items), "request_params": params},
            )
        except Exception as e:
            return SearchResponse(
                provider=self.name,
                query=query,
                ok=False,
                latency_ms=0,
                error_type="provider_error",
                error_message=str(e),
                meta={"request_params": params},
            )


# ============================================================
# Tavily
# ============================================================

class TavilyProvider(BaseHTTPProvider):
    name = "tavily"
    endpoint = "https://api.tavily.com/search"

    def search(self, query: str, **kwargs: Any) -> SearchResponse:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "query": query,
            "search_depth": kwargs.get("search_depth", "basic"),
            "max_results": kwargs.get("count", 10),
            "topic": kwargs.get("topic", "general"),
            "include_answer": False,
            "include_raw_content": False,
        }
        try:
            resp, latency = self._timed_request(
                "POST",
                self.endpoint,
                json=payload,
                headers=headers,
            )

            if not resp.ok:
                try:
                    error_body = json.dumps(resp.json(), ensure_ascii=False)
                except Exception:
                    error_body = resp.text

                return SearchResponse(
                    provider=self.name,
                    query=query,
                    ok=False,
                    latency_ms=latency,
                    error_type=f"http_{resp.status_code}",
                    error_message=f"Tavily {resp.status_code}: {error_body}",
                    meta={"request_payload": payload},
                )

            data = resp.json()
            items = data.get("results", [])

            results: List[SearchResult] = []
            for i, item in enumerate(items, start=1):
                url = item.get("url", "")
                result = SearchResult(
                    title=item.get("title", ""),
                    url=url,
                    snippet=item.get("content", ""),
                    rank=i,
                    source_type="organic",
                    provider=self.name,
                    domain=extract_domain(url),
                    raw=item,
                )
                score_result(result, query)
                results.append(result)

            return SearchResponse(
                provider=self.name,
                query=query,
                ok=True,
                results=results,
                latency_ms=latency,
                meta={"raw_count": len(items), "request_payload": payload},
            )
        except Exception as e:
            return SearchResponse(
                provider=self.name,
                query=query,
                ok=False,
                latency_ms=0,
                error_type="provider_error",
                error_message=str(e),
                meta={"request_payload": payload},
            )


# ============================================================
# Router
# ============================================================

class SearchRouter:
    def __init__(
        self,
        providers: Dict[str, SearchProvider],
        config: Optional[RouterConfig] = None,
    ) -> None:
        self.providers = providers
        self.config = config or RouterConfig()
        self.cache = InMemoryCache()
        self.provider_cooldowns: Dict[str, float] = {}

    def _provider_available(self, name: str) -> bool:
        until = self.provider_cooldowns.get(name)
        return not until or time.time() > until

    def _mark_provider_failed(self, name: str) -> None:
        self.provider_cooldowns[name] = time.time() + self.config.cooldown_sec

    def _is_good_enough(self, resp: SearchResponse) -> bool:
        if not resp.ok:
            return False
        if len(resp.results) < self.config.policy.min_results:
            return False

        top = max((r.score for r in resp.results), default=0.0)
        return top >= self.config.policy.min_top_score

    def search(self, query: str, **kwargs: Any) -> SearchResponse:
        debug_log: List[Dict[str, Any]] = []
        providers_order = kwargs.get("providers_order", self.config.providers_order)

        best_partial_resp: Optional[SearchResponse] = None
        best_partial_score = -1.0

        for provider_name in providers_order:
            provider = self.providers.get(provider_name)
            if not provider:
                debug_log.append({"provider": provider_name, "skipped": "not_configured"})
                continue

            if not self._provider_available(provider_name):
                debug_log.append({"provider": provider_name, "skipped": "cooldown"})
                continue

            search_kwargs = {
                "count": kwargs.get("count", 10),
                "country": kwargs.get("country", self.config.country),
                "language": kwargs.get("language", self.config.language),
            }
            search_kwargs.update(kwargs)

            cache_key = query_cache_key(provider_name, query, search_kwargs)
            cached = self.cache.get(cache_key)
            if cached:
                cached.meta.setdefault("debug", []).append(
                    {"provider": provider_name, "cache": "hit"}
                )
                return cached

            resp = provider.search(query, **search_kwargs)

            if resp.ok and self.config.dedupe:
                resp.results = dedupe_results(resp.results)

            top_score = max((r.score for r in resp.results), default=0.0)

            debug_log.append(
                {
                    "provider": provider_name,
                    "ok": resp.ok,
                    "results": len(resp.results),
                    "top_score": top_score,
                    "latency_ms": resp.latency_ms,
                    "error": resp.error_message,
                }
            )

            if resp.ok and resp.results:
                if top_score > best_partial_score:
                    best_partial_score = top_score
                    best_partial_resp = resp

            if self._is_good_enough(resp):
                resp.meta["debug"] = debug_log
                self.cache.set(cache_key, resp, self.config.cache_ttl_sec)
                return resp

            if not resp.ok:
                self._mark_provider_failed(provider_name)

        if best_partial_resp is not None:
            best_partial_resp.meta["debug"] = debug_log
            best_partial_resp.meta["partial_fallback"] = True
            return best_partial_resp

        return SearchResponse(
            provider="router",
            query=query,
            ok=False,
            error_type="all_providers_failed",
            error_message="No provider returned any usable result",
            meta={"debug": debug_log},
        )


# ============================================================
# Factory
# ============================================================

def build_router() -> SearchRouter:
    providers: Dict[str, SearchProvider] = {}

    serper_key = os.getenv("SERPER_API_KEY", "").strip()
    serpapi_key = os.getenv("SERPAPI_API_KEY", "").strip()
    tavily_key = os.getenv("TAVILY_API_KEY", "").strip()

    if serper_key:
        providers["serper"] = SerperProvider(serper_key)
    if serpapi_key:
        providers["serpapi"] = SerpApiProvider(serpapi_key)
    if tavily_key:
        providers["tavily"] = TavilyProvider(tavily_key)

    return SearchRouter(providers=providers)


# ============================================================
# Example usage
# ============================================================

def extract_business_profiles(results: List[SearchResult]) -> Dict[str, Optional[str]]:
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

        if domain and all(x not in domain for x in ["facebook.com", "instagram.com", "linkedin.com", "youtube.com"]):
            if not website:
                website = url + "/"

    return {
        "website": website,
        "facebook": facebook,
        "instagram": instagram,
    }


if __name__ == "__main__":
    router = build_router()

    demo_name = "Ristorante Black Moon"
    demo_city = "Avellino"
    query = f"{demo_name} {demo_city} restaurant ristorante pizzeria official website facebook instagram"

    response = router.search(
        query,
        country="IT",
        language="it",
        count=10,
    )

    print("OK:", response.ok)
    print("PROVIDER:", response.provider)
    print("ERROR:", response.error_message)
    print("DEBUG:")
    print(json.dumps(response.meta.get("debug", []), indent=2, ensure_ascii=False))

    for r in response.results[:5]:
        print("-" * 80)
        print("TITLE:", r.title)
        print("URL:", r.url)
        print("DOMAIN:", r.domain)
        print("SCORE:", r.score)
        print("SNIPPET:", r.snippet[:220])

    profiles = extract_business_profiles(response.results)
    print("\nFINAL PROFILES:")
    print(json.dumps(profiles, indent=2, ensure_ascii=False))