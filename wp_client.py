"""
WordPress REST API client for EdOpt.org.
Fetches providers, posts, pages, and style taxonomy.
"""
import logging
import time
import requests
from bs4 import BeautifulSoup

from config import WP_API_BASE, WP_USER, WP_APP_PASSWORD

logger = logging.getLogger(__name__)

# Use Basic Auth with WordPress application password — bypasses Cloudflare
SESSION = requests.Session()
if WP_USER and WP_APP_PASSWORD:
    SESSION.auth = (WP_USER, WP_APP_PASSWORD)
SESSION.headers.update({
    "User-Agent": "EdOptChatBot/1.0",
    "Accept": "application/json",
})


def _fetch_paginated(endpoint: str, per_page: int = 100, **extra_params) -> list:
    """Fetch all pages of a paginated WordPress REST API endpoint."""
    url = f"{WP_API_BASE}/{endpoint}"
    params = {"per_page": per_page, "page": 1, **extra_params}
    all_items = []

    while True:
        logger.info(f"Fetching {url} page {params['page']}...")
        resp = SESSION.get(url, params=params, timeout=30)
        resp.raise_for_status()

        items = resp.json()
        if not items:
            break

        all_items.extend(items)

        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        if params["page"] >= total_pages:
            break

        params["page"] += 1
        time.sleep(1)  # Rate limiting

    logger.info(f"Fetched {len(all_items)} items from {endpoint}")
    return all_items


def clean_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    return " ".join(text.split()).strip()


def parse_grade(value) -> int | None:
    """Convert grade string to integer. PreK=-1, K=0, 1-12=numeric, Post-Secondary=13."""
    if value is None or value == "":
        return None
    s = str(value).strip().lower()
    if s in ("prek", "pre-k", "pre k", "preschool"):
        return -1
    if s in ("k", "kindergarten"):
        return 0
    if s in ("post-secondary", "postsecondary", "college"):
        return 13
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def fetch_styles() -> dict:
    """Fetch the style taxonomy and return {style_id: {name, slug, parent_id}}."""
    items = _fetch_paginated("style")
    styles = {}
    for item in items:
        styles[item["id"]] = {
            "name": item.get("name", ""),
            "slug": item.get("slug", ""),
            "parent_id": item.get("parent", 0),
            "description": clean_html(item.get("description", "")),
            "count": item.get("count", 0),
        }
    return styles


def infer_education_style(style_names: list, style_slugs: list) -> str:
    """Infer the primary education style from taxonomy terms."""
    combined = " ".join(style_names + style_slugs).lower()

    if "homeschool" in combined or "home-education" in combined or "home education" in combined:
        return "homeschool"
    if "charter" in combined:
        return "charter"
    if "private" in combined:
        return "private"
    if "public" in combined:
        return "public"
    if "online" in combined or "virtual" in combined:
        return "online"
    if any(w in combined for w in ("enrichment", "tutoring", "camp", "supplement")):
        return "enrichment"
    if "preschool" in combined or "early childhood" in combined:
        return "preschool"
    return "other"


def fetch_all_providers(styles_dict: dict) -> list:
    """Fetch all providers with ACF fields parsed."""
    items = _fetch_paginated("provider")
    providers = []

    for item in items:
        acf = item.get("acf", {})
        if not acf:
            acf = {}

        # Resolve style taxonomy
        style_ids = item.get("style", [])
        style_names = []
        style_slugs = []
        for sid in style_ids:
            if sid in styles_dict:
                style_names.append(styles_dict[sid]["name"])
                style_slugs.append(styles_dict[sid]["slug"])

        # Parse coordinates
        lat = None
        lng = None
        try:
            lat = float(acf.get("latitude")) if acf.get("latitude") else None
        except (ValueError, TypeError):
            pass
        try:
            lng = float(acf.get("longitude")) if acf.get("longitude") else None
        except (ValueError, TypeError):
            pass

        # Determine online_only
        online_flag = acf.get("online_only", [])
        is_online = bool(online_flag) if isinstance(online_flag, list) and len(online_flag) > 0 else False
        if "online" in " ".join(style_slugs).lower():
            is_online = True

        provider = {
            "id": item["id"],
            "slug": item.get("slug", ""),
            "title": clean_html(item.get("title", {}).get("rendered", "")),
            "description": acf.get("description", ""),
            "content_text": clean_html(item.get("content", {}).get("rendered", "")),
            "url": item.get("link", f"https://edopt.org/provider/{item.get('slug', '')}/"),
            "address": acf.get("address", ""),
            "latitude": lat,
            "longitude": lng,
            "encoded_address": acf.get("encoded_address", ""),
            "geocoded_address": acf.get("geocoded_address", ""),
            "grade_start": parse_grade(acf.get("grade_start")),
            "grade_end": parse_grade(acf.get("grade_end")),
            "age_range_start": str(acf.get("age_range_start", "")) if acf.get("age_range_start") else None,
            "age_range_end": str(acf.get("age_range_end", "")) if acf.get("age_range_end") else None,
            "education_style": infer_education_style(style_names, style_slugs),
            "styles_raw": ", ".join(style_names),
            "focus": acf.get("focus", ""),
            "website": acf.get("website", ""),
            "contact_name": acf.get("contact_name", ""),
            "contact_email": acf.get("contact_email", ""),
            "contact_phone": acf.get("contact_phone", ""),
            "online_only": is_online,
            "date_published": item.get("date", ""),
            "date_modified": item.get("modified", ""),
        }
        providers.append(provider)

    return providers


def fetch_all_posts() -> list:
    """Fetch all blog posts."""
    items = _fetch_paginated("posts")
    posts = []
    for item in items:
        posts.append({
            "id": item["id"],
            "content_type": "post",
            "slug": item.get("slug", ""),
            "title": clean_html(item.get("title", {}).get("rendered", "")),
            "content_text": clean_html(item.get("content", {}).get("rendered", "")),
            "excerpt": clean_html(item.get("excerpt", {}).get("rendered", "")),
            "url": item.get("link", ""),
            "author_id": item.get("author"),
            "date_published": item.get("date", ""),
            "date_modified": item.get("modified", ""),
        })
    return posts


def fetch_all_pages() -> list:
    """Fetch all pages."""
    items = _fetch_paginated("pages")
    pages = []
    for item in items:
        pages.append({
            "id": item["id"],
            "content_type": "page",
            "slug": item.get("slug", ""),
            "title": clean_html(item.get("title", {}).get("rendered", "")),
            "content_text": clean_html(item.get("content", {}).get("rendered", "")),
            "excerpt": clean_html(item.get("excerpt", {}).get("rendered", "")),
            "url": item.get("link", ""),
            "author_id": item.get("author"),
            "date_published": item.get("date", ""),
            "date_modified": item.get("modified", ""),
        })
    return pages
