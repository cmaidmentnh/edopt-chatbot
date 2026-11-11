#!/usr/bin/env python3
"""
Monthly EdOpt provider audit.

1. Dead links: checks every published provider's `website` and `contact_page`
   URL and classifies it ok / redirected / dead / blocked / error.
2. DOE changes: compares the directory's public, charter and private-school
   providers against NH DOE data (iPlatform 2025-26 enrollment, already in the
   chatbot DB) and the latest NCES CCD directory (NH DOE's federal submission:
   status, addresses). Lists closures still listed, renames, address changes,
   and DOE schools missing from the directory.
3. Safe fixes: updates a URL to its redirect target only when it lands on the
   same organization's site (see `is_safe_redirect`). Never deletes or
   unpublishes anything. Old values are backed up to post meta
   `_edopt_backup_<field>_<YYYYMMDD>` by the WordPress side before the change.
   Writes go through the signed endpoint in wordpress/edopt-provider-audit.php
   (needs EDOPT_AUDIT_SECRET in .env and that mu-plugin installed). Without it,
   fixes are written to a JSON file and listed as pending in the report.
4. Writes an .xlsx (one tab per category) and emails a summary to Chris via SES.

Usage:
  python3 provider_audit.py                 # full run: check, apply, email
  python3 provider_audit.py --no-apply --no-email --out /tmp/audit.xlsx
  python3 provider_audit.py --cron          # only runs when it's 9am America/New_York
Cron (server is UTC; the --cron guard picks the right hour across DST):
  0 13,14 1 * * cd /opt/edopt-chatbot && venv/bin/python3 provider_audit.py --cron >> /var/log/edopt-provider-audit.log 2>&1
"""
import argparse
import hashlib
import hmac
import json
import logging
import os
import re
import socket
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

import requests
import urllib3
from rapidfuzz import fuzz

from config import DATABASE_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("provider_audit")
logging.getLogger("urllib3").setLevel(logging.ERROR)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ET = ZoneInfo("America/New_York")
EMAIL_TO = ["chris@maidmentnh.com"]
EMAIL_FROM = "chatbot@edopt.org"
AWS_REGION = "us-east-1"
SITE = "https://edopt.org"
FIX_ENDPOINT = f"{SITE}/wp-json/edopt/v1/provider-url-fix"
URBAN_CCD = "https://educationdata.urban.org/api/v1/schools/ccd/directory/{year}/?fips=33"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
BROWSER_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 10
WORKERS = 12

PUBLIC_STYLES = {"Public School", "Charter School", "Career & Technical Centers"}
NONPUBLIC_STYLES = {"Private School", "Religious School"}

CCD_STATUS = {1: "open", 2: "closed", 3: "new", 4: "added", 5: "changed agency",
              6: "inactive", 7: "future", 8: "reopened"}
CCD_TYPE = {1: "Regular", 2: "Special education", 3: "Career and technical", 4: "Alternative"}


# --------------------------------------------------------------------------
# Providers
# --------------------------------------------------------------------------

def load_providers():
    from wp_client import fetch_styles, fetch_all_providers
    styles = fetch_styles()
    providers = fetch_all_providers(styles)
    for p in providers:
        p["styles"] = {s.strip().replace("&amp;", "&") for s in (p.get("styles_raw") or "").split(",") if s.strip()}
        p["edit_url"] = f"{SITE}/wp-admin/post.php?post={p['id']}&action=edit"
    log.info(f"Loaded {len(providers)} published providers")
    return providers


# --------------------------------------------------------------------------
# Link checking
# --------------------------------------------------------------------------

_tls = threading.local()


def _session():
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update(BROWSER_HEADERS)
        s.max_redirects = 10
        _tls.s = s
    return s


BOT_MARKERS = ("just a moment...", "attention required! | cloudflare", "cf-chl", "captcha",
               "access denied", "incapsula", "sgcaptcha", "are you a robot", "bot protection",
               "ddos-guard", "please enable cookies")
PARKED_MARKERS = ("this domain is for sale", "domain is for sale", "buy this domain",
                  "this domain may be for sale", "hugedomains", "domain has expired",
                  "parked free", "is parked", "related searches")
SPAM_HOST_WORDS = ("sbobet", "casino", "slot", "togel", "judi", "poker", "bet88", "gacor", "porn", "xxx")
LOGIN_WALL_HOSTS = ("facebook.com", "instagram.com", "linkedin.com")
PARKED_HOSTS = ("hugedomains.com", "sedo.com", "afternic.com", "dan.com", "godaddy.com/domainsearch",
                "parkingcrew", "bodis.com", "above.com", "domainmarket.com", "squadhelp.com")


def normalize_input_url(u):
    u = (u or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, re.I):
        u = "http://" + u.lstrip("/")
    return u


def _canon(u):
    """Comparable form: scheme + lowercase host, no www, no default port, no fragment, no trailing slash."""
    try:
        sp = urlsplit(u)
    except ValueError:
        return u
    host = (sp.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = sp.path.rstrip("/")
    return f"{sp.scheme.lower()}://{host}{path}" + (f"?{sp.query}" if sp.query else "")


def _classify_response(resp, body_snippet):
    code = resp.status_code
    low = body_snippet.lower()
    final = resp.url
    fh = (urlsplit(final).hostname or "").lower()
    if fh == "accounts.google.com" and "sites.google.com" in final:
        return "dead", "Google Sites page now requires sign-in (old Google Sites page is no longer public)"
    if any(w in fh for w in SPAM_HOST_WORDS) and fh != (urlsplit(resp.history[0].url).hostname if resp.history else fh):
        return "dead", f"domain appears hijacked (redirects to {fh})"
    if any(h in final.lower() for h in PARKED_HOSTS):
        return "dead", f"redirects to domain-sale/parking page ({urlsplit(final).hostname})"
    if (urlsplit(final).hostname or "").endswith(LOGIN_WALL_HOSTS) and (code in (400, 401, 403) or "/login" in final):
        return "blocked", f"HTTP {code} (login wall)"
    if code in (404, 410):
        return "dead", f"HTTP {code}"
    if code in (401, 403, 429, 999) or resp.headers.get("cf-mitigated") or \
            (code in (503, 202) and any(m in low for m in BOT_MARKERS)):
        return "blocked", f"HTTP {code}" + (" (bot wall)" if any(m in low for m in BOT_MARKERS) else "")
    if code >= 500:
        return "error", f"HTTP {code}"
    if code >= 400:
        return "error", f"HTTP {code}"
    if any(m in low for m in PARKED_MARKERS) and len(low) < 60000:
        return "dead", "domain parked / for sale"
    return "ok", f"HTTP {code}"


def _is_dns_error(exc):
    s = str(exc).lower()
    return any(x in s for x in ("name or service not known", "nodename nor servname", "getaddrinfo failed",
                                "name resolution", "no address associated", "failed to resolve",
                                "temporary failure in name resolution"))


def _request(s, method, url, verify):
    if method == "HEAD":
        r = s.head(url, allow_redirects=True, timeout=TIMEOUT, verify=verify)
        return r, ""
    r = s.get(url, allow_redirects=True, timeout=TIMEOUT, verify=verify, stream=True)
    return r, _read_some(r)


def check_url(url):
    """HEAD, then GET fallback. Returns dict: status, reason, http, final_url."""
    url = normalize_input_url(url)
    out = {"url": url, "status": "error", "reason": "", "http": None, "final_url": ""}
    host = urlsplit(url).hostname if url else None
    if not host:
        out.update(status="dead", reason="malformed URL")
        return out
    for attempt in range(2):  # DNS twice so a dead domain is unambiguous
        try:
            socket.getaddrinfo(host, None)
            break
        except socket.gaierror:
            if attempt == 1:
                out.update(status="dead", reason="DNS failure (domain does not resolve)")
                return out
            time.sleep(2)
        except UnicodeError:
            out.update(status="dead", reason="malformed host")
            return out

    s = _session()
    verify = True
    last_exc = None
    resp = body = None
    for attempt in range(3):
        try:
            resp, body = _request(s, "HEAD", url, verify)
            if resp.status_code >= 400 or (urlsplit(resp.url).hostname or "") != host:
                # non-2xx, or landed on another host (could be a parking page): look at the real page
                resp, body = _request(s, "GET", url, verify)
            break
        except requests.exceptions.TooManyRedirects:
            out.update(status="error", reason="redirect loop")
            return out
        except requests.exceptions.SSLError as e:
            last_exc = e
            if verify:
                verify = False  # tell "bad certificate" apart from "down"
                continue
            break
        except requests.RequestException as e:
            last_exc = e
            try:  # some servers refuse HEAD outright
                resp, body = _request(s, "GET", url, verify)
                break
            except requests.exceptions.TooManyRedirects:
                out.update(status="error", reason="redirect loop")
                return out
            except requests.exceptions.SSLError as e2:
                last_exc = e2
                if verify:
                    verify = False
                    continue
                break
            except requests.RequestException as e2:
                last_exc = e2
                resp = None
                time.sleep(3)

    if resp is None:
        name = type(last_exc).__name__ if last_exc else "unknown"
        if isinstance(last_exc, requests.exceptions.SSLError):
            out.update(status="error", reason="SSL error; site unreachable")
        elif isinstance(last_exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
            out.update(status="dead", reason=f"repeated connection failure ({name})")
        else:
            out.update(status="error", reason=name)
        return out

    status, reason = _classify_response(resp, body or "")
    out["http"] = resp.status_code
    out["final_url"] = resp.url
    if not verify:
        if status in ("ok", "redirected"):
            status, reason = "ssl", "page loads, but its SSL certificate doesn't verify (browsers may warn)"
        else:
            reason += "; SSL certificate doesn't verify"
    if status == "ok" and _canon(resp.url) != _canon(url):
        status = "redirected"
    out.update(status=status, reason=reason)
    return out


def _read_some(resp, limit=65536):
    try:
        chunk = next(resp.iter_content(limit), b"")
        enc = resp.encoding or "utf-8"
        return chunk.decode(enc, errors="ignore")
    except Exception:
        return ""
    finally:
        resp.close()


def check_all_links(providers):
    urls = set()
    for p in providers:
        for f in ("website", "contact_page"):
            u = (p.get(f) or "").strip()
            if u:
                urls.add(u)
    log.info(f"Checking {len(urls)} unique URLs with {WORKERS} workers")
    results = {}
    with ThreadPoolExecutor(WORKERS) as ex:
        for u, r in zip(urls, ex.map(check_url, urls)):
            results[u] = r
    rows = []
    for p in providers:
        for f in ("website", "contact_page"):
            u = (p.get(f) or "").strip()
            if not u:
                continue
            r = dict(results[u])
            r.update(provider=p, field=f, stored=u)
            rows.append(r)
    return rows


# --------------------------------------------------------------------------
# Safe redirect rule
# --------------------------------------------------------------------------

MULTI_SUFFIX = ("k12.nh.us", "nh.us", "nh.gov", "co.uk", "org.uk", "com.au", "k12.vt.us", "k12.me.us", "k12.ma.us")
SHARED_HOSTS = ("google.com", "facebook.com", "fb.com", "instagram.com", "wixsite.com", "weebly.com",
                "wordpress.com", "blogspot.com", "squarespace.com", "linktr.ee", "square.site",
                "godaddysites.com", "myshopify.com", "carrd.co", "webflow.io", "github.io",
                "eventbrite.com", "meetup.com", "youtube.com", "linkedin.com", "twitter.com", "x.com",
                "edlio.com", "finalsite.com", "schoolwires.net", "thrillshare.com", "apptegy.net",
                "sharpschool.com", "jimdosite.com", "site123.me", "mailchimpsites.com", "canva.site")
GENERIC_SLUG = {"academy", "school", "schools", "program", "programs", "page", "domain", "site", "index",
                "home", "default", "aspx", "html", "htm", "php", "the", "and", "for", "new", "hampshire", "www"}
BAD_PATH_WORDS = ("login", "signin", "sign-in", "sso", "auth", "404", "not-found", "notfound",
                  "error", "cdn-cgi", "captcha", "sorry", "suspended", "cgi-sys", "account")


def reg_domain(host):
    host = (host or "").lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    for suf in MULTI_SUFFIX:
        if host.endswith("." + suf):
            n = suf.count(".") + 2
            return ".".join(parts[-n:])
    return ".".join(parts[-2:])


def is_safe_redirect(orig, final):
    """(safe: bool, reason: str). Safe = same organization's site and a real page."""
    o, f = urlsplit(normalize_input_url(orig)), urlsplit(final)
    if f.scheme not in ("http", "https"):
        return False, "non-http target"
    oh, fh = (o.hostname or "").lower(), (f.hostname or "").lower()
    od, fd = reg_domain(oh), reg_domain(fh)
    if od != fd:
        return False, f"different domain ({od} -> {fd})"
    if od in SHARED_HOSTS:
        if oh.removeprefix("www.") != fh.removeprefix("www.") or \
                not f.path.lower().rstrip("/").startswith(o.path.lower().rstrip("/")):
            return False, "shared hosting platform; can't confirm same organization"
    low = (f.path + "?" + f.query).lower()
    if any(w in low for w in BAD_PATH_WORDS):
        return False, "lands on a login/error-looking page"
    if o.path.strip("/") and not f.path.strip("/") and oh.removeprefix("www.") == fh.removeprefix("www."):
        # (a deep link moving to its own subdomain, e.g. sau57.org/fisk -> fisk.sau57.org, is fine)
        return False, "deep link now redirects to the homepage (page may be gone)"
    op, fp = o.path.lower().strip("/"), f.path.lower().strip("/")
    if op and fp and op != fp and op.startswith(fp + "/"):
        return False, "now redirects to a parent/listing page (the specific page may be gone)"
    oseg = set(re.findall(r"[a-z]{3,}", op.split("/")[-1])) - GENERIC_SLUG
    fseg = set(re.findall(r"[a-z]{3,}", fp.split("/")[-1] if fp else "")) - GENERIC_SLUG
    if oseg and fp and not (oseg & fseg) and not any(a in b or b in a for a in oseg for b in fseg):
        return False, "page slug changed to a different name (could be a different program/school)"
    if f.query and not o.query:
        return False, "target adds a query string"
    if len(final) > 300:
        return False, "target URL too long"
    if f.scheme == "http" and o.scheme == "https":
        return False, "downgrades https to http"
    return True, "same site"


def clean_target(final):
    sp = urlsplit(final)
    return urlunsplit((sp.scheme, sp.netloc.lower(), sp.path or "/", sp.query, ""))


# --------------------------------------------------------------------------
# DOE data
# --------------------------------------------------------------------------

STOP = {"the", "school", "schools", "of", "inc", "and", "nh", "new", "hampshire", "llc", "at", "for"}
ABBR = {"elem": "elementary", "es": "elementary", "ms": "middle", "hs": "high", "jr": "junior",
        "sr": "senior", "st": "saint", "mt": "mount", "acad": "academy", "chartered": "charter",
        "intl": "international", "ctr": "center", "centre": "center", "reg": "regional",
        "rd": "road", "ave": "avenue", "comm": "community", "prep": "preparatory"}


def norm_name(s):
    s = (s or "").lower().replace("&amp;", "&").replace("&", " and ")
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    toks = [ABBR.get(t, t) for t in s.split()]
    toks = [t for t in toks if t not in STOP and t not in ("public",)]
    return " ".join(toks)


def norm_street(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    toks = [ABBR.get(t, t) for t in s.split()]
    rep = {"street": "st", "road": "rd", "avenue": "ave", "highway": "hwy", "drive": "dr", "lane": "ln",
           "north": "n", "south": "s", "east": "e", "west": "w", "route": "rte", "rt": "rte"}
    toks = [rep.get(t, t) for t in toks]
    return " ".join(toks)


def street_key(s):
    """(number, first street word) for a loose address comparison."""
    m = re.search(r"\b(\d{1,6})\s+([a-z0-9]+(?:\s+[a-z0-9]+)?)", norm_street(s))
    if not m:
        return None
    words = [w for w in m.group(2).split() if w not in ("n", "s", "e", "w")]
    return (m.group(1), words[0] if words else "")


def load_ccd():
    """Latest NCES CCD school directory for NH (Urban Institute Education Data API)."""
    this_year = datetime.now().year
    for year in range(this_year, this_year - 4, -1):
        try:
            r = requests.get(URBAN_CCD.format(year=year), timeout=60)
            r.raise_for_status()
            rows = r.json().get("results", [])
        except Exception as e:
            log.warning(f"CCD {year}: {e}")
            continue
        if rows:
            log.info(f"CCD {year}-{(year + 1) % 100:02d}: {len(rows)} NH schools")
            return year, rows
    return None, []


def load_doe(conn):
    """Return (public_current, public_closed, nonpublic_current, source_notes)."""
    notes = []
    cur = conn.cursor()
    (pub_year,) = cur.execute("SELECT max(school_year) FROM education_statistics WHERE stat_type='school_enrollment'").fetchone()
    pub = cur.execute("SELECT school_number, school_name, district_name, sau_number FROM education_statistics "
                      "WHERE stat_type='school_enrollment' AND school_year=?", (pub_year,)).fetchall()
    (np_year,) = cur.execute("SELECT max(school_year) FROM education_statistics WHERE stat_type='nonpublic_enrollment'").fetchone()
    npub = cur.execute("SELECT school_number, school_name, town FROM education_statistics "
                       "WHERE stat_type='nonpublic_enrollment' AND school_year=?", (np_year,)).fetchall()
    notes.append(f"NH DOE iPlatform school enrollment {pub_year}: {len(pub)} public/charter schools")
    notes.append(f"NH DOE iPlatform nonpublic enrollment {np_year}: {len(npub)} nonpublic schools")

    ccd_year, ccd = load_ccd()
    if ccd_year:
        notes.append(f"NCES CCD school directory {ccd_year}-{(ccd_year + 1) % 100:02d} (NH DOE's federal "
                     f"submission, via Urban Institute Education Data API): {len(ccd)} schools, status + addresses")
    ccd_by_num = {}
    for c in ccd:
        try:
            ccd_by_num[int(str(c.get("seasch", "")).split("-")[-1])] = c
        except ValueError:
            pass

    def ccd_fields(c):
        if not c:
            return {}
        return {
            "street": (c.get("street_location") or "").title(),
            "city": (c.get("city_location") or "").title(),
            "zip": (c.get("zip_location") or "")[:5],
            "ccd_status": CCD_STATUS.get(c.get("school_status"), str(c.get("school_status"))),
            "charter": c.get("charter") == 1,
            "type": CCD_TYPE.get(c.get("school_type"), ""),
            "grades": f"{c.get('lowest_grade_offered')}-{c.get('highest_grade_offered')}",
            "district": c.get("lea_name") or "",
            "phone": c.get("phone") or "",
        }

    pub_nums = set()
    public_current = []
    public_closed = []
    for num, name, district, sau in pub:
        if not name or not num or re.match(r"^\s*(sub)?\s*total\b", name, re.I):
            continue
        if re.search(r"\bclosed\b", name, re.I):
            # iPlatform marks mid-year closures in the name, e.g. "Bluff School - Closed 10/07/2025"
            d = {"doe_id": num, "name": re.split(r"\s*-\s*closed", name, flags=re.I)[0].strip(),
                 "district": district or "", "source": f"iPlatform {pub_year}: {name}"}
            d.update(ccd_fields(ccd_by_num.get(num)))
            public_closed.append(d)
            pub_nums.add(num)
            continue
        pub_nums.add(num)
        c = ccd_by_num.get(num)
        d = {"doe_id": num, "name": name, "district": district or "", "sau": sau, "source": f"iPlatform {pub_year}"}
        d.update(ccd_fields(c))
        d["district"] = d.get("district") or district or ""
        public_current.append(d)
    # CCD schools open/new but not in iPlatform enrollment (e.g. opened after Oct 1 count)
    for num, c in ccd_by_num.items():
        if num in pub_nums:
            continue
        st = c.get("school_status")
        if st in (1, 3, 4, 5, 7, 8):
            d = {"doe_id": num, "name": c.get("school_name"), "source": f"CCD {ccd_year} only (not in iPlatform {pub_year} enrollment)"}
            d.update(ccd_fields(c))
            public_current.append(d)

    for num, c in ccd_by_num.items():
        if c.get("school_status") in (2, 6) and num not in pub_nums:
            d = {"doe_id": num, "name": c.get("school_name"), "source": f"CCD {ccd_year}: {CCD_STATUS[c['school_status']]}"}
            d.update(ccd_fields(c))
            public_closed.append(d)

    seen = set()
    nonpublic_current = []
    for n, nm, t in npub:
        if not nm or n in seen or re.match(r"^\s*(sub)?\s*total\b", nm, re.I):
            continue
        seen.add(n)
        nonpublic_current.append({"doe_id": n, "name": nm, "city": (t or "").title(), "source": f"iPlatform nonpublic {np_year}"})
    return public_current, public_closed, nonpublic_current, notes, ccd


def build_town_index(ccd, nonpublic):
    towns = set()
    zip_town = {}
    for c in ccd:
        city = (c.get("city_location") or "").title()
        if city:
            towns.add(town_norm(city))
            z = (c.get("zip_location") or "")[:5]
            if z:
                zip_town.setdefault(z, city)
    for n in nonpublic:
        if n.get("city"):
            towns.add(town_norm(n["city"]))
    return towns, zip_town


TOWN_ABBR = {"ctr": "center", "w": "west", "e": "east", "n": "north", "s": "south", "no": "north",
             "so": "south", "mt": "mount", "moultonboro": "moultonborough"}


def town_norm(t):
    toks = re.sub(r"[^a-z ]", " ", (t or "").lower()).split()
    return " ".join(TOWN_ABBR.get(x, x) for x in toks)


def town_relation(a, b):
    """1 same town, 0 unknown/village-of, -1 different."""
    a, b = town_norm(a), town_norm(b)
    if not a or not b:
        return 0
    if a == b:
        return 1
    if a.endswith(" " + b) or b.endswith(" " + a):  # West Lebanon / Lebanon, Center Conway / Conway
        return 0
    return -1


def provider_town(p, towns, zip_town):
    """Town from the provider's address: the known town name that appears LAST (after the street)."""
    for field in ("address", "geocoded_address"):
        addr = p.get(field) or ""
        low = " " + town_norm(addr) + " "
        best, pos = "", -1
        for t in towns:
            i = low.rfind(f" {t} ")
            if i > pos or (i == pos and i >= 0 and len(t) > len(best)):
                best, pos = t, i
        if best:
            return best.title()
        m = re.search(r"\b(03\d{3})\b", addr)
        if m and m.group(1) in zip_town:
            return zip_town[m.group(1)]
    return ""


OUT_OF_STATE = re.compile(r"\b(MA|VT|ME|CT|NY|RI|Massachusetts|Vermont|Maine)\b[ ,]*(\d{5})?", re.I)


def in_nh(p):
    a = (p.get("address") or "") + " " + (p.get("geocoded_address") or "")
    if re.search(r"\b(NH|New Hampshire)\b", a) or re.search(r"\b03\d{3}\b", a):
        return True
    if OUT_OF_STATE.search(a):
        return False
    return bool(p.get("town"))


def best_match(name, town, candidates, town_key="city"):
    """Best candidate by name score; same town helps, a different town costs (less for exact names)."""
    nn = norm_name(name)
    best, best_score = None, 0
    for c in candidates:
        cn = norm_name(c["name"])
        if not cn or not nn:
            continue
        raw = max(fuzz.token_sort_ratio(nn, cn), fuzz.token_set_ratio(nn, cn) - 5)
        rel = town_relation(town, c.get(town_key) or "")
        score = raw + (3 if rel == 1 else 0) - (0 if rel >= 0 else (8 if raw >= 97 else 25))
        score = min(score, 100)
        if score > best_score:
            best, best_score = c, score
    return best, best_score


def compare_doe(providers, public_current, public_closed, nonpublic_current, ccd):
    towns, zip_town = build_town_index(ccd, nonpublic_current)
    for p in providers:
        p["town"] = provider_town(p, towns, zip_town)

    pub_prov = [p for p in providers if p["styles"] & PUBLIC_STYLES]
    np_prov = [p for p in providers if p["styles"] & NONPUBLIC_STYLES and not (p["styles"] & PUBLIC_STYLES)
               and in_nh(p) and not p.get("online_only")]

    closures, renames, address_changes, not_on_np_list = [], [], [], []
    open_by_street = {}
    for d in public_current:
        k = street_key(d.get("street"))
        if k and d.get("city"):
            open_by_street.setdefault((k, d["city"].lower()), []).append(d)

    matched_doe = set()
    for p in pub_prov:
        cur, cs = best_match(p["title"], p["town"], public_current)
        clo, xs = best_match(p["title"], p["town"], public_closed)
        if clo and xs >= 90 and xs > cs:
            successor = None
            k = street_key(clo.get("street"))
            if k and clo.get("city"):
                succ = [s for s in open_by_street.get((k, clo["city"].lower()), []) if s["doe_id"] != clo["doe_id"]]
                successor = succ[0] if succ else None
            closures.append({"p": p, "doe": clo, "score": xs, "successor": successor})
            if successor:
                renames.append({"p": p, "doe": successor, "old_doe": clo, "kind": "closed; another DOE school now at the same address",
                                "score": xs})
            continue
        if cur and cs >= 85:
            matched_doe.add(cur["doe_id"])
            pk, dk = street_key(p.get("address")), street_key(cur.get("street"))
            if cs < 95 and norm_name(p["title"]) != norm_name(cur["name"]):
                # high-but-imperfect name match in the same town: possible rename
                if pk and dk and pk == dk:
                    renames.append({"p": p, "doe": cur, "kind": "name differs from DOE (same address)", "score": cs})
                elif cs < 90:
                    renames.append({"p": p, "doe": cur, "kind": "name differs from DOE", "score": cs})
            if pk and dk and pk != dk and cs >= 90:
                address_changes.append({"p": p, "doe": cur, "score": cs})
            continue
        # Fall back to an address match: same street + town but different name
        pk = street_key(p.get("address"))
        if pk and p["town"] and "Career & Technical Centers" not in p["styles"]:
            same = open_by_street.get((pk, p["town"].lower()), [])
            if len(same) == 1:
                matched_doe.add(same[0]["doe_id"])
                renames.append({"p": p, "doe": same[0], "kind": "different name at the same DOE address", "score": cs})

    # Missing public schools: DOE current not matched by any provider (search all providers, not only public)
    missing_public = []
    for d in public_current:
        if d["doe_id"] in matched_doe:
            continue
        m, s = best_match(d["name"], d.get("city") or d.get("district") or "",
                          [{"name": p["title"], "city": p["town"], "p": p} for p in providers])
        if m and s >= 80:
            continue
        missing_public.append({"doe": d, "nearest": m["p"] if m and s >= 60 else None, "score": s})

    # Nonpublic
    matched_np = set()
    for p in np_prov:
        m, s = best_match(p["title"], p["town"], nonpublic_current)
        if m and s >= 82:
            matched_np.add(m["doe_id"])
            if s < 92 and norm_name(p["title"]) != norm_name(m["name"]):
                renames.append({"p": p, "doe": m, "kind": "name differs from DOE nonpublic list", "score": s})
        else:
            not_on_np_list.append({"p": p, "nearest": m if m and s >= 60 else None, "score": s})
    missing_np = []
    for d in nonpublic_current:
        if d["doe_id"] in matched_np:
            continue
        m, s = best_match(d["name"], d.get("city", ""), [{"name": p["title"], "city": p["town"], "p": p} for p in providers])
        if m and s >= 80:
            continue
        missing_np.append({"doe": d, "nearest": m["p"] if m and s >= 60 else None, "score": s})

    return {
        "closures": closures, "renames": renames, "address_changes": address_changes,
        "missing_public": missing_public, "missing_nonpublic": missing_np, "not_on_np_list": not_on_np_list,
        "pub_prov": len(pub_prov), "np_prov": len(np_prov),
    }


# --------------------------------------------------------------------------
# Applying fixes
# --------------------------------------------------------------------------

def plan_fixes(link_rows):
    fixes, review = [], []
    for r in link_rows:
        if r["status"] != "redirected":
            continue
        safe, why = is_safe_redirect(r["stored"], r["final_url"])
        r["safe_reason"] = why
        if not (safe and r["http"] == 200):
            review.append(r)
            continue
        new = clean_target(r["final_url"])
        o = urlsplit(normalize_input_url(r["stored"]))
        if not o.path.strip("/") and not o.query:
            # A homepage link stays a homepage link: take the new scheme/host, not the
            # CMS landing path (/en-US, /home, /index.php) the root happens to redirect to.
            f = urlsplit(new)
            new = f"{f.scheme}://{f.netloc}/"
        if _canon(new) == _canon(r["stored"]):
            r["status"], r["reason"] = "ok", r["reason"] + " (redirects within the same site)"
            continue
        fixes.append({"post_id": r["provider"]["id"], "field": r["field"], "old": r["stored"], "new": new, "row": r})
    return fixes, review


def apply_fixes(fixes, backup_date, dry_run=False):
    secret = os.getenv("EDOPT_AUDIT_SECRET", "")
    if not secret:
        return None, "EDOPT_AUDIT_SECRET not set; fixes not applied"
    results = []
    for i in range(0, len(fixes), 25):
        batch = fixes[i:i + 25]
        body = json.dumps({"backup_date": backup_date, "dry_run": dry_run,
                           "fixes": [{k: f[k] for k in ("post_id", "field", "old", "new")} for f in batch]})
        ts = str(int(time.time()))
        sig = hmac.new(secret.encode(), f"{ts}.{body}".encode(), hashlib.sha256).hexdigest()
        try:
            r = requests.post(FIX_ENDPOINT, data=body, timeout=120, headers={
                "Content-Type": "application/json", "X-Edopt-Timestamp": ts, "X-Edopt-Signature": sig,
                "User-Agent": "EdOptProviderAudit/1.0"})
        except requests.RequestException as e:
            return results, f"endpoint unreachable: {type(e).__name__}"
        if r.status_code != 200:
            return results, f"endpoint returned HTTP {r.status_code}"
        results.extend(r.json().get("results", []))
    return results, ""


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def build_xlsx(path, link_rows, fixes, review, doe, notes, apply_note):
    import xlsxwriter
    wb = xlsxwriter.Workbook(path)
    hdr = wb.add_format({"bold": True, "bg_color": "#1863DC", "font_color": "#FFFFFF", "text_wrap": True,
                         "valign": "top"})
    wrap = wb.add_format({"text_wrap": True, "valign": "top"})
    link = wb.add_format({"font_color": "#1863DC", "underline": 1, "valign": "top"})
    bold = wb.add_format({"bold": True})

    def sheet(name, headers, rows, widths):
        ws = wb.add_worksheet(name[:31])
        for c, (h, w) in enumerate(zip(headers, widths)):
            ws.write(0, c, h, hdr)
            ws.set_column(c, c, w)
        for r_i, row in enumerate(rows, start=1):
            for c, v in enumerate(row):
                if isinstance(v, str) and v.startswith("http"):
                    ws.write_url(r_i, c, v[:2000], link, string=v[:255])
                else:
                    ws.write(r_i, c, "" if v is None else v, wrap)
        ws.freeze_panes(1, 0)
        if rows:
            ws.autofilter(0, 0, len(rows), len(headers) - 1)
        return ws

    by = lambda st: [r for r in link_rows if r["status"] == st]
    counts = summary_counts(link_rows, fixes, review, doe)

    ws = wb.add_worksheet("Summary")
    ws.set_column(0, 0, 48)
    ws.set_column(1, 1, 12)
    ws.write(0, 0, f"EdOpt provider audit, {datetime.now(ET):%B %-d, %Y}", bold)
    r = 2
    for label, n in counts:
        ws.write(r, 0, label)
        ws.write(r, 1, n)
        r += 1
    r += 1
    ws.write(r, 0, "Sources", bold)
    for n in notes + [apply_note]:
        r += 1
        ws.write(r, 0, n)

    def prow(p):
        return [p["title"], p["url"], p["edit_url"], ", ".join(sorted(p["styles"]))]

    phead = ["Provider", "EdOpt page", "Edit in WP", "Styles"]
    pw = [34, 40, 22, 26]

    sheet("Dead Links", phead + ["Field", "URL", "Reason"],
          [prow(r["provider"]) + [r["field"], r["stored"], r["reason"]] for r in by("dead")], pw + [12, 45, 36])
    sheet("Blocked (not dead)", phead + ["Field", "URL", "Reason"],
          [prow(r["provider"]) + [r["field"], r["stored"], r["reason"]] for r in by("blocked")], pw + [12, 45, 24])
    sheet("Errors (recheck)", phead + ["Field", "URL", "Reason"],
          [prow(r["provider"]) + [r["field"], r["stored"], r["reason"]] for r in by("error")], pw + [12, 45, 36])
    sheet("SSL Warnings", phead + ["Field", "URL", "Note"],
          [prow(r["provider"]) + [r["field"], r["stored"], r["reason"]] for r in by("ssl")], pw + [12, 45, 36])
    sheet("Redirects - Review", phead + ["Field", "Stored URL", "Redirects to", "Why not auto-fixed"],
          [prow(r["provider"]) + [r["field"], r["stored"], r["final_url"], r.get("safe_reason", "")] for r in review],
          pw + [12, 40, 40, 36])
    sheet("Auto-Fixed URLs", phead + ["Field", "Old URL", "New URL", "Result", "Backup meta key"],
          [prow(f["row"]["provider"]) + [f["field"], f["old"], f["new"], f.get("result", ""), f.get("backup_key", "")]
           for f in fixes], pw + [12, 40, 40, 22, 34])

    dhead = ["DOE school", "DOE id", "Street", "Town", "District", "Source"]

    def drow(d):
        return [d.get("name"), d.get("doe_id"), d.get("street", ""), d.get("city", ""), d.get("district", ""), d.get("source", "")]

    sheet("Closures Still Listed", phead + ["Provider address"] + dhead + ["Replaced by (same address)", "Match score"],
          [prow(c["p"]) + [c["p"].get("address", "")] + drow(c["doe"]) +
           [(c["successor"] or {}).get("name", ""), round(c["score"])] for c in doe["closures"]],
          pw + [30, 32, 9, 24, 14, 22, 28, 30, 9])
    sheet("Renames", phead + ["Provider address", "DOE name", "DOE id", "DOE street", "DOE town", "What", "Match score"],
          [prow(x["p"]) + [x["p"].get("address", ""), x["doe"]["name"], x["doe"].get("doe_id"), x["doe"].get("street", ""),
                           x["doe"].get("city", ""), x["kind"], round(x["score"])] for x in doe["renames"]],
          pw + [30, 34, 9, 24, 14, 32, 9])
    sheet("Address Changes", phead + ["Directory address", "DOE address", "DOE name", "DOE id", "Source"],
          [prow(x["p"]) + [x["p"].get("address", ""),
                           f"{x['doe'].get('street', '')}, {x['doe'].get('city', '')} {x['doe'].get('zip', '')}",
                           x["doe"]["name"], x["doe"].get("doe_id"), x["doe"].get("source", "")] for x in doe["address_changes"]],
          pw + [34, 34, 30, 9, 22])
    add_url = f"{SITE}/wp-admin/post-new.php?post_type=provider"
    sheet("Missing Public Schools", dhead + ["Type", "Grades", "Charter", "CCD status", "Closest directory listing", "Add provider"],
          [drow(m["doe"]) + [m["doe"].get("type", ""), m["doe"].get("grades", ""), "Yes" if m["doe"].get("charter") else "",
                             m["doe"].get("ccd_status", ""), (m["nearest"] or {}).get("url", ""), add_url]
           for m in doe["missing_public"]], [34, 9, 26, 14, 24, 24, 16, 8, 8, 10, 40, 30])
    sheet("Missing Nonpublic Schools", ["DOE school", "DOE id", "Town", "Source", "Closest directory listing", "Add provider"],
          [[m["doe"]["name"], m["doe"]["doe_id"], m["doe"].get("city", ""), m["doe"]["source"],
            (m["nearest"] or {}).get("url", ""), add_url] for m in doe["missing_nonpublic"]], [36, 9, 16, 22, 40, 30])
    sheet("Private Not On DOE List", phead + ["Address", "Closest DOE nonpublic name", "Score"],
          [prow(x["p"]) + [x["p"].get("address", ""), (x["nearest"] or {}).get("name", ""), round(x["score"])]
           for x in doe["not_on_np_list"]], pw + [32, 32, 8])
    wb.close()


def summary_counts(link_rows, fixes, review, doe):
    c = lambda st: sum(1 for r in link_rows if r["status"] == st)
    applied = sum(1 for f in fixes if f.get("result") == "updated")
    return [
        ("URLs checked (website + contact page)", len(link_rows)),
        ("OK", c("ok")),
        ("Redirected", c("redirected")),
        ("   auto-fixed (same site)", applied),
        ("   safe fixes not yet applied", len(fixes) - applied),
        ("   redirects needing review", len(review)),
        ("Dead", c("dead")),
        ("Blocked / bot wall (not dead)", c("blocked")),
        ("Errors (5xx, 400, SSL failures, loops; recheck)", c("error")),
        ("Loads but SSL certificate doesn't verify", c("ssl")),
        ("Closed per DOE, still listed", len(doe["closures"])),
        ("Possible renames", len(doe["renames"])),
        ("Address differs from DOE", len(doe["address_changes"])),
        ("DOE public/charter schools missing from directory", len(doe["missing_public"])),
        ("DOE nonpublic schools missing from directory", len(doe["missing_nonpublic"])),
        ("Private listings not on DOE nonpublic list", len(doe["not_on_np_list"])),
    ]


def build_email_html(counts, link_rows, doe, fixes, notes, apply_note):
    td = 'style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:14px;"'
    th = 'style="padding:6px 10px;background:#1863DC;color:#ffffff;font-size:13px;text-align:left;"'

    def table(headers, rows):
        h = "".join(f"<th {th}>{escape(x)}</th>" for x in headers)
        b = "".join("<tr>" + "".join(f"<td {td}>{cell}</td>" for cell in row) + "</tr>" for row in rows)
        return (f'<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" '
                f'style="border-collapse:collapse;margin:0 0 20px 0;"><tr>{h}</tr>{b}</table>')

    def a(url, text):
        return f'<a href="{escape(url)}" style="color:#1863DC;">{escape(text)}</a>'

    parts = [f'<p style="font-size:15px;margin:0 0 16px 0;">Monthly provider audit for edopt.org. '
             f'The full lists are in the attached spreadsheet, one tab per category.</p>']
    parts.append(table(["Category", "Count"], [[escape(l), str(n)] for l, n in counts]))

    dead = [r for r in link_rows if r["status"] == "dead"][:15]
    if dead:
        parts.append('<p style="font-size:15px;font-weight:bold;margin:0 0 8px 0;">Dead links (first 15)</p>')
        parts.append(table(["Provider", "Field", "Reason"],
                           [[a(r["provider"]["url"], r["provider"]["title"]), r["field"], escape(r["reason"])] for r in dead]))
    if doe["closures"]:
        parts.append('<p style="font-size:15px;font-weight:bold;margin:0 0 8px 0;">Closed per DOE, still listed</p>')
        parts.append(table(["Provider", "DOE record"],
                           [[a(c["p"]["url"], c["p"]["title"]), escape(f"{c['doe']['name']} ({c['doe']['source']})"
                             + (f"; now {c['successor']['name']}" if c["successor"] else ""))] for c in doe["closures"]]))
    applied = [f for f in fixes if f.get("result") == "updated"][:10]
    if applied:
        parts.append('<p style="font-size:15px;font-weight:bold;margin:0 0 8px 0;">Auto-fixed URLs (first 10)</p>')
        parts.append(table(["Provider", "Old", "New"],
                           [[a(f["row"]["provider"]["url"], f["row"]["provider"]["title"]), escape(f["old"]), escape(f["new"])]
                            for f in applied]))
    parts.append('<p style="font-size:12px;color:#6b7280;margin:0;">Sources: ' + escape("; ".join(notes)) +
                 ". " + escape(apply_note) + " Nothing was deleted or unpublished.</p>")
    inner = "".join(parts)
    return (f'<!doctype html><html><body style="margin:0;padding:0;background:#ffffff;">'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'<td align="center" style="padding:20px;"><table role="presentation" width="640" cellpadding="0" '
            f'cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;color:#1a1a2e;"><tr><td>'
            f'<p style="font-size:20px;font-weight:bold;margin:0 0 12px 0;color:#1863DC;">EdOpt Provider Audit</p>'
            f'{inner}</td></tr></table></td></tr></table></body></html>')


def send_email(subject, html, xlsx_path, to):
    import boto3
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(to)
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText("EdOpt provider audit. Open the HTML version or the attached spreadsheet.", "plain", "utf-8"))
    alt.attach(MIMEText(html, "html", "utf-8"))
    msg.attach(alt)
    with open(xlsx_path, "rb") as fh:
        part = MIMEApplication(fh.read(), _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    part.add_header("Content-Disposition", "attachment", filename=os.path.basename(xlsx_path))
    msg.attach(part)
    boto3.client("ses", region_name=AWS_REGION).send_raw_email(
        Source=EMAIL_FROM, Destinations=to, RawMessage={"Data": msg.as_string()})
    log.info(f"Audit email sent to {', '.join(to)}")


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-apply", action="store_true", help="don't change any provider")
    ap.add_argument("--no-email", action="store_true")
    ap.add_argument("--cron", action="store_true", help="exit unless it's the 9am hour in America/New_York")
    ap.add_argument("--out", help="xlsx path (default data/provider_audit/EdOpt Provider Audit YYYY-MM.xlsx)")
    ap.add_argument("--fixes-json", help="also write the planned safe fixes to this JSON file")
    ap.add_argument("--applied-json", help="results of fixes applied out-of-band (e.g. via WP Engine SSH) to report")
    ap.add_argument("--links-cache", help="reuse/save link-check results (JSON) to skip re-checking")
    args = ap.parse_args()

    now = datetime.now(ET)
    if args.cron and now.hour != 9:
        log.info(f"--cron: it's {now:%H:%M} ET, not 9am; skipping")
        return
    stamp = now.strftime("%Y%m%d")
    out = args.out or os.path.join("data", "provider_audit", f"EdOpt Provider Audit {now:%Y-%m}.xlsx")
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)

    providers = load_providers()
    by_id = {p["id"]: p for p in providers}

    if args.links_cache and os.path.exists(args.links_cache):
        cached = json.load(open(args.links_cache))
        link_rows = []
        for r in cached:
            if r["pid"] in by_id:
                r["provider"] = by_id[r.pop("pid")]
                link_rows.append(r)
        log.info(f"Loaded {len(link_rows)} cached link results")
    else:
        t0 = time.time()
        link_rows = check_all_links(providers)
        log.info(f"Link check done in {time.time() - t0:.0f}s")
        if args.links_cache:
            json.dump([{**{k: v for k, v in r.items() if k != "provider"}, "pid": r["provider"]["id"]} for r in link_rows],
                      open(args.links_cache, "w"))

    conn = sqlite3.connect(DATABASE_PATH)
    public_current, public_closed, nonpublic_current, notes, ccd = load_doe(conn)
    doe = compare_doe(providers, public_current, public_closed, nonpublic_current, ccd)

    fixes, review = plan_fixes(link_rows)
    if args.fixes_json:
        json.dump([{k: f[k] for k in ("post_id", "field", "old", "new")} for f in fixes], open(args.fixes_json, "w"), indent=1)

    if args.applied_json:
        results, err = json.load(open(args.applied_json)).get("results", []), ""
    else:
        results, err = None, ""
    if args.no_apply and not args.applied_json:
        apply_note = f"{len(fixes)} safe URL fixes identified; not applied (--no-apply)."
    else:
        if not args.applied_json:
            results, err = apply_fixes(fixes, stamp)
        if results is None:
            apply_note = f"{len(fixes)} safe URL fixes identified; not applied ({err})."
        else:
            by_key = {(r["post_id"], r["field"]): r for r in results}
            for f in fixes:
                r = by_key.get((f["post_id"], f["field"]))
                if r:
                    f["result"] = r.get("status") if r.get("status") != "skipped" else f"skipped: {r.get('reason', '')}"
                    f["backup_key"] = r.get("backup_key", "")
            n = sum(1 for f in fixes if f.get("result") == "updated")
            apply_note = (f"{n} of {len(fixes)} safe URL fixes applied; old values backed up to post meta "
                          f"_edopt_backup_<field>_{stamp}.") + (f" Stopped early: {err}." if err else "")
    log.info(apply_note)

    build_xlsx(out, link_rows, fixes, review, doe, notes, apply_note)
    log.info(f"Wrote {out}")
    counts = summary_counts(link_rows, fixes, review, doe)
    for label, n in counts:
        log.info(f"  {label}: {n}")

    if not args.no_email:
        html = build_email_html(counts, link_rows, doe, fixes, notes, apply_note)
        dead = sum(1 for r in link_rows if r["status"] == "dead")
        send_email(f"EdOpt provider audit {now:%B %Y}: {dead} dead links, {len(doe['closures'])} closures",
                   html, out, EMAIL_TO)


if __name__ == "__main__":
    main()
