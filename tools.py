"""
Claude tool definitions and handler functions.
Four tools: search_providers, lookup_rsa, search_legislation, search_content.
"""
import logging
from geopy.distance import geodesic

from models import SessionLocal, Provider, RSASection, Legislation, LegislationSponsor, ContentPage
from geo import normalize_location, NH_TOWNS, NH_COUNTIES
from embeddings import search as embedding_search

logger = logging.getLogger(__name__)

# Tool definitions for Claude Messages API
TOOLS = [
    {
        "name": "search_providers",
        "description": (
            "Search for education providers near a New Hampshire location. "
            "Use when the user asks about schools, programs, tutoring, enrichment, "
            "or education options in a specific area."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "NH town, city, or county name (e.g., 'Concord', 'Hillsborough County')",
                },
                "grade": {
                    "type": "string",
                    "description": "Grade level: 'Pre-K', 'K', '1' through '12', or 'Post-Secondary'. Omit if not specified.",
                },
                "style": {
                    "type": "string",
                    "enum": ["public", "private", "homeschool", "charter", "enrichment", "online", "preschool", "any"],
                    "description": "Education style filter. Use 'any' if the user hasn't specified a preference.",
                },
                "radius_miles": {
                    "type": "integer",
                    "description": "Search radius in miles. Default 20. Increase to 50 if few results found.",
                },
            },
            "required": ["location"],
        },
    },
    {
        "name": "lookup_rsa",
        "description": (
            "Look up a specific New Hampshire RSA (Revised Statute Annotated) section. "
            "Use when the user asks about NH education law, legal requirements, "
            "homeschool notification rules, EFA eligibility, or specific RSA references. "
            "Common education RSAs: 193-A (Home Education), 194-F (EFA Program), "
            "194-B (Charter Schools), 193-E (Adequate Public Education), 193:1 (Compulsory Attendance)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "chapter": {
                    "type": "string",
                    "description": "RSA chapter number (e.g., '193-A', '194-F', '194-B')",
                },
                "section": {
                    "type": "string",
                    "description": "Section number within the chapter (e.g., '1', '2', '3')",
                },
                "search_text": {
                    "type": "string",
                    "description": "Free-text search if the specific chapter/section is unknown (e.g., 'home education notification')",
                },
            },
        },
    },
    {
        "name": "search_legislation",
        "description": (
            "Search current NH legislation (bills) in the 2026 session. "
            "Use when the user asks about pending education bills, specific bill numbers "
            "like 'HB 1268' or 'SB 295', or wants to know about proposed education law changes."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "bill_number": {
                    "type": "string",
                    "description": "Specific bill number like 'HB 1268' or 'SB 295'",
                },
                "search_text": {
                    "type": "string",
                    "description": "Free-text search of bill titles (e.g., 'education freedom', 'homeschool')",
                },
                "session_year": {
                    "type": "integer",
                    "description": "Legislative session year. Default 2026.",
                },
            },
        },
    },
    {
        "name": "search_content",
        "description": (
            "Search EdOpt.org educational content including blog posts, guides, glossary, "
            "and resource pages. Use when the user asks general questions about education options, "
            "EFA application process, differences between school types, or educational terminology."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query (e.g., 'EFA application process', 'what is a charter school')",
                },
                "content_type": {
                    "type": "string",
                    "enum": ["post", "page", "any"],
                    "description": "Filter by content type. Default 'any'.",
                },
            },
            "required": ["query"],
        },
    },
]


def execute_tool(tool_name: str, tool_input: dict) -> str:
    """Route tool call to the appropriate handler."""
    handlers = {
        "search_providers": _handle_search_providers,
        "lookup_rsa": _handle_lookup_rsa,
        "search_legislation": _handle_search_legislation,
        "search_content": _handle_search_content,
    }
    handler = handlers.get(tool_name)
    if not handler:
        return f"Unknown tool: {tool_name}"
    try:
        return handler(**tool_input)
    except Exception as e:
        logger.error(f"Tool {tool_name} failed: {e}", exc_info=True)
        return f"Error executing {tool_name}: {str(e)}"


def _parse_grade_input(grade_str: str) -> int | None:
    """Parse user grade input to integer."""
    if not grade_str:
        return None
    s = grade_str.strip().lower()
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


def _handle_search_providers(
    location: str,
    grade: str = None,
    style: str = "any",
    radius_miles: int = 20,
) -> str:
    """Search for education providers near a location."""
    name, coords, is_county = normalize_location(location)
    if coords is None:
        return (
            f"Could not find '{location}' in New Hampshire. "
            "Please specify a valid NH town, city, or county name."
        )

    grade_num = _parse_grade_input(grade)

    db = SessionLocal()
    try:
        providers = db.query(Provider).all()
    finally:
        db.close()

    local_results = []
    online_results = []
    for p in providers:
        # Grade filter (apply to all)
        if grade_num is not None and p.grade_start is not None and p.grade_end is not None:
            if not (p.grade_start <= grade_num <= p.grade_end):
                continue

        # Style filter (apply to all)
        if style and style != "any":
            if p.education_style and p.education_style != style:
                continue

        # Distance filter and categorization
        if p.online_only:
            online_results.append((p, 0.0))
        elif p.latitude and p.longitude:
            distance = geodesic(coords, (p.latitude, p.longitude)).miles
            if distance > radius_miles:
                continue
            local_results.append((p, distance))
        else:
            # No coordinates and not online — skip unless county search matches address
            if is_county and p.address:
                county_towns = NH_COUNTIES.get(name, {}).get("towns", [])
                if not any(t in p.address.lower() for t in county_towns):
                    continue
                local_results.append((p, 0.0))
            else:
                continue

    # Sort local by distance, then append online providers after
    local_results.sort(key=lambda x: x[1])
    online_results.sort(key=lambda x: x[0].title)  # alphabetical for online

    # Prioritize local results; only fill remaining slots with online
    max_local = min(len(local_results), 8)  # Reserve most slots for local
    max_online = 10 - max_local  # Fill rest with online
    results = local_results[:max_local] + online_results[:max_online]

    if not results:
        nearby_text = ""
        if not is_county:
            # Suggest expanding radius
            nearby_text = f" Try expanding your search radius beyond {radius_miles} miles, or search by county."
        return (
            f"No providers found near {name.title()} matching your criteria.{nearby_text} "
            "You might also consider online education options or Education Freedom Accounts (EFAs) "
            "which can fund a wide range of education expenses."
        )

    # Label sections for clarity
    n_local = sum(1 for p, d in results if not p.online_only)
    n_online = sum(1 for p, d in results if p.online_only)
    lines = [f"Found {len(results)} education provider(s) near {name.title()}:\n"]
    if n_local > 0 and n_online > 0:
        lines.append("**Local Options:**\n")

    shown_online_header = False
    for p, dist in results:
        # Add online section header when transitioning
        if p.online_only and not shown_online_header and n_local > 0:
            shown_online_header = True
            lines.append("\n**Online/Statewide Options:**\n")

        # Provider name with EdOpt profile link
        line = f"**{p.title}**"
        if p.url:
            line = f"[{p.title}]({p.url})"
        # Add direct website link inline if available and different from EdOpt profile
        if p.website and p.website != p.url:
            line += f" | [Website]({p.website})"

        parts = []
        if p.address:
            parts.append(f"Address: {p.address}")
        if dist > 0:
            parts.append(f"Distance: {dist:.1f} miles")
        if p.education_style:
            parts.append(f"Type: {p.education_style.title()}")
        if p.styles_raw:
            parts.append(f"Styles: {p.styles_raw}")
        if p.grade_start is not None and p.grade_end is not None:
            gs = "Pre-K" if p.grade_start == -1 else "K" if p.grade_start == 0 else str(p.grade_start)
            ge = "Post-Secondary" if p.grade_end == 13 else "K" if p.grade_end == 0 else str(p.grade_end)
            parts.append(f"Grades: {gs} - {ge}")
        if p.contact_phone:
            parts.append(f"Phone: {p.contact_phone}")
        if p.contact_email:
            parts.append(f"Email: {p.contact_email}")
        if p.online_only:
            parts.append("Available online statewide")
        if p.description:
            desc = p.description[:200] + "..." if len(p.description) > 200 else p.description
            parts.append(f"Description: {desc}")

        lines.append(f"- {line}")
        for part in parts:
            lines.append(f"  - {part}")

    return "\n".join(lines)


def _handle_lookup_rsa(
    chapter: str = None,
    section: str = None,
    search_text: str = None,
) -> str:
    """Look up RSA sections."""
    if chapter and section:
        # Direct lookup from local SQLite cache
        db = SessionLocal()
        try:
            rsa = db.query(RSASection).filter_by(
                chapter_no=chapter, section_no=section
            ).first()
        finally:
            db.close()

        if rsa:
            text = rsa.rsa_text or "(No text available)"
            # Truncate very long statutes
            if len(text) > 3000:
                text = text[:3000] + "\n\n[Text truncated. Full text available at gencourt.state.nh.us]"
            return (
                f"**RSA {rsa.chapter_no}:{rsa.section_no} - {rsa.section_name or ''}**\n"
                f"Chapter: {rsa.chapter_name or ''}\n"
                f"Title: {rsa.title_name or ''}\n\n"
                f"{text}"
            )

        # Try live lookup from GenCourt if not in cache
        from gencourt_client import lookup_rsa_section
        result = lookup_rsa_section(chapter, section)
        if result:
            text = result["rsa_text"] or "(No text available)"
            if len(text) > 3000:
                text = text[:3000] + "\n\n[Text truncated]"
            return (
                f"**RSA {result['chapter_no']}:{result['section_no']} - {result['section_name'] or ''}**\n"
                f"Chapter: {result['chapter_name'] or ''}\n\n"
                f"{text}"
            )

        return f"RSA {chapter}:{section} not found. Check the chapter and section numbers."

    if chapter and not section:
        # Return all sections in a chapter
        db = SessionLocal()
        try:
            rsas = db.query(RSASection).filter_by(chapter_no=chapter).order_by(RSASection.section_no).all()
        finally:
            db.close()

        if rsas:
            lines = [f"**RSA Chapter {chapter} - {rsas[0].chapter_name or ''}**\n"]
            lines.append(f"Found {len(rsas)} sections:\n")
            for r in rsas[:20]:  # Limit to 20 sections
                summary = (r.rsa_text or "")[:150]
                lines.append(f"- **{r.chapter_no}:{r.section_no}** - {r.section_name or ''}")
                if summary:
                    lines.append(f"  {summary}...")
            if len(rsas) > 20:
                lines.append(f"\n... and {len(rsas) - 20} more sections. Ask about a specific section for full text.")
            return "\n".join(lines)

        return f"No RSA sections found for chapter {chapter}."

    if search_text:
        # Semantic search against RSA embeddings
        results = embedding_search(search_text, content_type="rsa", top_k=5)
        if results:
            lines = [f"RSA sections matching '{search_text}':\n"]
            seen_ids = set()
            for r in results:
                if r["content_id"] in seen_ids:
                    continue
                seen_ids.add(r["content_id"])
                # Get the full RSA record
                db = SessionLocal()
                try:
                    rsa = db.get(RSASection, r["content_id"])
                finally:
                    db.close()
                if rsa:
                    text_preview = (rsa.rsa_text or "")[:200]
                    lines.append(
                        f"- **RSA {rsa.chapter_no}:{rsa.section_no}** - {rsa.section_name or ''}\n"
                        f"  {text_preview}..."
                    )
            return "\n".join(lines)

        return f"No RSA sections found matching '{search_text}'."

    return "Please provide either a chapter/section number or search text to look up an RSA."


def _handle_search_legislation(
    bill_number: str = None,
    search_text: str = None,
    session_year: int = 2026,
) -> str:
    """Search current legislation."""
    if bill_number:
        # Normalize bill number — try both with and without space
        bn = bill_number.strip().upper()
        # Remove spaces for the no-space variant
        bn_nospace = bn.replace(" ", "")
        # Add space for the space variant
        bn_spaced = bn_nospace
        for prefix in ("CACR", "HB", "SB", "HR", "SR"):
            if bn_nospace.startswith(prefix):
                bn_spaced = prefix + " " + bn_nospace[len(prefix):]
                break

        # First check local cache (try both formats)
        db = SessionLocal()
        try:
            bill = db.query(Legislation).filter(
                Legislation.bill_number.in_([bn_nospace, bn_spaced])
            ).first()
            if bill:
                sponsors = db.query(LegislationSponsor).filter_by(
                    legislation_id=bill.id
                ).order_by(LegislationSponsor.is_prime_sponsor.desc()).all()

                sponsor_lines = []
                for s in sponsors:
                    party = "R" if s.party == "r" else "D" if s.party == "d" else s.party or ""
                    role = " (Prime Sponsor)" if s.is_prime_sponsor else ""
                    body = "House" if s.legislative_body == "H" else "Senate" if s.legislative_body == "S" else ""
                    sponsor_lines.append(f"  - {s.first_name} {s.last_name} ({party}, {body} Dist. {s.district}){role}")

                status_desc = _describe_status(bill.general_status)
                lines = [
                    f"**{bill.bill_number}**: {bill.title}",
                    f"Session: {bill.session_year}",
                    f"Status: {status_desc}",
                ]
                if bill.committee_name:
                    lines.append(f"Committee: {bill.committee_name}")
                if bill.next_hearing_date:
                    lines.append(f"Hearing: {bill.next_hearing_date} at {bill.next_hearing_room or 'TBD'}")
                if sponsor_lines:
                    lines.append(f"Sponsors ({len(sponsor_lines)}):")
                    lines.extend(sponsor_lines[:10])
                if bill.docket_summary:
                    lines.append(f"\nRecent Activity:")
                    for dline in bill.docket_summary.split("\n")[:5]:
                        lines.append(f"  {dline}")
                if bill.bill_text_summary:
                    summary = bill.bill_text_summary[:1500]
                    lines.append(f"\nBill Text Summary:\n{summary}...")

                return "\n".join(lines)
        finally:
            db.close()

        # Try live lookup from GenCourt (try both formats)
        from gencourt_client import get_bill_details
        details = get_bill_details(bn_nospace, session_year)
        if not details:
            details = get_bill_details(bn_spaced, session_year)
        if details:
            lines = [
                f"**{details['bill_number']}**: {details['title']}",
                f"Status: {_describe_status(details['general_status'])}",
            ]
            if details.get("sponsors"):
                lines.append(f"Sponsors ({len(details['sponsors'])}):")
                for s in details["sponsors"][:10]:
                    role = " (Prime)" if s["is_prime"] else ""
                    lines.append(f"  - {s['name']} ({s['party']}, {s['body']}){role}")
            if details.get("hearing"):
                h = details["hearing"]
                lines.append(f"Hearing: {h['date']} at {h['room']} ({h['committee']})")
            if details.get("docket"):
                lines.append("Recent Activity:")
                for d in details["docket"][-5:]:
                    lines.append(f"  {d}")
            return "\n".join(lines)

        return f"Bill {bill_number} not found in the {session_year} session."

    if search_text:
        # Synonym map for common education topic searches
        SEARCH_SYNONYMS = {
            "open enrollment": ["open enrollment", "school assignment", "school choice", "district enrollment", "transfer"],
            "school choice": ["school choice", "open enrollment", "education freedom", "education options"],
            "homeschool": ["home education", "homeschool", "home school", "home instruction"],
            "efa": ["education freedom account", "EFA", "scholarship account"],
            "charter": ["charter school", "charter", "public academy"],
            "voucher": ["voucher", "education freedom account", "scholarship", "tuition"],
            "special education": ["special education", "disability", "IEP", "504"],
        }

        # Build list of search terms: original + synonyms
        search_terms = [search_text]
        for key, synonyms in SEARCH_SYNONYMS.items():
            if key in search_text.lower():
                search_terms.extend(s for s in synonyms if s.lower() != search_text.lower())
                break

        # Search by title keyword across all terms
        db = SessionLocal()
        try:
            seen_ids = set()
            all_bills = []
            for term in search_terms:
                pattern = f"%{term}%"
                bills = db.query(Legislation).filter(
                    Legislation.title.ilike(pattern),
                    Legislation.session_year == session_year,
                ).order_by(Legislation.bill_number).limit(10).all()
                for b in bills:
                    if b.id not in seen_ids:
                        seen_ids.add(b.id)
                        all_bills.append(b)
        finally:
            db.close()

        if all_bills:
            lines = [f"Bills matching '{search_text}' in {session_year} session:\n"]
            for b in all_bills[:15]:
                status = _describe_status(b.general_status)
                lines.append(f"- **{b.bill_number}**: {b.title} [{status}]")
            return "\n".join(lines)

        # Fallback: try embedding search
        results = embedding_search(search_text, content_type="legislation", top_k=5)
        if results:
            lines = [f"Bills related to '{search_text}':\n"]
            seen = set()
            for r in results:
                if r["content_id"] in seen:
                    continue
                seen.add(r["content_id"])
                db = SessionLocal()
                try:
                    bill = db.get(Legislation, r["content_id"])
                finally:
                    db.close()
                if bill:
                    status = _describe_status(bill.general_status)
                    lines.append(f"- **{bill.bill_number}**: {bill.title} [{status}]")
            return "\n".join(lines)

        return f"No bills found matching '{search_text}' in the {session_year} session."

    return "Please provide a bill number or search text."


def _describe_status(code) -> str:
    """Convert GenCourt status code to human-readable description."""
    status_map = {
        "01": "In Legislative Services (drafting)",
        "02": "In House",
        "03": "In Senate",
        "04": "Passed both chambers",
        "05": "Signed by Governor (enacted)",
        "06": "Became law without signature",
        "07": "Vetoed by Governor",
        "08": "Pocket vetoed",
        "09": "Veto overridden",
        "10": "Miscellaneous",
    }
    if code is None:
        return "Unknown"
    return status_map.get(str(code).zfill(2), f"Status code {code}")


def _handle_search_content(
    query: str,
    content_type: str = "any",
) -> str:
    """Search EdOpt.org content via embeddings."""
    ct = content_type if content_type != "any" else None
    results = embedding_search(query, content_type=ct, top_k=5)

    if not results:
        return f"No content found matching '{query}' on EdOpt.org."

    # Get full records for context
    db = SessionLocal()
    try:
        lines = [f"EdOpt.org content matching '{query}':\n"]
        seen = set()
        for r in results:
            key = (r["content_type"], r["content_id"])
            if key in seen:
                continue
            seen.add(key)

            if r["content_type"] == "provider":
                p = db.get(Provider, r["content_id"])
                if p:
                    lines.append(f"- **[{p.title}]({p.url})** (Provider)")
                    if p.description:
                        lines.append(f"  {p.description[:300]}...")
            elif r["content_type"] in ("post", "page"):
                page = db.get(ContentPage, r["content_id"])
                if page:
                    label = "Blog Post" if page.content_type == "post" else "Page"
                    lines.append(f"- **[{page.title}]({page.url})** ({label})")
                    text = page.content_text or page.excerpt or ""
                    if text:
                        lines.append(f"  {text[:300]}...")
            elif r["content_type"] == "rsa":
                rsa = db.get(RSASection, r["content_id"])
                if rsa:
                    lines.append(f"- **RSA {rsa.chapter_no}:{rsa.section_no}** - {rsa.section_name}")
                    if rsa.rsa_text:
                        lines.append(f"  {rsa.rsa_text[:200]}...")
            elif r["content_type"] == "handbook":
                page = db.get(ContentPage, r["content_id"])
                if page:
                    lines.append(f"- **{page.title}** (EFA Parent Handbook, CSFNH)")
                    text = page.content_text or ""
                    if text:
                        lines.append(f"  {text[:500]}...")
            elif r["content_type"] == "legislation":
                bill = db.get(Legislation, r["content_id"])
                if bill:
                    lines.append(f"- **{bill.bill_number}**: {bill.title}")
    finally:
        db.close()

    return "\n".join(lines)
