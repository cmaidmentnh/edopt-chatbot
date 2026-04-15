"""
Claude tool definitions and handler functions.
Five tools: search_providers, lookup_rsa, search_legislation, search_content, lookup_education_stats.
"""
import json
import logging
from geopy.distance import geodesic
from fuzzywuzzy import process as fuzz_process

from models import SessionLocal, Provider, RSASection, Legislation, LegislationSponsor, ContentPage, EducationStatistic
from geo import normalize_location, NH_TOWNS, NH_COUNTIES, is_statewide_query
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
                    "description": (
                        "NH town, city, county, or region name (e.g., 'Concord', 'Hillsborough County', 'Upper Valley', 'Seacoast'). "
                        "Pass 'New Hampshire' or 'NH' for a statewide directory overview (total provider count + online/statewide options) "
                        "when the user asks aggregate questions like 'how many providers are there' or 'what's on your list'."
                    ),
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
                "keyword": {
                    "type": "string",
                    "description": "Optional keyword to filter results by relevance (e.g., 'Spanish', 'piano', 'math tutoring'). Only returns providers whose name or description matches.",
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
    {
        "name": "lookup_education_stats",
        "description": (
            "Look up NH education statistics for a specific district, school, or town. "
            "Includes enrollment (district, school, town — 15 years of history), "
            "cost per pupil (7 years), assessment/test scores (2008-2024), "
            "class size, student-teacher ratio, attendance rates, graduation rates, "
            "teacher/principal salaries, demographics (race/ethnicity, LEP, free/reduced lunch), "
            "home education counts, nonpublic enrollment, and more. "
            "Use when the user asks about school size, enrollment, spending, demographics, "
            "test scores, school performance, teacher pay, class size, or graduation rates."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "district_or_town": {
                    "type": "string",
                    "description": (
                        "District name, school name, or town (e.g., 'Concord', 'Manchester', 'Laconia'). "
                        "Pass 'New Hampshire' or 'NH' for statewide aggregates — public K-12 enrollment by grade, "
                        "home-education total, nonpublic enrollment, provider directory count, and computed "
                        "percentages (e.g. share of NH kids in home education). Use this whenever the user asks "
                        "a statewide or percentage question."
                    ),
                },
                "stat_type": {
                    "type": "string",
                    "enum": [
                        "district_enrollment", "home_education", "cost_per_pupil",
                        "nonpublic_enrollment", "free_reduced_lunch", "school_enrollment",
                        "assessment", "attendance_rate", "cohort_graduation",
                        "avg_class_size", "student_teacher_ratio", "teacher_salary",
                        "teacher_attainment", "staff_fte", "completers_school",
                        "race_ethnic", "limited_english", "town_enrollment",
                        "principal_salary", "admin_salary", "teacher_salary_schedule",
                        "adm", "equalized_valuation",
                        "all",
                    ],
                    "description": "Type of statistic to look up. Default 'all'.",
                },
                "year": {
                    "type": "string",
                    "description": "School year to filter by (e.g., '2025-26', '2024-25', 'FY2024'). Omit to get all years.",
                },
            },
            "required": ["district_or_town"],
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
        "lookup_education_stats": _handle_lookup_education_stats,
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
    keyword: str = None,
) -> str:
    """Search for education providers near a location."""
    # Statewide queries ("NH", "New Hampshire", "statewide", etc.) return the
    # full directory count plus online/statewide providers.
    if is_statewide_query(location):
        return _handle_statewide_provider_query(grade=grade, style=style, keyword=keyword)

    name, coords, is_county = normalize_location(location)
    if coords is None:
        return (
            f"Could not find '{location}' in New Hampshire. "
            "Please specify a valid NH town, city, or county name — "
            "or say 'New Hampshire' / 'statewide' for a directory overview."
        )

    grade_num = _parse_grade_input(grade)
    keyword_lower = keyword.strip().lower() if keyword else None

    # Keyword synonyms for fallback when initial search returns nothing
    KEYWORD_SYNONYMS = {
        "gifted": ["advanced", "enrichment", "talented", "accelerated", "stem"],
        "therapy": ["counseling", "support", "social-emotional", "behavioral"],
        "special needs": ["disability", "iep", "504", "accommodations", "therapeutic"],
        "music": ["piano", "guitar", "violin", "instrument", "band", "choir"],
        "art": ["arts", "creative", "painting", "drawing", "pottery"],
        "spanish": ["language", "bilingual", "immersion"],
        "math": ["stem", "tutoring", "academic"],
        "reading": ["literacy", "tutoring", "academic", "dyslexia"],
        "montessori": ["self-directed", "progressive", "alternative"],
        "classical": ["liberal arts", "great books", "traditional"],
        "waldorf": ["steiner", "progressive", "alternative"],
    }

    def _search_providers_with_keyword(providers_list, kw_lower, coords_val, radius, is_county_val, county_name):
        """Inner search function for a single keyword."""
        local = []
        online = []
        for p in providers_list:
            if kw_lower:
                searchable = " ".join(filter(None, [
                    p.title, p.description, p.styles_raw
                ])).lower()
                keyword_words = kw_lower.split()
                if not all(word in searchable for word in keyword_words):
                    from fuzzywuzzy import fuzz
                    title_lower = (p.title or "").lower()
                    if fuzz.partial_ratio(kw_lower, title_lower) < 75:
                        continue
            if p.online_only:
                online.append((p, 0.0))
            elif p.latitude and p.longitude:
                distance = geodesic(coords_val, (p.latitude, p.longitude)).miles
                if distance > radius:
                    continue
                local.append((p, distance))
            else:
                if is_county_val and p.address:
                    county_towns = NH_COUNTIES.get(county_name, {}).get("towns", [])
                    if not any(t in p.address.lower() for t in county_towns):
                        continue
                    local.append((p, 0.0))
                else:
                    continue
        return local, online

    db = SessionLocal()
    try:
        providers = db.query(Provider).all()
    finally:
        db.close()

    # Pre-filter by grade and style (applied once, not per keyword)
    filtered_providers = []
    for p in providers:
        # Grade filter
        if grade_num is not None and p.grade_start is not None and p.grade_end is not None:
            if not (p.grade_start <= grade_num <= p.grade_end):
                continue
        # Style filter
        if style and style != "any":
            if p.education_style and p.education_style != style:
                continue
        filtered_providers.append(p)

    # Search with primary keyword
    local_results, online_results = _search_providers_with_keyword(
        filtered_providers, keyword_lower, coords, radius_miles, is_county, name
    )

    # Synonym fallback: if keyword search returned nothing, try related terms
    synonym_used = None
    if keyword_lower and not local_results and not online_results:
        for key, synonyms in KEYWORD_SYNONYMS.items():
            if key in keyword_lower or keyword_lower in key:
                for syn in synonyms:
                    local_results, online_results = _search_providers_with_keyword(
                        filtered_providers, syn, coords, radius_miles, is_county, name
                    )
                    if local_results or online_results:
                        synonym_used = syn
                        break
                break

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
            nearby_text = f" Try expanding your search radius beyond {radius_miles} miles, or search by county."
        keyword_text = f" for '{keyword}'" if keyword else ""
        return (
            f"No providers found near {name.title()}{keyword_text} matching your criteria.{nearby_text} "
            "Note: this search only covers the EdOpt.org provider directory, which is growing but may not yet include all specialized providers. "
            "You might also consider online education options or Education Freedom Accounts (EFAs) "
            "which can fund a wide range of education expenses."
        )

    # Label sections for clarity
    n_local = sum(1 for p, d in results if not p.online_only)
    n_online = sum(1 for p, d in results if p.online_only)
    header = f"Found {len(results)} education provider(s) near {name.title()}:\n"
    if synonym_used:
        header = f"No exact matches for '{keyword}', but found {len(results)} related provider(s) (matched on '{synonym_used}') near {name.title()}:\n"
    lines = [header]
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
        if p.contact_page:
            parts.append(f"Contact page: {p.contact_page}")
        if p.online_only:
            parts.append("Available online statewide")
        if p.description:
            desc = p.description[:200] + "..." if len(p.description) > 200 else p.description
            parts.append(f"Description: {desc}")

        lines.append(f"- {line}")
        for part in parts:
            lines.append(f"  - {part}")

    return "\n".join(lines)


def _handle_statewide_provider_query(
    grade: str = None,
    style: str = "any",
    keyword: str = None,
) -> str:
    """Answer 'what's on your list / how many providers' at the NH level.

    Returns total count, style breakdown, and a sample of online/statewide providers
    that any family in NH can access. For narrower queries, ask for a town.
    """
    grade_num = _parse_grade_input(grade)
    keyword_lower = keyword.strip().lower() if keyword else None

    db = SessionLocal()
    try:
        providers = db.query(Provider).all()
    finally:
        db.close()

    filtered = []
    for p in providers:
        if grade_num is not None and p.grade_start is not None and p.grade_end is not None:
            if not (p.grade_start <= grade_num <= p.grade_end):
                continue
        if style and style != "any" and p.education_style and p.education_style != style:
            continue
        if keyword_lower:
            searchable = " ".join(filter(None, [p.title, p.description, p.styles_raw])).lower()
            if not all(w in searchable for w in keyword_lower.split()):
                continue
        filtered.append(p)

    total = len(filtered)
    style_counts = {}
    for p in filtered:
        s = (p.education_style or "other").lower()
        style_counts[s] = style_counts.get(s, 0) + 1
    online = [p for p in filtered if p.online_only]
    online.sort(key=lambda p: (p.title or "").lower())

    filters_note = []
    if grade:
        filters_note.append(f"grade {grade}")
    if style and style != "any":
        filters_note.append(f"{style} style")
    if keyword:
        filters_note.append(f"keyword '{keyword}'")
    filter_suffix = f" matching {', '.join(filters_note)}" if filters_note else ""

    lines = [
        f"**EdOpt.org directory — New Hampshire overview{filter_suffix}:**\n",
        f"**{total:,} total providers** in the directory.",
    ]
    if style_counts and not (style and style != "any"):
        breakdown = ", ".join(
            f"{c:,} {s}" for s, c in sorted(style_counts.items(), key=lambda x: -x[1])
        )
        lines.append(f"By type: {breakdown}.")
    lines.append("")

    if online:
        lines.append(f"**{len(online)} online/statewide provider(s)** available to any NH family:\n")
        for p in online[:15]:
            link = f"[{p.title}]({p.url})" if p.url else f"**{p.title}**"
            if p.website and p.website != p.url:
                link += f" | [Website]({p.website})"
            lines.append(f"- {link}")
            if p.description:
                desc = (p.description[:160] + "...") if len(p.description) > 160 else p.description
                lines.append(f"  - {desc}")
        if len(online) > 15:
            lines.append(f"- ...and {len(online) - 15} more online options.")
        lines.append("")

    lines.append(
        "To see *local* options (brick-and-mortar schools, tutoring, enrichment near you), "
        "tell me your town or county and I'll search within a radius."
    )
    return "\n".join(lines)


def _handle_lookup_rsa(
    chapter: str = None,
    section: str = None,
    search_text: str = None,
) -> str:
    """Look up RSA sections."""
    if chapter and section:
        # Direct lookup from local SQLite cache
        # DB stores section_no as "193-1" not "1", so try both formats
        db = SessionLocal()
        try:
            rsa = db.query(RSASection).filter_by(
                chapter_no=chapter, section_no=section
            ).first()
            if not rsa:
                # Try chapter-section format (e.g., "193-1" for chapter 193 section 1)
                alt_section = f"{chapter}-{section}"
                rsa = db.query(RSASection).filter_by(
                    chapter_no=chapter, section_no=alt_section
                ).first()
        finally:
            db.close()

        if rsa:
            text = rsa.rsa_text or "(No text available)"
            # Truncate very long statutes
            if len(text) > 3000:
                text = text[:3000] + "\n\n[Text truncated. Full text available at gencourt.state.nh.us]"
            # Clean display: use "193:1" not "193:193-1"
            display_section = rsa.section_no
            if display_section and display_section.startswith(f"{rsa.chapter_no}-"):
                display_section = display_section[len(rsa.chapter_no) + 1:]
            return (
                f"**RSA {rsa.chapter_no}:{display_section} - {rsa.section_name or ''}**\n"
                f"Chapter: {rsa.chapter_name or ''}\n"
                f"Title: {rsa.title_name or ''}\n\n"
                f"{text}"
            )

        # Try live lookup from GenCourt if not in cache
        try:
            from gencourt_client import lookup_rsa_section
            result = lookup_rsa_section(chapter, section)
        except Exception as e:
            logger.warning(f"GenCourt RSA lookup failed: {e}")
            result = None
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
                ds = r.section_no
                if ds and ds.startswith(f"{r.chapter_no}-"):
                    ds = ds[len(r.chapter_no) + 1:]
                lines.append(f"- **{r.chapter_no}:{ds}** - {r.section_name or ''}")
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
                    ds = rsa.section_no
                    if ds and ds.startswith(f"{rsa.chapter_no}-"):
                        ds = ds[len(rsa.chapter_no) + 1:]
                    lines.append(
                        f"- **RSA {rsa.chapter_no}:{ds}** - {rsa.section_name or ''}\n"
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
                updated_str = ""
                if bill.ingested_at:
                    updated_str = bill.ingested_at.strftime("%B %d, %Y at %I:%M %p UTC")
                lines = [
                    f"**{bill.bill_number}**: {bill.title}",
                    f"Session: {bill.session_year}",
                    f"Status: {status_desc}",
                    f"Data last updated: {updated_str}" if updated_str else "",
                ]
                lines = [l for l in lines if l]
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
        try:
            from gencourt_client import get_bill_details
            details = get_bill_details(bn_nospace, session_year)
            if not details:
                details = get_bill_details(bn_spaced, session_year)
        except Exception as e:
            logger.warning(f"GenCourt bill lookup failed: {e}")
            details = None
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
            # Show freshness timestamp from most recent ingestion
            latest_update = max((b.ingested_at for b in all_bills if b.ingested_at), default=None)
            if latest_update:
                lines.append(f"\nData last updated: {latest_update.strftime('%B %d, %Y at %I:%M %p UTC')}")
            lines.append(f"\nNOTE: These are the ONLY bills found matching '{search_text}'. Do NOT describe any bill not listed above. If the user asked about a specific bill that is not in these results, say you could not find it.")
            return "\n".join(lines)

        # Fallback: try embedding search (these are approximate matches, not exact)
        results = embedding_search(search_text, content_type="legislation", top_k=5)
        if results:
            lines = [f"Bills possibly related to '{search_text}' (approximate matches — may not be directly on topic):\n"]
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
                    ds = rsa.section_no
                    if ds and ds.startswith(f"{rsa.chapter_no}-"):
                        ds = ds[len(rsa.chapter_no) + 1:]
                    lines.append(f"- **RSA {rsa.chapter_no}:{ds}** - {rsa.section_name}")
                    if rsa.rsa_text:
                        lines.append(f"  {rsa.rsa_text[:200]}...")
            elif r["content_type"] == "handbook":
                page = db.get(ContentPage, r["content_id"])
                if page:
                    lines.append(f"- **{page.title}** (EFA Parent Handbook, CSFNH)")
                    text = page.content_text or ""
                    if text:
                        lines.append(f"  {text[:500]}...")
            elif r["content_type"] == "education_stat":
                stat = db.get(EducationStatistic, r["content_id"])
                if stat:
                    data = json.loads(stat.data_json)
                    label = stat.district_name or stat.school_name or stat.town or "Unknown"
                    total = data.get("total", "")
                    lines.append(f"- **{label}** ({stat.stat_type.replace('_', ' ').title()}, {stat.school_year}): {total}")
            elif r["content_type"] == "legislation":
                bill = db.get(Legislation, r["content_id"])
                if bill:
                    lines.append(f"- **{bill.bill_number}**: {bill.title}")
    finally:
        db.close()

    return "\n".join(lines)


    # SAU number → primary district name mapping
SAU_NAMES = {
    1: "Contoocook Valley", 2: "Inter-Lakes Cooperative", 3: "Berlin",
    4: "Newfound Area", 5: "Oyster River", 6: "Claremont",
    7: "Colebrook", 8: "Concord", 9: "Conway",
    10: "Derry Cooperative", 11: "Dover", 12: "Londonderry",
    13: "Tamworth", 14: "Epping", 15: "Hooksett",
    16: "Exeter", 17: "Sanborn Regional", 18: "Franklin",
    19: "Goffstown", 20: "Gorham", 21: "Winnacunnet",
    23: "Haverhill Cooperative", 24: "Henniker", 25: "Bedford",
    26: "Merrimack", 27: "Litchfield", 28: "Pelham",
    29: "Keene", 30: "Laconia", 31: "Newmarket",
    32: "Plainfield", 33: "Raymond", 34: "Hillsboro-Deering",
    35: "Bethlehem", 36: "White Mountains Regional", 37: "Manchester",
    39: "Amherst", 40: "Milford", 41: "Hollis-Brookline",
    42: "Nashua", 43: "Newport", 44: "Northwood",
    45: "Moultonborough", 46: "Merrimack Valley", 47: "Jaffrey-Rindge",
    48: "Plymouth", 49: "Governor Wentworth Regional", 50: "Greenland",
    51: "Pittsfield", 52: "Portsmouth", 53: "Pembroke",
    54: "Rochester", 55: "Timberlane Regional", 56: "Somersworth",
    57: "Salem", 58: "Northumberland", 59: "Winnisquam Regional",
    60: "Fall Mountain Regional", 61: "Farmington", 62: "Mascoma Valley",
    63: "Wilton", 64: "Milton", 65: "Kearsarge Regional",
    66: "Hopkinton", 67: "Bow", 68: "Lincoln-Woodstock",
    70: "Hanover", 72: "Alton", 73: "Gilford",
    74: "Barrington", 75: "Grantham", 76: "Lyme",
    77: "Monroe", 78: "Rivendell Interstate", 79: "Gilmanton",
    80: "Shaker Regional", 81: "Hudson", 82: "Chester",
    83: "Fremont", 84: "Littleton", 85: "Sunapee",
    86: "Barnstead", 87: "Mascenic Regional", 88: "Lebanon",
    89: "Mason", 90: "Hampton", 92: "Hinsdale",
    93: "Monadnock Regional", 94: "Winchester", 95: "Windham",
    99: "Croydon",
}


def _resolve_sau_query(search: str) -> str:
    """Resolve SAU number references like 'SAU 6' or 'SAU6' to district names."""
    import re
    match = re.match(r'^sau\s*#?\s*(\d+)$', search.strip().lower())
    if match:
        sau_num = int(match.group(1))
        if sau_num in SAU_NAMES:
            return SAU_NAMES[sau_num]
    return search


def _handle_lookup_education_stats(
    district_or_town: str,
    stat_type: str = "all",
    year: str = None,
) -> str:
    """Look up education statistics for a district, school, or town."""
    search = district_or_town.strip()

    # Statewide aggregates: "New Hampshire", "NH", "statewide", etc.
    if is_statewide_query(search):
        return _handle_statewide_education_stats(stat_type=stat_type, year=year)

    # Resolve SAU number references (e.g., "SAU 6" -> "Claremont")
    search = _resolve_sau_query(search)

    # Detect county queries (e.g., "Sullivan County", "Sullivan")
    county_towns = _detect_county_query(search)

    db = SessionLocal()
    try:
        query = db.query(EducationStatistic)
        if stat_type and stat_type != "all":
            query = query.filter(EducationStatistic.stat_type == stat_type)
        if year:
            query = query.filter(EducationStatistic.school_year == year)

        if county_towns:
            # County-based search: filter by towns belonging to the county
            results = _filter_by_county_towns(query, county_towns)
        else:
            # Standard search: ILIKE match on name fields
            pattern = f"%{search}%"
            results = query.filter(
                (EducationStatistic.district_name.ilike(pattern)) |
                (EducationStatistic.school_name.ilike(pattern)) |
                (EducationStatistic.town.ilike(pattern)) |
                (EducationStatistic.sau_name.ilike(pattern))
            ).all()

            # Fuzzy fallback if no results
            if not results:
                all_stats = query.all()
                candidates = {}
                for s in all_stats:
                    for name in (s.district_name, s.school_name, s.town, s.sau_name):
                        if name and name not in candidates:
                            candidates[name] = name
                if candidates:
                    match, score = fuzz_process.extractOne(search, list(candidates.keys()))
                    if score >= 70:
                        pattern = f"%{match}%"
                        results = query.filter(
                            (EducationStatistic.district_name.ilike(pattern)) |
                            (EducationStatistic.school_name.ilike(pattern)) |
                            (EducationStatistic.town.ilike(pattern)) |
                            (EducationStatistic.sau_name.ilike(pattern))
                        ).all()

        if not results:
            return f"No education statistics found for '{district_or_town}'. Try a different district, school, or town name."

        return _format_education_stats(results, search)
    finally:
        db.close()


def _handle_statewide_education_stats(stat_type: str = "all", year: str = None) -> str:
    """Aggregate education statistics across all of New Hampshire.

    Uses the pre-aggregated state_totals rows where available (by grade, 10-year
    trend), sums district-level rows for per-district types, and aggregates town
    rows for town_enrollment. Returns a readable summary.
    """
    db = SessionLocal()
    try:
        default_year = "2025-26"
        target_year = year or default_year
        lines = [f"**New Hampshire statewide education statistics** (school year {target_year}):\n"]

        # Helpers
        def sum_total(stat, yr):
            rows = db.query(EducationStatistic).filter_by(
                stat_type=stat, school_year=yr
            ).all()
            total = 0
            for r in rows:
                try:
                    v = json.loads(r.data_json).get("total")
                    if isinstance(v, (int, float)):
                        total += v
                except Exception:
                    pass
            return total, len(rows)

        def state_totals_by_grade(yr_key):
            """Pull the state_totals rows (one per grade, district_name='public: X')
            and return a dict of grade_label -> enrollment for yr_key (e.g. '25 - 26')."""
            rows = db.query(EducationStatistic).filter(
                EducationStatistic.stat_type == "state_totals",
                EducationStatistic.district_name.like("public:%"),
            ).all()
            out = {}
            for r in rows:
                try:
                    d = json.loads(r.data_json)
                    v = d.get(yr_key)
                    if isinstance(v, (int, float)):
                        grade = (r.district_name or "").replace("public:", "").strip()
                        out[grade] = v
                except Exception:
                    pass
            return out

        # Convert '2025-26' to the key used inside state_totals data (e.g. '25 - 26')
        def year_trend_key(yr):
            if yr and "-" in yr and len(yr) >= 7:
                left, right = yr.split("-")
                return f"{left[-2:]} - {right[-2:]}"
            return None

        want = stat_type or "all"

        # Directory: provider count (useful for "how many options on the list?")
        if want in ("all", "providers", "directory"):
            ptotal = db.query(Provider).count()
            lines.append(f"- **EdOpt.org provider directory:** {ptotal:,} providers statewide.")

        # Public K-12 enrollment (use state_totals — pre-aggregated, no double count)
        if want in ("all", "state_totals", "district_enrollment", "public_enrollment"):
            key = year_trend_key(target_year)
            grades = state_totals_by_grade(key) if key else {}
            if grades:
                pub_total = sum(v for g, v in grades.items() if g.lower() != "preschool")
                pre = grades.get("Preschool", 0)
                lines.append(
                    f"- **Public school enrollment (K–12):** {pub_total:,} students."
                    + (f" Plus {pre:,} public preschool." if pre else "")
                )
                # Brief grade distribution (K, elementary 1-5, middle 6-8, high 9-12)
                def bucket(prefix_list):
                    return sum(v for g, v in grades.items() if any(g == p for p in prefix_list))
                k = grades.get("Kindergarten", 0)
                elem = bucket([f"Grade {i}" for i in range(1, 6)])
                mid = bucket([f"Grade {i}" for i in range(6, 9)])
                high = bucket([f"Grade {i}" for i in range(9, 13)])
                if any((k, elem, mid, high)):
                    lines.append(
                        f"  Breakdown — K: {k:,} · Elem (1–5): {elem:,} · Middle (6–8): {mid:,} · High (9–12): {high:,}"
                    )

        # Home education
        if want in ("all", "home_education"):
            he_total, he_rows = sum_total("home_education", target_year)
            lines.append(
                f"- **Home-educated students (non-EFA, reported under RSA 193-A):** {he_total:,} "
                f"({he_rows} reporting districts)."
            )
            lines.append(
                "  Note: this counts students whose families file home-education notification "
                "with their local district. EFA students are reported separately to CSFNH."
            )

        # Nonpublic enrollment
        if want in ("all", "nonpublic_enrollment"):
            np_total, np_rows = sum_total("nonpublic_enrollment", target_year)
            lines.append(f"- **Nonpublic school enrollment:** {np_total:,} students ({np_rows} schools reporting).")

        # If the user asked for a specific stat_type that's not covered above, fall back
        if want not in (
            "all", "providers", "directory",
            "state_totals", "district_enrollment", "public_enrollment",
            "home_education", "nonpublic_enrollment",
        ):
            rows = db.query(EducationStatistic).filter_by(
                stat_type=want, school_year=target_year
            ).all()
            if rows:
                lines.append(
                    f"- **{want.replace('_',' ').title()}:** {len(rows):,} rows statewide for {target_year}. "
                    "Provide a specific district or town to see detail."
                )
            else:
                lines.append(
                    f"- No statewide rows found for stat_type='{want}' in {target_year}. "
                    "Try a specific district or 'all' for a summary."
                )

        # If "all" summary, add homeschool percentage as a computed convenience
        if want == "all":
            key = year_trend_key(target_year)
            grades = state_totals_by_grade(key) if key else {}
            pub_total = sum(v for g, v in grades.items() if g.lower() != "preschool")
            he_total, _ = sum_total("home_education", target_year)
            np_total, _ = sum_total("nonpublic_enrollment", target_year)
            denom = pub_total + he_total + np_total
            if denom > 0 and he_total > 0:
                pct = 100 * he_total / denom
                lines.append("")
                lines.append(
                    f"**Share of NH school-age students in home education:** "
                    f"~{pct:.1f}% ({he_total:,} of {denom:,} K–12 students across public + nonpublic + home ed)."
                )

        return "\n".join(lines)
    finally:
        db.close()


def _detect_county_query(search: str) -> list[str] | None:
    """Check if search term is a county name. Returns list of towns or None."""
    s = search.lower().replace(" county", "").strip()
    county_data = NH_COUNTIES.get(s)
    if county_data:
        return county_data.get("towns", [])
    return None


def _filter_by_county_towns(query, towns: list[str]) -> list:
    """Filter education stats to only records matching towns in a county.
    Matches district_name or town field against county town list."""
    from sqlalchemy import or_

    conditions = []
    for town in towns:
        pattern = f"%{town}%"
        conditions.append(EducationStatistic.district_name.ilike(pattern))
        conditions.append(EducationStatistic.town.ilike(pattern))
    if not conditions:
        return []
    return query.filter(or_(*conditions)).all()


def _format_education_stats(results, search_term: str) -> str:
    """Format education statistics into readable output."""
    by_type = {}
    for r in results:
        by_type.setdefault(r.stat_type, []).append(r)

    lines = [f"Education statistics for '{search_term}':\n"]

    if "district_enrollment" in by_type:
        lines.append("**District Enrollment (2025-26):**")
        for r in by_type["district_enrollment"]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.district_name}: **{data['total']:,}** total students")
            parts = []
            for key, label in [("preschool", "PreK"), ("kindergarten", "K"),
                               ("elementary", "Elem"), ("middle", "Middle"),
                               ("high", "High")]:
                if data.get(key):
                    parts.append(f"{label}: {data[key]:,}")
            if parts:
                lines.append(f"  Breakdown: {', '.join(parts)}")
        lines.append("")

    if "school_enrollment" in by_type:
        lines.append("**School Enrollment (2025-26):**")
        for r in by_type["school_enrollment"][:15]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.school_name}: **{data['total']:,}** students")
            grade_parts = []
            for key, label in [("preschool", "PreK"), ("kindergarten", "K")]:
                if data.get(key):
                    grade_parts.append(f"{label}: {data[key]:,}")
            for g in range(1, 13):
                if data.get(f"grade{g}"):
                    grade_parts.append(f"Gr{g}: {data[f'grade{g}']:,}")
            if grade_parts:
                lines.append(f"  {', '.join(grade_parts)}")
        if len(by_type["school_enrollment"]) > 15:
            lines.append(f"  ... and {len(by_type['school_enrollment']) - 15} more schools")
        lines.append("")

    if "home_education" in by_type:
        lines.append("**Home Education Enrollment (2025-26):**")
        for r in by_type["home_education"]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.district_name}: **{data['total']:,}** home-educated students")
        lines.append("")

    if "cost_per_pupil" in by_type:
        # Group by year, show most recent first
        cpp_by_year = {}
        for r in by_type["cost_per_pupil"]:
            cpp_by_year.setdefault(r.school_year, []).append(r)
        years = sorted(cpp_by_year.keys(), reverse=True)[:3]  # show up to 3 years
        for yr in years:
            lines.append(f"**Cost Per Pupil ({yr}, excluding transportation):**")
            for r in cpp_by_year[yr]:
                data = json.loads(r.data_json)
                lines.append(f"- {r.district_name}: **${data['total']:,}** per pupil overall")
                parts = []
                for key, label in [("elementary", "Elementary"), ("middle", "Middle"), ("high", "High")]:
                    if data.get(key):
                        parts.append(f"{label}: ${data[key]:,}")
                if parts:
                    lines.append(f"  {', '.join(parts)}")
            lines.append("")

    if "nonpublic_enrollment" in by_type:
        lines.append("**Nonpublic School Enrollment (2025-26):**")
        for r in by_type["nonpublic_enrollment"]:
            data = json.loads(r.data_json)
            town_info = f" ({r.town})" if r.town else ""
            lines.append(f"- {r.school_name}{town_info}: **{data['total']:,}** students")
        lines.append("")

    if "free_reduced_lunch" in by_type:
        lines.append("**Free/Reduced Lunch Eligibility (2025-26):**")
        for r in by_type["free_reduced_lunch"][:15]:
            data = json.loads(r.data_json)
            name = r.school_name or r.district_name
            pct = data.get("pct_eligible")
            pct_str = f" ({pct}%)" if pct is not None else ""
            lines.append(f"- {name}: {data.get('eligible', 0):,} of {data['enrollment']:,} eligible{pct_str}")
        if len(by_type["free_reduced_lunch"]) > 15:
            lines.append(f"  ... and {len(by_type['free_reduced_lunch']) - 15} more schools")
        lines.append("")

    if "assessment" in by_type:
        # Group assessments by year, then school, then show all-grades summaries
        by_year = {}
        for r in by_type["assessment"]:
            by_year.setdefault(r.school_year, []).append(r)

        # If showing alongside other stats, limit to most recent year
        assessment_only = len(by_type) == 1
        years_to_show = sorted(by_year.keys(), reverse=True)
        if not assessment_only:
            years_to_show = years_to_show[:1]  # most recent only

        for year in years_to_show:
            year_records = by_year[year]
            lines.append(f"**Assessment Results ({year}):**")

            # Group by school
            by_school = {}
            for r in year_records:
                by_school.setdefault(r.school_name, []).append(r)

            subject_names = {"mat": "Math", "rea": "Reading/ELA", "sci": "Science"}
            school_count = 0
            for school_name in sorted(by_school.keys()):
                if school_count >= 10:
                    lines.append(f"  ... and {len(by_school) - 10} more schools")
                    break
                school_count += 1
                school_records = by_school[school_name]

                # Show all-grades summary first
                summaries = [r for r in school_records if json.loads(r.data_json).get("grade") == "all"]
                grade_records = [r for r in school_records if json.loads(r.data_json).get("grade") != "all"]

                if summaries:
                    subject_parts = []
                    for s in sorted(summaries, key=lambda x: json.loads(x.data_json)["subject"]):
                        d = json.loads(s.data_json)
                        subj = subject_names.get(d["subject"], d["subject"])
                        above = d.get("pct_above_proficient", "?")
                        subject_parts.append(f"{subj}: {above}% proficient")
                    lines.append(f"- {school_name}: {'; '.join(subject_parts)}")
                elif grade_records:
                    # No all-grades summary, just show per-grade
                    subject_parts = []
                    for s in sorted(grade_records[:3], key=lambda x: json.loads(x.data_json)["subject"]):
                        d = json.loads(s.data_json)
                        subj = subject_names.get(d["subject"], d["subject"])
                        above = d.get("pct_above_proficient", "?")
                        subject_parts.append(f"{subj} Gr{d['grade']}: {above}%")
                    lines.append(f"- {school_name}: {'; '.join(subject_parts)}")
            lines.append("")

    # === NEW STAT TYPES ===

    if "attendance_rate" in by_type:
        lines.append("**Attendance Rate:**")
        for r in by_type["attendance_rate"][:10]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.district_name} ({r.school_year}): **{data['total_pct']}%** overall")
            parts = []
            for key, label in [("elementary_pct", "Elem"), ("middle_pct", "Middle"), ("high_pct", "High")]:
                if data.get(key):
                    parts.append(f"{label}: {data[key]}%")
            if parts:
                lines.append(f"  {', '.join(parts)}")
        lines.append("")

    if "cohort_graduation" in by_type:
        lines.append("**Graduation/Dropout Rate:**")
        for r in by_type["cohort_graduation"][:10]:
            data = json.loads(r.data_json)
            name = r.school_name or r.district_name
            grad = data.get("graduation_rate", "?")
            lines.append(f"- {name} ({r.school_year}): **{grad}%** graduation rate (cohort: {data.get('cohort_size', '?')})")
            if data.get("dropout_rate"):
                lines.append(f"  Dropout rate: {data['dropout_rate']}%")
        lines.append("")

    if "avg_class_size" in by_type:
        lines.append("**Average Class Size (Elementary):**")
        for r in by_type["avg_class_size"][:10]:
            data = json.loads(r.data_json)
            parts = []
            for key, label in [("grades_1_2", "Gr 1-2"), ("grades_3_4", "Gr 3-4"), ("grades_5_8", "Gr 5-8")]:
                if data.get(key):
                    parts.append(f"{label}: {data[key]}")
            lines.append(f"- {r.district_name}: {', '.join(parts)}")
        lines.append("")

    if "avg_class_size_school" in by_type:
        lines.append("**Average Class Size By School (Elementary):**")
        for r in by_type["avg_class_size_school"][:15]:
            data = json.loads(r.data_json)
            parts = []
            for key, label in [("grades_1_2", "Gr 1-2"), ("grades_3_4", "Gr 3-4"), ("grades_5_8", "Gr 5-8")]:
                if data.get(key):
                    parts.append(f"{label}: {data[key]}")
            lines.append(f"- {r.school_name}: {', '.join(parts)}")
        lines.append("")

    if "student_teacher_ratio" in by_type:
        lines.append("**Student-Teacher Ratio:**")
        for r in by_type["student_teacher_ratio"][:10]:
            data = json.loads(r.data_json)
            ratio = data.get("ratio", "?")
            lines.append(f"- {r.district_name} ({r.school_year}): **{ratio}:1** ({data.get('enrollment', '?')} students, {data.get('teachers', '?')} teachers)")
        lines.append("")

    if "teacher_salary" in by_type:
        lines.append("**Teacher Average Salary:**")
        sal_by_year = {}
        for r in by_type["teacher_salary"]:
            sal_by_year.setdefault(r.school_year, []).append(r)
        for yr in sorted(sal_by_year.keys(), reverse=True)[:2]:
            for r in sal_by_year[yr]:
                data = json.loads(r.data_json)
                lines.append(f"- {r.district_name} ({yr}): **${data['avg_salary']:,}**")
        lines.append("")

    if "teacher_attainment" in by_type:
        lines.append("**Teacher Educational Attainment:**")
        for r in by_type["teacher_attainment"][:10]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.district_name}: {data.get('num_teachers', '?')} teachers — Bachelor's: {data.get('pct_bachelors', '?')}%, Master's: {data.get('pct_masters', '?')}%, Beyond: {data.get('pct_beyond_masters', '?')}%")
        lines.append("")

    if "staff_fte" in by_type:
        lines.append("**Staff FTE:**")
        for r in by_type["staff_fte"][:10]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.district_name}: Teachers: {data.get('teachers', '?')}, Support: {data.get('instruction_support', '?')}, Specialists: {data.get('specialists', '?')}")
        lines.append("")

    if "limited_english" in by_type:
        lines.append("**Limited English Proficiency:**")
        for r in by_type["limited_english"][:10]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.district_name}: {data.get('el_eligible', 0)} EL eligible of {data.get('enrollment', '?')} enrolled")
        lines.append("")

    if "race_ethnic" in by_type:
        lines.append("**Race/Ethnicity Enrollment:**")
        for r in by_type["race_ethnic"][:10]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.district_name}: White: {data.get('white_pct', '?')}, Hispanic: {data.get('hispanic_pct', '?')}, Asian: {data.get('asian_pacific_pct', '?')}, Black: {data.get('black_pct', '?')}, Multi: {data.get('multi_race_pct', '?')}")
        lines.append("")

    if "completers_school" in by_type:
        lines.append("**Graduation Outcomes:**")
        for r in by_type["completers_school"][:10]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.school_name}: {data.get('total', '?')} completers — 4yr college: {data.get('four_year_college_pct', '?')}%, employed: {data.get('employed_pct', '?')}%")
        lines.append("")

    if "principal_salary" in by_type:
        lines.append("**Principal Salary:**")
        for r in by_type["principal_salary"][:10]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.school_name} ({data.get('contact_type', '')}): **${data['salary']:,}**")
        lines.append("")

    if "admin_salary" in by_type:
        lines.append("**Admin Salary (by SAU):**")
        sal_by_year = {}
        for r in by_type["admin_salary"]:
            sal_by_year.setdefault(r.school_year, []).append(r)
        for yr in sorted(sal_by_year.keys(), reverse=True)[:2]:
            lines.append(f"  *{yr}:*")
            for r in sal_by_year[yr]:
                data = json.loads(r.data_json)
                name = r.sau_name or r.district_name or "Unknown SAU"
                role = data.get("contact_type", "Administrator")
                salary = data.get("salary", 0)
                lines.append(f"- {name} — {role}: **${salary:,}**")
        lines.append("")

    if "teacher_salary_schedule" in by_type:
        lines.append("**Teacher Salary Schedule:**")
        for r in by_type["teacher_salary_schedule"][:10]:
            data = json.loads(r.data_json)
            min_s = f"${data['min_salary']:,}" if data.get('min_salary') else '?'
            max_s = f"${data['max_salary']:,}" if data.get('max_salary') else '?'
            lines.append(f"- {r.district_name} ({data.get('degree_type', '?')}): {min_s} - {max_s} ({data.get('steps', '?')} steps)")
        lines.append("")

    if "town_enrollment" in by_type:
        lines.append("**Town Enrollment:**")
        for r in by_type["town_enrollment"][:10]:
            data = json.loads(r.data_json)
            lines.append(f"- {r.town} ({r.school_year}): **{data['total']:,}** students")
        lines.append("")

    # Generic fallback for types without custom formatting
    for st in by_type:
        if st not in {
            "district_enrollment", "school_enrollment", "home_education",
            "cost_per_pupil", "nonpublic_enrollment", "free_reduced_lunch",
            "assessment", "attendance_rate", "cohort_graduation",
            "avg_class_size", "avg_class_size_school", "student_teacher_ratio",
            "teacher_salary", "teacher_attainment", "staff_fte",
            "limited_english", "race_ethnic", "completers_school",
            "principal_salary", "admin_salary", "teacher_salary_schedule", "town_enrollment",
            "sau_enrollment",
        }:
            label = st.replace("_", " ").title()
            lines.append(f"**{label}:**")
            for r in by_type[st][:10]:
                data = json.loads(r.data_json)
                name = r.district_name or r.school_name or r.sau_name or r.town or "Unknown"
                total = data.get("total", data.get("salary", data.get("rate", "")))
                lines.append(f"- {name} ({r.school_year}): {total}")
            if len(by_type[st]) > 10:
                lines.append(f"  ... and {len(by_type[st]) - 10} more")
            lines.append("")

    return "\n".join(lines)
