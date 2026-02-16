"""
GenCourt SQL Server client for NH legislation and RSA lookups.
Queries NHLegislatureDB for RSA statutes, current legislation, sponsors, committees, and hearings.
"""
import logging
import pymssql
from bs4 import BeautifulSoup

from config import GENCOURT_HOST, GENCOURT_PORT, GENCOURT_USER, GENCOURT_PASS, GENCOURT_DB

logger = logging.getLogger(__name__)

# Education-related RSA chapters to ingest
EDUCATION_CHAPTERS = [
    "77-G",      # Child Care Provider Employer Tax Credit
    "170-E",     # Child Care Licensing
    "186",       # Department of Education
    "186-C",     # Special Education
    "189",       # School Attendance
    "193",       # Pupils
    "193-A",     # Home Education
    "193-B",     # Drug-Free School Zones
    "193-C",     # Statewide Testing
    "193-D",     # Safe School Zones
    "193-E",     # Adequate Public Education
    "193-F",     # Pupil Safety and Violence Prevention
    "194",       # School Districts
    "194-B",     # Charter Schools
    "194-C",     # Area Schools
    "194-D",     # STEM Education
    "194-F",     # Education Freedom Accounts
    "198",       # Education Funding
    "200",       # State Colleges and Universities
]

# Education-related subject codes used in GenCourt
EDUCATION_SUBJECT_CODES = [
    "EDH", "EDG", "EDA", "EDS", "EDC", "EDP", "EDT", "EDU", "EDV", "EDR",
]


def get_connection():
    """Get a pymssql connection to GenCourt."""
    return pymssql.connect(
        server=GENCOURT_HOST,
        port=GENCOURT_PORT,
        user=GENCOURT_USER,
        password=GENCOURT_PASS,
        database=GENCOURT_DB,
        tds_version="7.0",
    )


def clean_rsa_html(html_text: str) -> str:
    """Strip HTML from RSA text fields."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    return soup.get_text(separator="\n").strip()


def fetch_education_rsas() -> list:
    """Fetch education-related RSA sections from GenCourt."""
    conn = get_connection()
    cursor = conn.cursor(as_dict=True)

    placeholders = ",".join(["%s"] * len(EDUCATION_CHAPTERS))
    query = f"""
        SELECT id, TitleNo, ChapterNo, SectionNo,
               Title, Chapter, Section, rsa, EntireRSA, url
        FROM NH_RSA
        WHERE ChapterNo IN ({placeholders})
        ORDER BY ChapterNo, SectionNo
    """

    cursor.execute(query, tuple(EDUCATION_CHAPTERS))
    rows = cursor.fetchall()
    conn.close()

    results = []
    for row in rows:
        results.append({
            "id": row["id"],
            "title_no": row["TitleNo"],
            "chapter_no": row["ChapterNo"],
            "section_no": row["SectionNo"],
            "title_name": row["Title"],
            "chapter_name": row["Chapter"],
            "section_name": row["Section"],
            "rsa_text": clean_rsa_html(row["rsa"]),
            "entire_rsa": row["EntireRSA"],
        })

    logger.info(f"Fetched {len(results)} RSA sections")
    return results


def fetch_current_legislation(session_year: int = 2026) -> list:
    """Fetch education-related legislation for the given session year."""
    conn = get_connection()
    cursor = conn.cursor(as_dict=True)

    # Get all education bills (by subject code or committee assignment)
    query = """
        SELECT l.legislationID, l.CondensedBillNo, l.LSRTitle,
               l.sessionyear, l.GeneralStatusCode,
               l.HouseStatusCode, l.SenateStatusCode,
               l.SubjectCode, l.HouseCurrentCommitteeCode,
               l.SenateCurrentCommitteeCode
        FROM Legislation l
        WHERE l.sessionyear = %s
        AND (
            l.SubjectCode IN ('EDH', 'EDG', 'EDA', 'EDS', 'EDC', 'EDP', 'EDT', 'EDU', 'EDV', 'EDR')
            OR l.HouseCurrentCommitteeCode IN ('H05', 'H62')
            OR l.SenateCurrentCommitteeCode IN ('S02')
        )
        ORDER BY l.CondensedBillNo
    """
    cursor.execute(query, (session_year,))
    bills = cursor.fetchall()

    results = []
    for bill in bills:
        bill_id = bill["legislationID"]

        # Get sponsors
        cursor.execute("""
            SELECT s.PersonID, s.PrimeSponsor, s.SponsorWithdrawn,
                   leg.FirstName, leg.LastName, leg.party, leg.District, leg.LegislativeBody
            FROM Sponsors s
            LEFT JOIN Legislators leg ON s.PersonID = leg.PersonID
            WHERE s.LegislationID = %s
            ORDER BY s.PrimeSponsor DESC, leg.LastName
        """, (bill_id,))
        sponsors = cursor.fetchall()

        # Get latest docket entries
        cursor.execute("""
            SELECT TOP 5 StatusDate, LegislativeBody, Description
            FROM Docket
            WHERE legislationid = %s
            ORDER BY statusorder DESC, StatusDate DESC
        """, (bill_id,))
        docket = cursor.fetchall()

        # Get committee name
        committee_name = None
        comm_code = bill.get("HouseCurrentCommitteeCode") or bill.get("SenateCurrentCommitteeCode")
        if comm_code:
            cursor.execute("""
                SELECT committeename FROM Committees
                WHERE CommitteeCode = %s AND ActiveCommittee = 1
            """, (comm_code,))
            comm = cursor.fetchone()
            if comm:
                committee_name = comm["committeename"]

        # Get next hearing
        cursor.execute("""
            SELECT TOP 1 cname, starttime, roomnbr, buildingname
            FROM VHearings
            WHERE LegislationID = %s
            ORDER BY starttime DESC
        """, (bill_id,))
        hearing = cursor.fetchone()

        # Get bill text summary (first 3000 chars)
        cursor.execute("""
            SELECT TOP 1 Text FROM LegislationText
            WHERE LegislationID = %s
            ORDER BY LegislationTextID DESC
        """, (bill_id,))
        text_row = cursor.fetchone()
        bill_text_summary = ""
        if text_row and text_row.get("Text"):
            bill_text_summary = text_row["Text"][:3000]

        # Build docket summary
        docket_lines = []
        for d in docket:
            desc = d.get("Description", "")
            if isinstance(desc, bytes):
                desc = desc.decode("utf-8", errors="replace")
            date_str = str(d.get("StatusDate", ""))[:10]
            docket_lines.append(f"{date_str}: {desc}")

        results.append({
            "id": bill_id,
            "bill_number": bill["CondensedBillNo"],
            "title": bill["LSRTitle"],
            "session_year": bill["sessionyear"],
            "general_status": bill["GeneralStatusCode"],
            "house_status": bill["HouseStatusCode"],
            "senate_status": bill["SenateStatusCode"],
            "subject_code": bill["SubjectCode"],
            "bill_text_summary": bill_text_summary,
            "committee_name": committee_name,
            "next_hearing_date": str(hearing["starttime"])[:10] if hearing and hearing.get("starttime") else None,
            "next_hearing_room": f"{hearing.get('buildingname', '')} {hearing.get('roomnbr', '')}".strip() if hearing else None,
            "docket_summary": "\n".join(docket_lines),
            "sponsors": [
                {
                    "person_id": s["PersonID"],
                    "first_name": s["FirstName"],
                    "last_name": s["LastName"],
                    "party": s["party"],
                    "district": s["District"],
                    "legislative_body": s["LegislativeBody"],
                    "is_prime_sponsor": bool(s["PrimeSponsor"]),
                }
                for s in sponsors
                if not s.get("SponsorWithdrawn")
            ],
        })

    conn.close()
    logger.info(f"Fetched {len(results)} education bills for {session_year}")
    return results


def lookup_rsa_section(chapter: str, section: str) -> dict | None:
    """Direct lookup of a specific RSA section."""
    conn = get_connection()
    cursor = conn.cursor(as_dict=True)

    cursor.execute("""
        SELECT id, TitleNo, ChapterNo, SectionNo,
               Title, Chapter, Section, rsa, EntireRSA
        FROM NH_RSA
        WHERE ChapterNo = %s AND SectionNo = %s
    """, (chapter, section))

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "title_no": row["TitleNo"],
        "chapter_no": row["ChapterNo"],
        "section_no": row["SectionNo"],
        "title_name": row["Title"],
        "chapter_name": row["Chapter"],
        "section_name": row["Section"],
        "rsa_text": clean_rsa_html(row["rsa"]),
        "entire_rsa": row["EntireRSA"],
    }


def search_rsa_by_text(search_text: str, limit: int = 10) -> list:
    """Search RSA sections by keyword in section name or chapter name."""
    conn = get_connection()
    cursor = conn.cursor(as_dict=True)

    pattern = f"%{search_text}%"
    cursor.execute("""
        SELECT TOP %s id, TitleNo, ChapterNo, SectionNo,
               Title, Chapter, Section, rsa, EntireRSA
        FROM NH_RSA
        WHERE Section LIKE %s OR Chapter LIKE %s
        ORDER BY ChapterNo, SectionNo
    """, (limit, pattern, pattern))

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "title_no": row["TitleNo"],
            "chapter_no": row["ChapterNo"],
            "section_no": row["SectionNo"],
            "title_name": row["Title"],
            "chapter_name": row["Chapter"],
            "section_name": row["Section"],
            "rsa_text": clean_rsa_html(row["rsa"]),
            "entire_rsa": row["EntireRSA"],
        }
        for row in rows
    ]


def get_bill_details(bill_number: str, session_year: int = 2026) -> dict | None:
    """Get full details for a specific bill by number (e.g., 'HB 1268')."""
    conn = get_connection()
    cursor = conn.cursor(as_dict=True)

    cursor.execute("""
        SELECT legislationID, CondensedBillNo, LSRTitle,
               sessionyear, GeneralStatusCode,
               HouseStatusCode, SenateStatusCode, SubjectCode
        FROM Legislation
        WHERE CondensedBillNo = %s AND sessionyear = %s
    """, (bill_number, session_year))

    bill = cursor.fetchone()
    if not bill:
        conn.close()
        return None

    bill_id = bill["legislationID"]

    # Sponsors
    cursor.execute("""
        SELECT s.PrimeSponsor, leg.FirstName, leg.LastName, leg.party,
               leg.District, leg.LegislativeBody
        FROM Sponsors s
        LEFT JOIN Legislators leg ON s.PersonID = leg.PersonID
        WHERE s.LegislationID = %s AND (s.SponsorWithdrawn IS NULL OR s.SponsorWithdrawn = 0)
        ORDER BY s.PrimeSponsor DESC, leg.LastName
    """, (bill_id,))
    sponsors = cursor.fetchall()

    # Full docket
    cursor.execute("""
        SELECT StatusDate, LegislativeBody, Description
        FROM Docket
        WHERE legislationid = %s
        ORDER BY statusorder, StatusDate
    """, (bill_id,))
    docket = cursor.fetchall()

    # Bill text
    cursor.execute("""
        SELECT TOP 1 DocumentVersion, Text FROM LegislationText
        WHERE LegislationID = %s
        ORDER BY LegislationTextID DESC
    """, (bill_id,))
    text_row = cursor.fetchone()

    # Hearing
    cursor.execute("""
        SELECT TOP 1 cname, starttime, roomnbr, buildingname
        FROM VHearings
        WHERE LegislationID = %s
        ORDER BY starttime DESC
    """, (bill_id,))
    hearing = cursor.fetchone()

    conn.close()

    docket_lines = []
    for d in docket:
        desc = d.get("Description", "")
        if isinstance(desc, bytes):
            desc = desc.decode("utf-8", errors="replace")
        date_str = str(d.get("StatusDate", ""))[:10]
        docket_lines.append(f"{date_str}: {desc}")

    return {
        "bill_number": bill["CondensedBillNo"],
        "title": bill["LSRTitle"],
        "session_year": bill["sessionyear"],
        "general_status": bill["GeneralStatusCode"],
        "house_status": bill["HouseStatusCode"],
        "senate_status": bill["SenateStatusCode"],
        "sponsors": [
            {
                "name": f"{s['FirstName']} {s['LastName']}",
                "party": "R" if s["party"] == "r" else "D" if s["party"] == "d" else s["party"],
                "district": s["District"],
                "body": "House" if s["LegislativeBody"] == "H" else "Senate",
                "is_prime": bool(s["PrimeSponsor"]),
            }
            for s in sponsors
        ],
        "docket": docket_lines,
        "bill_text": text_row["Text"][:5000] if text_row and text_row.get("Text") else None,
        "bill_version": text_row["DocumentVersion"] if text_row else None,
        "hearing": {
            "committee": hearing["cname"],
            "date": str(hearing["starttime"])[:10],
            "room": f"{hearing.get('buildingname', '')} {hearing.get('roomnbr', '')}".strip(),
        } if hearing else None,
    }


def search_legislation_by_text(search_text: str, session_year: int = 2026, limit: int = 10) -> list:
    """Search legislation by keyword in title."""
    conn = get_connection()
    cursor = conn.cursor(as_dict=True)

    pattern = f"%{search_text}%"
    cursor.execute("""
        SELECT TOP %s legislationID, CondensedBillNo, LSRTitle,
               GeneralStatusCode, HouseStatusCode, SenateStatusCode
        FROM Legislation
        WHERE sessionyear = %s AND LSRTitle LIKE %s
        ORDER BY CondensedBillNo
    """, (limit, session_year, pattern))

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "bill_number": row["CondensedBillNo"],
            "title": row["LSRTitle"],
            "general_status": row["GeneralStatusCode"],
        }
        for row in rows
    ]
