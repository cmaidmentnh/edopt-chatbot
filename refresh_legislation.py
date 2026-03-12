#!/usr/bin/env python3
"""
Lightweight legislation refresh — pulls current bill status from GenCourt
and updates local SQLite. Safe to run frequently via cron (no embeddings,
no heavy dependencies).

Usage: python3 refresh_legislation.py
"""
import logging
import sqlite3
from datetime import datetime, timezone

from gencourt_client import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("refresh_legislation")

DB_PATH = "edopt_chatbot.db"


def refresh():
    gc = get_connection()
    gc_cur = gc.cursor(as_dict=True)

    gc_cur.execute("""
        SELECT l.legislationID, l.CondensedBillNo, l.LSRTitle,
               l.sessionyear, l.GeneralStatusCode,
               l.HouseStatusCode, l.SenateStatusCode,
               l.SubjectCode, l.HouseCurrentCommitteeCode,
               l.SenateCurrentCommitteeCode
        FROM Legislation l
        WHERE l.sessionyear = 2026
        AND (
            l.SubjectCode IN ('EDH','EDG','EDA','EDS','EDC','EDP','EDT','EDU','EDV','EDR')
            OR l.HouseCurrentCommitteeCode IN ('H05','H62')
            OR l.SenateCurrentCommitteeCode IN ('S02')
        )
        ORDER BY l.CondensedBillNo
    """)
    bills = gc_cur.fetchall()
    logger.info(f"Fetched {len(bills)} education bills from GenCourt")

    sq = sqlite3.connect(DB_PATH)
    sq_cur = sq.cursor()
    now = datetime.now(timezone.utc).isoformat()

    for bill in bills:
        bill_id = bill["legislationID"]

        # Committee name
        committee_name = None
        comm_code = bill.get("HouseCurrentCommitteeCode") or bill.get("SenateCurrentCommitteeCode")
        if comm_code:
            gc_cur.execute("""
                SELECT committeename FROM Committees
                WHERE CommitteeCode = %s AND ActiveCommittee = 1
            """, (comm_code,))
            comm = gc_cur.fetchone()
            if comm:
                committee_name = comm["committeename"]

        # Docket (last 5 entries)
        gc_cur.execute("""
            SELECT TOP 5 StatusDate, LegislativeBody, Description
            FROM Docket
            WHERE legislationid = %s
            ORDER BY statusorder DESC, StatusDate DESC
        """, (bill_id,))
        docket = gc_cur.fetchall()

        docket_lines = []
        for d in docket:
            desc = d.get("Description", "")
            if isinstance(desc, bytes):
                desc = desc.decode("utf-8", errors="replace")
            date_str = str(d.get("StatusDate", ""))[:10]
            docket_lines.append(f"{date_str}: {desc}")
        docket_summary = "\n".join(docket_lines)

        # Hearing
        gc_cur.execute("""
            SELECT TOP 1 cname, starttime, roomnbr, buildingname
            FROM VHearings
            WHERE LegislationID = %s
            ORDER BY starttime DESC
        """, (bill_id,))
        hearing = gc_cur.fetchone()
        next_hearing_date = str(hearing["starttime"])[:10] if hearing and hearing.get("starttime") else None
        next_hearing_room = f"{hearing.get('buildingname', '')} {hearing.get('roomnbr', '')}".strip() if hearing else None

        # Bill text (first 3000 chars)
        gc_cur.execute("""
            SELECT TOP 1 Text FROM LegislationText
            WHERE LegislationID = %s
            ORDER BY LegislationTextID DESC
        """, (bill_id,))
        text_row = gc_cur.fetchone()
        bill_text_summary = ""
        if text_row and text_row.get("Text"):
            bill_text_summary = text_row["Text"][:3000]

        # Sponsors
        gc_cur.execute("""
            SELECT s.PersonID, s.PrimeSponsor, s.SponsorWithdrawn,
                   leg.FirstName, leg.LastName, leg.party, leg.District, leg.LegislativeBody
            FROM Sponsors s
            LEFT JOIN Legislators leg ON s.PersonID = leg.PersonID
            WHERE s.LegislationID = %s
            ORDER BY s.PrimeSponsor DESC, leg.LastName
        """, (bill_id,))
        sponsors = gc_cur.fetchall()

        # Upsert bill
        sq_cur.execute("SELECT id FROM legislation WHERE id = ?", (bill_id,))
        if sq_cur.fetchone():
            sq_cur.execute("""
                UPDATE legislation SET
                    bill_number=?, title=?, session_year=?,
                    general_status=?, house_status=?, senate_status=?,
                    subject_code=?, bill_text_summary=?,
                    committee_name=?, next_hearing_date=?, next_hearing_room=?,
                    docket_summary=?, ingested_at=?
                WHERE id=?
            """, (
                bill["CondensedBillNo"], bill["LSRTitle"], bill["sessionyear"],
                bill["GeneralStatusCode"], bill["HouseStatusCode"], bill["SenateStatusCode"],
                bill["SubjectCode"], bill_text_summary,
                committee_name, next_hearing_date, next_hearing_room,
                docket_summary, now, bill_id
            ))
        else:
            sq_cur.execute("""
                INSERT INTO legislation (id, bill_number, title, session_year,
                    general_status, house_status, senate_status, subject_code,
                    bill_text_summary, committee_name, next_hearing_date,
                    next_hearing_room, docket_summary, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bill_id, bill["CondensedBillNo"], bill["LSRTitle"], bill["sessionyear"],
                bill["GeneralStatusCode"], bill["HouseStatusCode"], bill["SenateStatusCode"],
                bill["SubjectCode"], bill_text_summary,
                committee_name, next_hearing_date, next_hearing_room,
                docket_summary, now
            ))

        # Upsert sponsors
        sq_cur.execute("DELETE FROM legislation_sponsors WHERE legislation_id = ?", (bill_id,))
        for s in sponsors:
            if s.get("SponsorWithdrawn"):
                continue
            sq_cur.execute("""
                INSERT INTO legislation_sponsors
                    (legislation_id, person_id, first_name, last_name, party,
                     district, legislative_body, is_prime_sponsor)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bill_id, s["PersonID"], s["FirstName"], s["LastName"],
                s["party"], s["District"], s["LegislativeBody"],
                1 if s["PrimeSponsor"] else 0,
            ))

    sq.commit()
    sq.close()
    gc.close()
    logger.info(f"Updated {len(bills)} bills in local SQLite")


if __name__ == "__main__":
    try:
        refresh()
    except Exception as e:
        logger.error(f"Legislation refresh failed: {e}")
        raise
