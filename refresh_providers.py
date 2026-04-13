#!/usr/bin/env python3
"""
Refresh the providers table from WordPress without touching embeddings.
Also performs an idempotent ALTER to add contact_page if missing.

Usage: python3 refresh_providers.py
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import text

from models import engine, SessionLocal, Provider
from wp_client import fetch_styles, fetch_all_providers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("refresh_providers")


def ensure_contact_page_column():
    with engine.begin() as conn:
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(providers)"))}
        if "contact_page" not in cols:
            conn.execute(text("ALTER TABLE providers ADD COLUMN contact_page VARCHAR"))
            log.info("Added contact_page column to providers")
        else:
            log.info("contact_page column already present")


def main():
    ensure_contact_page_column()

    styles = fetch_styles()
    providers = fetch_all_providers(styles)
    log.info(f"Fetched {len(providers)} providers from WordPress")

    db = SessionLocal()
    try:
        with_cp = 0
        updated = 0
        created = 0
        for p in providers:
            if (p.get("contact_page") or "").strip():
                with_cp += 1
            existing = db.get(Provider, p["id"])
            if existing:
                for k, v in p.items():
                    setattr(existing, k, v)
                existing.ingested_at = datetime.now(timezone.utc)
                updated += 1
            else:
                db.add(Provider(**p, ingested_at=datetime.now(timezone.utc)))
                created += 1
        db.commit()
        log.info(f"Upserted: {updated} updated, {created} created, {with_cp} have contact_page")
    finally:
        db.close()


if __name__ == "__main__":
    main()
