#!/usr/bin/env python3
"""
Ingest just the EFA Parent Handbook sections into the existing database.
Use this when you can't run the full ingest.py (e.g., GenCourt unavailable).

Usage: python3 ingest_handbook.py
"""
import logging
from datetime import datetime, timezone

from models import init_db, SessionLocal, ContentPage
from ingest import EFA_HANDBOOK_SECTIONS, generate_all_embeddings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingest_handbook")


def main():
    logger.info("Ingesting EFA Parent Handbook sections...")
    init_db()

    db = SessionLocal()
    try:
        # Upsert handbook sections
        for section in EFA_HANDBOOK_SECTIONS:
            existing = db.get(ContentPage, section["id"])
            if existing:
                existing.title = section["title"]
                existing.content_text = section["content_text"]
                existing.ingested_at = datetime.now(timezone.utc)
            else:
                db.add(ContentPage(
                    id=section["id"],
                    content_type="handbook",
                    slug=f"efa-handbook-{section['id'] - 900000}",
                    title=section["title"],
                    content_text=section["content_text"],
                    url="https://nh.scholarshipfund.org",
                    ingested_at=datetime.now(timezone.utc),
                ))
        db.commit()
        logger.info(f"Stored {len(EFA_HANDBOOK_SECTIONS)} handbook sections")

        # Regenerate all embeddings (includes handbook)
        n_embeddings = generate_all_embeddings(db)
        logger.info(f"Total embeddings: {n_embeddings}")

    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
    finally:
        db.close()


if __name__ == "__main__":
    main()
