#!/usr/bin/env python3
"""
EdOpt Chatbot Ingestion Pipeline.
Pulls content from WordPress API + GenCourt DB into local SQLite + generates embeddings.

Usage: python3 ingest.py
"""
import json
import logging
import sys
from datetime import datetime, timezone

from models import (
    init_db, SessionLocal, Provider, ContentPage, StyleTaxonomy,
    RSASection, Legislation, LegislationSponsor, ContentEmbedding,
)
from wp_client import fetch_styles, fetch_all_providers, fetch_all_posts, fetch_all_pages
from gencourt_client import fetch_education_rsas, fetch_current_legislation
from embeddings import generate_batch_embeddings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingest")


def ingest_wordpress(db):
    """Pull providers, posts, pages, and styles from EdOpt.org."""
    logger.info("=== Phase 1: WordPress Content ===")

    # Fetch styles taxonomy first
    logger.info("Fetching style taxonomy...")
    styles_dict = fetch_styles()
    for sid, sdata in styles_dict.items():
        existing = db.get(StyleTaxonomy, sid)
        if existing:
            existing.name = sdata["name"]
            existing.slug = sdata["slug"]
            existing.parent_id = sdata["parent_id"]
            existing.description = sdata["description"]
            existing.count = sdata["count"]
        else:
            db.add(StyleTaxonomy(
                id=sid, name=sdata["name"], slug=sdata["slug"],
                parent_id=sdata["parent_id"], description=sdata["description"],
                count=sdata["count"],
            ))
    db.commit()
    logger.info(f"Stored {len(styles_dict)} style taxonomy terms")

    # Fetch providers
    logger.info("Fetching providers...")
    providers = fetch_all_providers(styles_dict)
    for p in providers:
        existing = db.get(Provider, p["id"])
        if existing:
            for key, val in p.items():
                setattr(existing, key, val)
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(Provider(**p, ingested_at=datetime.now(timezone.utc)))
    db.commit()
    logger.info(f"Stored {len(providers)} providers")

    # Fetch posts
    logger.info("Fetching posts...")
    posts = fetch_all_posts()
    for p in posts:
        existing = db.get(ContentPage, p["id"])
        if existing:
            for key, val in p.items():
                setattr(existing, key, val)
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(ContentPage(**p, ingested_at=datetime.now(timezone.utc)))
    db.commit()
    logger.info(f"Stored {len(posts)} posts")

    # Fetch pages
    logger.info("Fetching pages...")
    pages = fetch_all_pages()
    for p in pages:
        existing = db.get(ContentPage, p["id"])
        if existing:
            for key, val in p.items():
                setattr(existing, key, val)
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(ContentPage(**p, ingested_at=datetime.now(timezone.utc)))
    db.commit()
    logger.info(f"Stored {len(pages)} pages")

    return len(providers), len(posts), len(pages)


def ingest_gencourt_rsas(db):
    """Pull education-related RSA sections from GenCourt."""
    logger.info("=== Phase 2: GenCourt RSA Sections ===")

    rsas = fetch_education_rsas()
    for r in rsas:
        existing = db.query(RSASection).filter_by(
            chapter_no=r["chapter_no"], section_no=r["section_no"]
        ).first()
        if existing:
            for key, val in r.items():
                if key != "id":
                    setattr(existing, key, val)
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(RSASection(
                title_no=r["title_no"], chapter_no=r["chapter_no"],
                section_no=r["section_no"], title_name=r["title_name"],
                chapter_name=r["chapter_name"], section_name=r["section_name"],
                rsa_text=r["rsa_text"], entire_rsa=r["entire_rsa"],
            ))
    db.commit()
    logger.info(f"Stored {len(rsas)} RSA sections")
    return len(rsas)


def ingest_gencourt_legislation(db):
    """Pull current session education bills from GenCourt."""
    logger.info("=== Phase 3: GenCourt Legislation ===")

    bills = fetch_current_legislation()

    for b in bills:
        # Upsert bill
        existing = db.get(Legislation, b["id"])
        if existing:
            existing.bill_number = b["bill_number"]
            existing.title = b["title"]
            existing.session_year = b["session_year"]
            existing.general_status = b["general_status"]
            existing.house_status = b["house_status"]
            existing.senate_status = b["senate_status"]
            existing.subject_code = b["subject_code"]
            existing.bill_text_summary = b["bill_text_summary"]
            existing.committee_name = b["committee_name"]
            existing.next_hearing_date = b["next_hearing_date"]
            existing.next_hearing_room = b["next_hearing_room"]
            existing.docket_summary = b["docket_summary"]
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(Legislation(
                id=b["id"], bill_number=b["bill_number"], title=b["title"],
                session_year=b["session_year"], general_status=b["general_status"],
                house_status=b["house_status"], senate_status=b["senate_status"],
                subject_code=b["subject_code"], bill_text_summary=b["bill_text_summary"],
                committee_name=b["committee_name"], next_hearing_date=b["next_hearing_date"],
                next_hearing_room=b["next_hearing_room"], docket_summary=b["docket_summary"],
            ))

        # Upsert sponsors (delete old, insert new)
        db.query(LegislationSponsor).filter_by(legislation_id=b["id"]).delete()
        for s in b.get("sponsors", []):
            db.add(LegislationSponsor(
                legislation_id=b["id"],
                person_id=s["person_id"],
                first_name=s["first_name"],
                last_name=s["last_name"],
                party=s["party"],
                district=s["district"],
                legislative_body=s["legislative_body"],
                is_prime_sponsor=s["is_prime_sponsor"],
            ))

    db.commit()
    logger.info(f"Stored {len(bills)} education bills")
    return len(bills)


def chunk_text(text: str, max_tokens: int = 512) -> list:
    """Split text into chunks of approximately max_tokens words."""
    if not text:
        return []
    words = text.split()
    if len(words) <= max_tokens:
        return [text]
    chunks = []
    for i in range(0, len(words), max_tokens - 50):  # 50-word overlap
        chunk = " ".join(words[i:i + max_tokens])
        if chunk.strip():
            chunks.append(chunk)
    return chunks


def generate_all_embeddings(db):
    """Generate embeddings for all content in the database."""
    logger.info("=== Phase 4: Generating Embeddings ===")

    # Clear old embeddings
    db.query(ContentEmbedding).delete()
    db.commit()

    all_records = []  # (content_type, content_id, chunk_index, text)

    # Providers
    providers = db.query(Provider).all()
    for p in providers:
        text = f"{p.title}. {p.description or ''}. Styles: {p.styles_raw or ''}. Location: {p.address or ''}"
        all_records.append(("provider", p.id, 0, text.strip()))
    logger.info(f"Prepared {len(providers)} provider embedding texts")

    # Content pages (posts + pages)
    pages = db.query(ContentPage).all()
    for page in pages:
        text = f"{page.title}. {page.content_text or ''}"
        chunks = chunk_text(text)
        for i, chunk in enumerate(chunks):
            all_records.append((page.content_type, page.id, i, chunk))
    logger.info(f"Prepared {sum(1 for r in all_records if r[0] in ('post', 'page'))} content page chunks")

    # RSA sections
    rsas = db.query(RSASection).all()
    for r in rsas:
        text = f"RSA {r.chapter_no}:{r.section_no} - {r.section_name or ''}. {r.chapter_name or ''}. {r.rsa_text or ''}"
        chunks = chunk_text(text)
        for i, chunk in enumerate(chunks):
            all_records.append(("rsa", r.id, i, chunk))
    logger.info(f"Prepared {sum(1 for r in all_records if r[0] == 'rsa')} RSA embedding chunks")

    # Legislation
    bills = db.query(Legislation).all()
    for b in bills:
        sponsors = db.query(LegislationSponsor).filter_by(legislation_id=b.id).all()
        sponsor_names = ", ".join(f"{s.first_name} {s.last_name}" for s in sponsors)
        text = f"{b.bill_number} - {b.title}. Sponsors: {sponsor_names}"
        all_records.append(("legislation", b.id, 0, text.strip()))
    logger.info(f"Prepared {len(bills)} legislation embedding texts")

    # Batch generate embeddings
    logger.info(f"Generating embeddings for {len(all_records)} total chunks...")
    texts = [r[3] for r in all_records]
    embeddings = generate_batch_embeddings(texts)

    # Store in DB
    for (content_type, content_id, chunk_index, text_chunk), emb_bytes in zip(all_records, embeddings):
        db.add(ContentEmbedding(
            content_type=content_type,
            content_id=content_id,
            chunk_index=chunk_index,
            text_chunk=text_chunk,
            embedding=emb_bytes,
        ))

    db.commit()
    logger.info(f"Stored {len(all_records)} embeddings")
    return len(all_records)


def main():
    """Run the full ingestion pipeline."""
    logger.info("Starting EdOpt chatbot ingestion...")
    init_db()

    db = SessionLocal()
    try:
        n_providers, n_posts, n_pages = ingest_wordpress(db)
        n_rsas = ingest_gencourt_rsas(db)
        n_bills = ingest_gencourt_legislation(db)
        n_embeddings = generate_all_embeddings(db)

        summary = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "providers": n_providers,
            "posts": n_posts,
            "pages": n_pages,
            "rsa_sections": n_rsas,
            "legislation": n_bills,
            "embeddings": n_embeddings,
        }

        with open("last_ingest.json", "w") as f:
            json.dump(summary, f, indent=2)

        logger.info("=== Ingestion Complete ===")
        logger.info(f"Providers: {n_providers}")
        logger.info(f"Posts: {n_posts}")
        logger.info(f"Pages: {n_pages}")
        logger.info(f"RSA Sections: {n_rsas}")
        logger.info(f"Education Bills: {n_bills}")
        logger.info(f"Total Embeddings: {n_embeddings}")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
