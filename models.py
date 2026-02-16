from sqlalchemy import (
    create_engine, Column, Integer, Float, Text, Boolean,
    String, DateTime, LargeBinary, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timezone

from config import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class Provider(Base):
    __tablename__ = "providers"

    id = Column(Integer, primary_key=True)  # WordPress post ID
    slug = Column(String, nullable=False)
    title = Column(String, nullable=False)
    description = Column(Text)
    content_text = Column(Text)
    url = Column(String, nullable=False)
    address = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    encoded_address = Column(String)
    geocoded_address = Column(String)
    grade_start = Column(Integer)  # -1=PreK, 0=K, 1-12, 13=Post-Secondary
    grade_end = Column(Integer)
    age_range_start = Column(String)
    age_range_end = Column(String)
    education_style = Column(String)  # public, private, homeschool, charter, enrichment, online
    styles_raw = Column(String)  # Comma-separated style names
    focus = Column(String)
    website = Column(String)
    contact_name = Column(String)
    contact_email = Column(String)
    contact_phone = Column(String)
    online_only = Column(Boolean, default=False)
    date_published = Column(String)
    date_modified = Column(String)
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class ContentPage(Base):
    __tablename__ = "content_pages"

    id = Column(Integer, primary_key=True)  # WordPress post/page ID
    content_type = Column(String, nullable=False)  # 'post' or 'page'
    slug = Column(String, nullable=False)
    title = Column(String, nullable=False)
    content_text = Column(Text)
    excerpt = Column(Text)
    url = Column(String, nullable=False)
    author_id = Column(Integer)
    date_published = Column(String)
    date_modified = Column(String)
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class StyleTaxonomy(Base):
    __tablename__ = "style_taxonomy"

    id = Column(Integer, primary_key=True)  # WordPress taxonomy term ID
    name = Column(String, nullable=False)
    slug = Column(String, nullable=False)
    parent_id = Column(Integer)
    description = Column(Text)
    count = Column(Integer, default=0)


class RSASection(Base):
    __tablename__ = "rsa_sections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title_no = Column(String)
    chapter_no = Column(String)
    section_no = Column(String)
    title_name = Column(String)
    chapter_name = Column(String)
    section_name = Column(String)
    rsa_text = Column(Text)
    entire_rsa = Column(String)
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint("chapter_no", "section_no", name="uq_rsa_chapter_section"),
    )


class Legislation(Base):
    __tablename__ = "legislation"

    id = Column(Integer, primary_key=True)  # GenCourt legislationID
    bill_number = Column(String, nullable=False)  # e.g., 'HB 1268'
    title = Column(String, nullable=False)
    session_year = Column(Integer, default=2026)
    general_status = Column(String)
    house_status = Column(String)
    senate_status = Column(String)
    subject_code = Column(String)
    bill_text_summary = Column(Text)
    committee_name = Column(String)
    next_hearing_date = Column(String)
    next_hearing_room = Column(String)
    docket_summary = Column(Text)
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class LegislationSponsor(Base):
    __tablename__ = "legislation_sponsors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    legislation_id = Column(Integer, nullable=False)
    person_id = Column(Integer)
    first_name = Column(String)
    last_name = Column(String)
    party = Column(String)
    district = Column(String)
    legislative_body = Column(String)  # 'H' or 'S'
    is_prime_sponsor = Column(Boolean, default=False)


class ContentEmbedding(Base):
    __tablename__ = "content_embeddings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    content_type = Column(String, nullable=False)  # provider, post, page, rsa, legislation
    content_id = Column(Integer, nullable=False)
    chunk_index = Column(Integer, default=0)
    text_chunk = Column(Text, nullable=False)
    embedding = Column(LargeBinary, nullable=False)  # 384-dim float32 numpy bytes
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint("content_type", "content_id", "chunk_index",
                         name="uq_embedding_content_chunk"),
    )


class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id = Column(String, primary_key=True)  # UUID
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_active = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    ip_address = Column(String)
    metadata_json = Column(Text)


class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String, nullable=False)
    role = Column(String, nullable=False)  # 'user' or 'assistant'
    content = Column(Text, nullable=False)
    tool_calls_json = Column(Text)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# Indexes
Index("idx_providers_location", Provider.latitude, Provider.longitude)
Index("idx_providers_style", Provider.education_style)
Index("idx_providers_grades", Provider.grade_start, Provider.grade_end)
Index("idx_content_type", ContentPage.content_type)
Index("idx_rsa_chapter", RSASection.chapter_no)
Index("idx_legislation_bill", Legislation.bill_number)
Index("idx_embeddings_type", ContentEmbedding.content_type, ContentEmbedding.content_id)
Index("idx_messages_session", ChatMessage.session_id, ChatMessage.created_at)
Index("idx_sessions_active", ChatSession.last_active)


def init_db():
    """Create all tables."""
    Base.metadata.create_all(engine)


def get_db():
    """Get a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
