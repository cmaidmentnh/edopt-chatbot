"""
Embedding generation and in-memory vector search.
Uses sentence-transformers all-MiniLM-L6-v2 (384 dims).
Embeddings stored as numpy float32 bytes in SQLite BLOB columns.
"""
import logging
import numpy as np
from sentence_transformers import SentenceTransformer

from config import EMBEDDING_MODEL, EMBEDDING_DIMS
from models import SessionLocal, ContentEmbedding

logger = logging.getLogger(__name__)

# Global model instance (loaded once)
_model = None

# In-memory cache: {content_type: [(content_id, chunk_index, text_chunk, vector), ...]}
_embedding_cache = {}


def get_model() -> SentenceTransformer:
    """Load the embedding model (lazy singleton)."""
    global _model
    if _model is None:
        logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
        _model = SentenceTransformer(EMBEDDING_MODEL, device="cpu")
        logger.info("Embedding model loaded")
    return _model


def generate_embedding(text: str) -> bytes:
    """Generate a single embedding and return as numpy bytes."""
    model = get_model()
    vector = model.encode(text, normalize_embeddings=True)
    return vector.astype(np.float32).tobytes()


def generate_batch_embeddings(texts: list) -> list:
    """Generate embeddings for a batch of texts. Returns list of numpy bytes."""
    if not texts:
        return []
    model = get_model()
    vectors = model.encode(texts, normalize_embeddings=True, batch_size=32, show_progress_bar=True)
    return [v.astype(np.float32).tobytes() for v in vectors]


def bytes_to_vector(b: bytes) -> np.ndarray:
    """Convert stored bytes back to numpy vector."""
    return np.frombuffer(b, dtype=np.float32)


def cosine_similarity(vec_a: np.ndarray, vec_b: np.ndarray) -> float:
    """Compute cosine similarity between two normalized vectors."""
    return float(np.dot(vec_a, vec_b))


def load_embeddings_into_memory():
    """Load all embeddings from SQLite into memory for fast search."""
    global _embedding_cache
    _embedding_cache = {}

    db = SessionLocal()
    try:
        all_embeddings = db.query(ContentEmbedding).all()
        for emb in all_embeddings:
            ct = emb.content_type
            if ct not in _embedding_cache:
                _embedding_cache[ct] = []
            _embedding_cache[ct].append((
                emb.content_id,
                emb.chunk_index,
                emb.text_chunk,
                bytes_to_vector(emb.embedding),
            ))
        total = sum(len(v) for v in _embedding_cache.values())
        logger.info(f"Loaded {total} embeddings into memory: "
                    + ", ".join(f"{k}={len(v)}" for k, v in _embedding_cache.items()))
    finally:
        db.close()


def search(query: str, content_type: str = None, top_k: int = 10) -> list:
    """
    Search embeddings by cosine similarity.
    Returns list of {content_type, content_id, chunk_index, text_chunk, score}.
    """
    model = get_model()
    query_vec = model.encode(query, normalize_embeddings=True).astype(np.float32)

    results = []

    if content_type and content_type in _embedding_cache:
        search_types = [content_type]
    elif content_type:
        return []  # Requested type has no embeddings
    else:
        search_types = list(_embedding_cache.keys())

    for ct in search_types:
        for content_id, chunk_index, text_chunk, stored_vec in _embedding_cache.get(ct, []):
            score = cosine_similarity(query_vec, stored_vec)
            results.append({
                "content_type": ct,
                "content_id": content_id,
                "chunk_index": chunk_index,
                "text_chunk": text_chunk,
                "score": score,
            })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]
