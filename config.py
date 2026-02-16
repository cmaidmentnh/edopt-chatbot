import os
from dotenv import load_dotenv

load_dotenv()

# Claude API
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-sonnet-4-5-20250929"

# WordPress API
WP_API_BASE = os.getenv("WP_API_BASE", "https://edopt.org/wp-json/wp/v2")
WP_USER = os.getenv("WP_USER", "")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")

# GenCourt SQL Server (credentials must be in .env)
GENCOURT_HOST = os.getenv("GENCOURT_HOST", "")
GENCOURT_PORT = int(os.getenv("GENCOURT_PORT", "1433"))
GENCOURT_USER = os.getenv("GENCOURT_USER", "")
GENCOURT_PASS = os.getenv("GENCOURT_PASS", "")
GENCOURT_DB = os.getenv("GENCOURT_DB", "")

# SQLite database
DATABASE_PATH = os.getenv("DATABASE_PATH", "edopt_chatbot.db")
DATABASE_URL = f"sqlite:///{DATABASE_PATH}"

# Embeddings
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIMS = 384

# Server
PORT = int(os.getenv("PORT", "5012"))
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "https://edopt.org").split(",")

# Chat
MAX_HISTORY_TURNS = 10
RATE_LIMIT = "15/minute"
SESSION_TTL_HOURS = 24
