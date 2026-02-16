#!/usr/bin/env python3
"""
EdOpt Chatbot — FastAPI Application
"""
import logging
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from config import CORS_ORIGINS, PORT, RATE_LIMIT
from models import init_db, SessionLocal, ChatSession, ChatMessage
from chat import process_chat, get_greeting
from embeddings import load_embeddings_into_memory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="EdOpt Chatbot API",
    version="1.0.0",
    description="Chatbot for EdOpt.org — NH education options",
)

# CORS
origins = CORS_ORIGINS + ["http://localhost:5012", "http://127.0.0.1:5012"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Static files
app.mount("/static", StaticFiles(directory="static"), name="static")


# Pydantic models
class ChatRequest(BaseModel):
    message: str
    session_id: str | None = None


class ChatResponse(BaseModel):
    answer: str
    session_id: str


# Startup
@app.on_event("startup")
async def startup():
    logger.info("Initializing database...")
    init_db()
    logger.info("Loading embeddings into memory...")
    load_embeddings_into_memory()
    logger.info("EdOpt Chatbot ready on port %s", PORT)


# Endpoints
@app.get("/health")
async def health():
    return {"status": "ok", "service": "edopt-chatbot"}


@app.get("/greet", response_model=ChatResponse)
async def greet():
    sid = str(uuid.uuid4())
    greeting = await get_greeting()
    return ChatResponse(answer=greeting, session_id=sid)


@app.post("/chat", response_model=ChatResponse)
@limiter.limit(RATE_LIMIT)
async def chat(req: ChatRequest, request: Request):
    sid = req.session_id or str(uuid.uuid4())
    ip = request.client.host if request.client else None
    try:
        answer = await process_chat(sid, req.message, ip_address=ip)
        return ChatResponse(answer=answer, session_id=sid)
    except Exception as e:
        logger.error(f"Chat error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Something went wrong. Please try again.",
        )


@app.get("/widget.js")
async def widget_js():
    return FileResponse("static/widget.js", media_type="application/javascript")


@app.get("/widget.css")
async def widget_css():
    return FileResponse("static/widget.css", media_type="text/css")


@app.get("/demo", response_class=HTMLResponse)
async def demo():
    with open("templates/widget_demo.html") as f:
        return HTMLResponse(content=f.read())


@app.get("/beta", response_class=HTMLResponse)
async def beta():
    with open("templates/beta.html") as f:
        return HTMLResponse(content=f.read())


@app.get("/self-test", response_class=HTMLResponse)
async def self_test():
    with open("templates/self_test.html") as f:
        return HTMLResponse(content=f.read())


@app.get("/conversations", response_class=HTMLResponse)
async def conversations_page():
    with open("templates/conversations.html") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/conversations")
async def api_conversations():
    db = SessionLocal()
    try:
        sessions = (
            db.query(ChatSession)
            .order_by(ChatSession.last_active.desc())
            .limit(100)
            .all()
        )
        total_messages = db.query(ChatMessage).count()
        result = []
        for s in sessions:
            messages = (
                db.query(ChatMessage)
                .filter_by(session_id=s.id)
                .order_by(ChatMessage.created_at.asc())
                .all()
            )
            result.append({
                "id": s.id,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "last_active": s.last_active.isoformat() if s.last_active else None,
                "ip_address": s.ip_address,
                "messages": [
                    {
                        "role": m.role,
                        "content": m.content,
                        "created_at": m.created_at.isoformat() if m.created_at else None,
                        "tool_calls": m.tool_calls_json,
                    }
                    for m in messages
                ],
            })
        return {
            "total_sessions": len(sessions),
            "total_messages": total_messages,
            "sessions": result,
        }
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=True)
