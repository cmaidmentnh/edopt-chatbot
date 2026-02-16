"""
Chat orchestration: Claude Messages API with tool-use loop.
"""
import json
import logging
import uuid
from datetime import datetime, timezone

import anthropic

from config import ANTHROPIC_API_KEY, CLAUDE_MODEL, MAX_HISTORY_TURNS
from models import SessionLocal, ChatSession, ChatMessage
from system_prompt import build_system_prompt
from tools import TOOLS, execute_tool

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def get_or_create_session(session_id: str, ip_address: str = None) -> str:
    """Get an existing session or create a new one. Returns session_id."""
    db = SessionLocal()
    try:
        session = db.get(ChatSession, session_id)
        if session:
            session.last_active = datetime.now(timezone.utc)
            db.commit()
            return session_id
        # Create new session
        new_session = ChatSession(
            id=session_id,
            ip_address=ip_address,
        )
        db.add(new_session)
        db.commit()
        return session_id
    finally:
        db.close()


def get_session_history(session_id: str, limit: int = None) -> list:
    """Get conversation history for a session."""
    if limit is None:
        limit = MAX_HISTORY_TURNS
    db = SessionLocal()
    try:
        messages = (
            db.query(ChatMessage)
            .filter_by(session_id=session_id)
            .order_by(ChatMessage.created_at.desc())
            .limit(limit * 2)  # Each turn has 2 messages (user + assistant)
            .all()
        )
        messages.reverse()  # Chronological order
        return [{"role": m.role, "content": m.content} for m in messages]
    finally:
        db.close()


def save_message(session_id: str, role: str, content: str, tool_calls: list = None):
    """Save a chat message to the database."""
    db = SessionLocal()
    try:
        msg = ChatMessage(
            session_id=session_id,
            role=role,
            content=content,
            tool_calls_json=json.dumps(tool_calls) if tool_calls else None,
        )
        db.add(msg)
        db.commit()
    finally:
        db.close()


async def process_chat(session_id: str, user_message: str, ip_address: str = None) -> str:
    """Process a chat message through Claude with tool-use loop."""
    # Ensure session exists
    get_or_create_session(session_id, ip_address)

    # Build conversation history
    history = get_session_history(session_id)
    messages = list(history)
    messages.append({"role": "user", "content": user_message})

    # Track tool calls for logging
    tool_calls_made = []

    # Call Claude with tools
    try:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            system=build_system_prompt(),
            tools=TOOLS,
            messages=messages,
        )
    except anthropic.APIError as e:
        logger.error(f"Claude API error: {e}")
        return "I'm sorry, I'm having trouble connecting right now. Please try again in a moment."

    # Tool-use loop (max 5 iterations to prevent infinite loops)
    iterations = 0
    while response.stop_reason == "tool_use" and iterations < 5:
        iterations += 1

        # Extract tool calls and execute them
        tool_results = []
        assistant_content = response.content

        for block in response.content:
            if block.type == "tool_use":
                logger.info(f"Tool call: {block.name}({json.dumps(block.input)})")
                result = execute_tool(block.name, block.input)
                tool_calls_made.append({
                    "tool": block.name,
                    "input": block.input,
                    "result_preview": result[:200] if result else "",
                })
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

        # Continue conversation with tool results
        messages.append({"role": "assistant", "content": assistant_content})
        messages.append({"role": "user", "content": tool_results})

        try:
            response = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1024,
                system=build_system_prompt(),
                tools=TOOLS,
                messages=messages,
            )
        except anthropic.APIError as e:
            logger.error(f"Claude API error in tool loop: {e}")
            return "I'm sorry, I encountered an error while looking up that information. Please try again."

    # Extract final text response
    answer = ""
    for block in response.content:
        if hasattr(block, "text"):
            answer += block.text

    if not answer:
        answer = "I'm sorry, I wasn't able to generate a response. Could you try rephrasing your question?"

    # Save messages to DB
    save_message(session_id, "user", user_message)
    save_message(session_id, "assistant", answer, tool_calls=tool_calls_made)

    return answer


async def get_greeting() -> str:
    """Return a static greeting message."""
    return (
        "Hi there! I'm the EdOpt Assistant, here to help you explore "
        "education options in New Hampshire.\n\n"
        "I can help you:\n"
        "- **Find schools and programs** near you\n"
        "- **Learn about Education Freedom Accounts** (EFAs)\n"
        "- **Understand NH education laws** and requirements\n"
        "- **Track education legislation** in the current session\n\n"
        "What can I help you with today?"
    )
