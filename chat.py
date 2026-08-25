"""
Chat orchestration: Claude Messages API with tool-use loop.
"""
import json
import logging
import re
import uuid
from datetime import datetime, timezone

import anthropic

from config import ANTHROPIC_API_KEY, CLAUDE_MODEL, MAX_HISTORY_TURNS, MAX_TOKENS
from models import SessionLocal, ChatSession, ChatMessage
from system_prompt import build_system_prompt
from tools import TOOLS, execute_tool

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# A trailing question sentence stuck on the end of a longer paragraph.
_TRAILING_QUESTION = re.compile(r"(?<=[.!?])\s+([^.!?]{3,200}\?)$")

# Sentence-ending punctuation followed by whitespace or end-of-text. The optional
# closers let a sentence end inside quotes or parentheses; requiring whitespace
# after keeps "8.5 miles" and "starhop.com/" from looking like sentence ends.
_SENTENCE_END = re.compile(r"[.!?][\"')\]]*(?=\s|$)")

_CONTINUE_PROMPT = "**Would you like me to keep going with the rest?**"


def repair_truncated_answer(answer: str) -> str:
    """Cut a reply that hit the token cap back to its last complete thought.

    Hitting max_tokens ships whatever half-written sentence the model was on,
    which reads to a parent as a broken bot. Ending one sentence early and
    offering to continue is the honest version of the same reply.
    """
    text = answer.rstrip()
    if not text:
        return answer

    # Everything before the last newline is a complete line; within the final
    # line, the last sentence end is the safe cut. Take whichever reaches further
    # so a long final paragraph keeps its finished sentences.
    matches = list(_SENTENCE_END.finditer(text))
    cut = max(
        matches[-1].end() if matches else 0,
        text.rfind("\n") + 1,
    )
    text = text[:cut].rstrip() if cut > 0 else ""

    if not text:
        # The whole reply was one unfinished sentence, so there is nothing to keep.
        # Showing the fragment is worse than asking for a narrower question.
        return (
            "Sorry, that answer ran longer than I could fit in one reply.\n\n"
            "**Could you narrow it down a little, such as a town, a grade level, "
            "or a subject?**"
        )

    return f"{text}\n\n{_CONTINUE_PROMPT}"


def emphasize_closing_question(answer: str) -> str:
    """Give the closing question its own bold paragraph.

    The greeting hardcodes a bold closing question, so it stood out on the first
    screen and nowhere else: later ones came back unbolded and often glued to the
    end of the previous sentence, so users stopped noticing them and
    conversations stalled. The system prompt asks for this, but asking is not
    reliable enough on its own, so enforce it here too.
    """
    if not answer:
        return answer

    text = answer.rstrip()
    # rstrip the emphasis markers first, or an already-bold question fails the check.
    if not text.rstrip("*").endswith("?"):
        return answer

    lines = text.split("\n")
    last = lines[-1].strip()

    # Lists, headings and tables get left alone; splitting those mangles content.
    # The bullet markers need the trailing space or "**bold**" looks like a list.
    if not last or last.startswith(("- ", "* ", "#", ">", "|")):
        return answer

    if last.startswith("**") and last.endswith("**"):
        # Already bold, so only the blank line above it may be missing.
        question, body = last, lines[:-1]
    else:
        match = _TRAILING_QUESTION.search(last)
        if match:
            question = f"**{match.group(1).strip()}**"
            lines[-1] = last[: match.start()].rstrip()
            body = lines
        elif len(last) <= 200:
            question = f"**{last}**"
            body = lines[:-1]
        else:
            # Too long to be a closing question; leave it as written.
            return answer

    while body and not body[-1].strip():
        body.pop()

    return "\n".join(body + ["", question]) if body else question


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
            max_tokens=MAX_TOKENS,
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
                max_tokens=MAX_TOKENS,
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
    elif response.stop_reason == "max_tokens":
        logger.warning(
            f"Response hit the {MAX_TOKENS}-token cap (session {session_id}); "
            "trimming to the last complete sentence"
        )
        answer = repair_truncated_answer(answer)

    answer = emphasize_closing_question(answer)

    # Save messages to DB
    save_message(session_id, "user", user_message)
    save_message(session_id, "assistant", answer, tool_calls=tool_calls_made)

    return answer


async def get_greeting() -> str:
    """Return a static greeting message."""
    return (
        "Hi there! I'm the EdOpt Navigator, here to help you explore "
        "education options in New Hampshire.\n\n"
        "I can help you:\n"
        "- **Find schools and programs** near you\n"
        "- **Learn about Education Freedom Accounts** (EFAs)\n"
        "- **Understand NH education laws** and requirements\n\n"
        "You can also take our [Education Options Self-Test](https://chatbot.edopt.org/self-test) "
        "to find your pathway and get personalized recommendations.\n\n"
        "Prefer to talk with a real person? "
        "[Schedule a free consultation](https://edopt.org/schedule-a-meeting/) "
        "with an EdOpt volunteer.\n\n"
        "**What can I help you with today?**"
    )
