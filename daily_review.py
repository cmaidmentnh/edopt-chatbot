#!/usr/bin/env python3
"""
Daily conversation review for EdOpt Chatbot.
Pulls recent conversations, analyzes with Claude, emails improvement suggestions.

Usage: python3 daily_review.py
Cron:  0 8 * * * cd /opt/edopt-chatbot && /opt/edopt-chatbot/venv/bin/python3 daily_review.py
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta

import anthropic
import boto3
from botocore.exceptions import ClientError

from config import ANTHROPIC_API_KEY, DATABASE_PATH
from models import SessionLocal, ChatSession, ChatMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("daily_review")

# Config
REVIEW_EMAIL_TO = "chris@maidmentnh.com"
REVIEW_EMAIL_FROM = "chatbot@edopt.org"
NOTES_FILE = "review_notes.json"
AWS_REGION = "us-east-1"


def get_recent_conversations(hours=24):
    """Fetch conversations from the last N hours."""
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        sessions = (
            db.query(ChatSession)
            .filter(ChatSession.last_active >= cutoff)
            .order_by(ChatSession.last_active.desc())
            .all()
        )

        conversations = []
        for s in sessions:
            messages = (
                db.query(ChatMessage)
                .filter_by(session_id=s.id)
                .order_by(ChatMessage.created_at.asc())
                .all()
            )
            if not messages:
                continue

            user_msgs = [m for m in messages if m.role == "user"]
            if not user_msgs:
                continue

            conversations.append({
                "session_id": s.id[:8],
                "ip": s.ip_address,
                "time": s.created_at.isoformat() if s.created_at else "unknown",
                "messages": [
                    {
                        "role": m.role,
                        "content": m.content[:2000],
                        "tools": m.tool_calls_json[:500] if m.tool_calls_json else None,
                    }
                    for m in messages
                ],
            })

        return conversations
    finally:
        db.close()


def load_past_notes():
    """Load previously saved review notes."""
    if os.path.exists(NOTES_FILE):
        with open(NOTES_FILE) as f:
            return json.load(f)
    return {"reviews": [], "known_issues": []}


def save_notes(notes):
    """Save review notes."""
    with open(NOTES_FILE, "w") as f:
        json.dump(notes, f, indent=2)


def analyze_conversations(conversations, past_notes):
    """Use Claude to analyze conversations and suggest improvements."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    conv_text = ""
    for i, conv in enumerate(conversations, 1):
        conv_text += f"\n--- Conversation {i} (session {conv['session_id']}, {conv['time']}) ---\n"
        for msg in conv["messages"]:
            role = "USER" if msg["role"] == "user" else "BOT"
            conv_text += f"{role}: {msg['content']}\n"
            if msg.get("tools"):
                conv_text += f"  [Tools used: {msg['tools']}]\n"

    known_issues = "\n".join(f"- {issue}" for issue in past_notes.get("known_issues", []))

    prompt = f"""You are reviewing conversation logs from the EdOpt.org chatbot — an AI assistant that helps New Hampshire families explore education options (schools, homeschool, EFAs, charter schools, legislation).

Here are the conversations from the last 24 hours:

{conv_text}

Previously identified issues (avoid repeating these unless still present):
{known_issues or "None yet."}

Please analyze these conversations and provide:

1. **SUMMARY**: Brief overview — how many conversations, what topics were asked about, overall quality of responses.

2. **ISSUES FOUND**: Specific problems you noticed:
   - Incorrect or fabricated information
   - Responses that were too long or too short
   - Questions the bot couldn't answer well
   - Missing tool usage (should have searched but didn't)
   - Confusing or unhelpful formatting
   - Tone issues

3. **IMPROVEMENT SUGGESTIONS**: Concrete, actionable changes to make:
   - System prompt tweaks (be specific about what to add/change)
   - New tool capabilities needed
   - Response length/format adjustments
   - Missing knowledge areas

4. **POSITIVE OBSERVATIONS**: What worked well — good responses worth noting.

5. **PRIORITY FIXES**: Top 3 most impactful changes to make, ranked.

Be specific and actionable. Reference actual conversation examples. Keep the total response under 1000 words."""

    response = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text


def send_email(subject, body_text):
    """Send review email via AWS SES."""
    ses = boto3.client("ses", region_name=AWS_REGION)
    try:
        ses.send_email(
            Source=REVIEW_EMAIL_FROM,
            Destination={"ToAddresses": [REVIEW_EMAIL_TO]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": body_text, "Charset": "UTF-8"},
                },
            },
        )
        logger.info(f"Review email sent to {REVIEW_EMAIL_TO}")
    except ClientError as e:
        logger.error(f"SES send failed: {e}")
        raise


def main():
    logger.info("Starting daily conversation review...")

    # Get recent conversations
    conversations = get_recent_conversations(hours=24)
    logger.info(f"Found {len(conversations)} conversations in the last 24 hours")

    if not conversations:
        logger.info("No conversations to review. Skipping.")
        return

    # Load past notes
    past_notes = load_past_notes()

    # Analyze with Claude
    logger.info("Analyzing conversations with Claude...")
    analysis = analyze_conversations(conversations, past_notes)
    logger.info("Analysis complete")

    # Update notes
    past_notes["reviews"].append({
        "date": datetime.now(timezone.utc).isoformat(),
        "conversation_count": len(conversations),
        "analysis_preview": analysis[:300],
    })
    # Keep last 30 reviews
    past_notes["reviews"] = past_notes["reviews"][-30:]
    save_notes(past_notes)

    # Build email
    date_str = datetime.now().strftime("%B %d, %Y")
    total_user_msgs = sum(
        len([m for m in c["messages"] if m["role"] == "user"])
        for c in conversations
    )

    subject = f"EdOpt Chatbot Daily Review — {date_str} ({len(conversations)} conversations)"

    body = f"""EdOpt Chatbot — Daily Conversation Review
{'=' * 50}
Date: {date_str}
Conversations reviewed: {len(conversations)}
Total user questions: {total_user_msgs}

{analysis}

---
This is an automated daily review of the EdOpt chatbot at chatbot.edopt.org.
View all conversations: https://chatbot.edopt.org/conversations
"""

    # Send email
    send_email(subject, body)
    logger.info("Daily review complete")


if __name__ == "__main__":
    main()
