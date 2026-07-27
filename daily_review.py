#!/usr/bin/env python3
"""
Daily conversation review for EdOpt Chatbot.
Pulls recent conversations, analyzes with Claude, emails improvement suggestions.

Usage: python3 daily_review.py
Cron:  0 8 * * * cd /opt/edopt-chatbot && /opt/edopt-chatbot/venv/bin/python3 daily_review.py
"""
import json
import csv
import io
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv
load_dotenv()

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
REVIEW_EMAIL_TO = [
    "chris@maidmentnh.com",
    "kevin.tyson@edopt.org",
    "jody.underwood@edopt.org",
]
REVIEW_EMAIL_FROM = "chatbot@edopt.org"
NOTES_FILE = "review_notes.json"
AWS_REGION = "us-east-1"


def get_recent_conversations(hours=24):
    """Fetch conversations since the last review (or last N hours as fallback)."""
    db = SessionLocal()
    try:
        # Use last review timestamp if available, otherwise fall back to hours
        past_notes = load_past_notes()
        last_review = None
        if past_notes.get("last_review_utc"):
            try:
                last_review = datetime.fromisoformat(past_notes["last_review_utc"])
            except (ValueError, TypeError):
                pass

        if last_review:
            cutoff = last_review
        else:
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
                        # Keep enough of the tool-calls JSON that the reviewer can see
                        # ALL tool calls + result previews — short truncation here led
                        # to a false-positive "fabricated citation" finding (Apr 16).
                        "tools": m.tool_calls_json[:4000] if m.tool_calls_json else None,
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
        conv_text += f"\n--- Conversation {i} (session {conv['session_id']}, IP: {conv['ip'] or 'unknown'}, {conv['time']}) ---\n"
        for msg in conv["messages"]:
            role = "USER" if msg["role"] == "user" else "BOT"
            conv_text += f"{role}: {msg['content']}\n"
            if msg.get("tools"):
                conv_text += f"  [Tools used: {msg['tools']}]\n"

    known_issues = "\n".join(f"- {issue}" for issue in past_notes.get("known_issues", []))

    prompt = f"""You are reviewing conversation logs from the EdOpt.org chatbot — an AI assistant that helps New Hampshire families explore education options (schools, homeschool, EFAs, charter schools, legislation).

Here are ALL conversations from the last 24 hours:

{conv_text}

Previously identified issues (avoid repeating these unless still present):
{known_issues or "None yet."}

Please analyze these conversations and provide:

1. **SUMMARY**: Brief overview — how many conversations, what topics were asked about, overall quality of responses.

2. **THEMES DISCOVERED**: Identify the main themes and topics users are asking about. For each theme, provide a one-sentence summary and the number of conversations that touched on it. This helps us understand what families care about most.

3. **ISSUES FOUND**: Specific problems you noticed:
   - Incorrect or fabricated information
   - Responses that were too long or too short
   - Questions the bot couldn't answer well
   - Missing tool usage (should have searched but didn't)
   - Context loss (bot forgetting earlier conversation details)
   - Confusing or unhelpful formatting
   - Tone issues

4. **IMPROVEMENT SUGGESTIONS**: Concrete, actionable changes to make:
   - System prompt tweaks (be specific about what to add/change)
   - New tool capabilities needed
   - Response length/format adjustments
   - Missing knowledge areas

5. **POSITIVE OBSERVATIONS**: What worked well — good responses worth noting.

6. **PRIORITY FIXES**: Top 3 most impactful changes to make, ranked.

Be specific and actionable. Reference actual conversation examples. Keep the total response under 1200 words."""

    response = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text


def _html_escape(value):
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def build_transcript_html(conversations, title="EdOpt chatbot transcripts"):
    """Render full conversation transcripts as a standalone HTML document."""
    parts = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        f"<title>{_html_escape(title)}</title>",
        "<style>",
        "body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:24px;color:#1a1a2e;}",
        "h1{font-size:20px;} h2{font-size:15px;margin:28px 0 8px;border-bottom:2px solid #1863DC;padding-bottom:4px;}",
        ".meta{color:#64748b;font-size:12px;margin-bottom:12px;}",
        ".turn{margin:10px 0;padding:10px 14px;border-radius:8px;line-height:1.5;font-size:14px;}",
        ".user{background:#1863DC;color:#fff;} .assistant{background:#f1f5f9;}",
        ".role{font-weight:700;font-size:11px;text-transform:uppercase;letter-spacing:.05em;opacity:.75;}",
        "pre{white-space:pre-wrap;word-wrap:break-word;margin:4px 0 0;font-family:inherit;}",
        "</style></head><body>",
        f"<h1>{_html_escape(title)}</h1>",
        f"<div class='meta'>{len(conversations)} conversation(s). "
        f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.</div>",
    ]
    for i, conv in enumerate(conversations, 1):
        parts.append(
            f"<h2>Conversation {i} — session {_html_escape(conv['session_id'])} "
            f"— {_html_escape(conv['time'])}</h2>"
        )
        for m in conv["messages"]:
            role = m["role"]
            cls = "user" if role == "user" else "assistant"
            parts.append(
                f"<div class='turn {cls}'><div class='role'>{_html_escape(role)}</div>"
                f"<pre>{_html_escape(m['content'])}</pre></div>"
            )
    parts.append("</body></html>")
    return "".join(parts)


def build_transcript_csv(conversations):
    """Render transcripts as CSV: one row per message."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["session_id", "session_time", "ip", "turn", "role", "content"])
    for conv in conversations:
        for turn, m in enumerate(conv["messages"], 1):
            writer.writerow([
                conv["session_id"], conv["time"], conv["ip"] or "",
                turn, m["role"], m["content"],
            ])
    return buf.getvalue()


def send_email(subject, body_text, attachments=None):
    """Send review email via AWS SES, optionally with file attachments.

    Uses send_raw_email when attachments are present so full chat transcripts
    can ride along with the analysis.
    """
    ses = boto3.client("ses", region_name=AWS_REGION)
    try:
        if not attachments:
            ses.send_email(
                Source=REVIEW_EMAIL_FROM,
                Destination={"ToAddresses": REVIEW_EMAIL_TO},
                Message={
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {
                        "Text": {"Data": body_text, "Charset": "UTF-8"},
                    },
                },
            )
        else:
            msg = MIMEMultipart()
            msg["Subject"] = subject
            msg["From"] = REVIEW_EMAIL_FROM
            msg["To"] = ", ".join(REVIEW_EMAIL_TO)
            msg.attach(MIMEText(body_text, "plain", "utf-8"))
            for filename, content in attachments:
                part = MIMEApplication(content.encode("utf-8"))
                part.add_header("Content-Disposition", "attachment", filename=filename)
                msg.attach(part)
            ses.send_raw_email(
                Source=REVIEW_EMAIL_FROM,
                Destinations=REVIEW_EMAIL_TO,
                RawMessage={"Data": msg.as_string()},
            )
        logger.info(f"Review email sent to {', '.join(REVIEW_EMAIL_TO)}")
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
    # Track when this review ran so next run picks up from here
    past_notes["last_review_utc"] = datetime.now(timezone.utc).isoformat()
    save_notes(past_notes)

    # Build email
    date_str = datetime.now().strftime("%B %d, %Y")
    total_user_msgs = sum(
        len([m for m in c["messages"] if m["role"] == "user"])
        for c in conversations
    )
    unique_ips = len(set(c["ip"] for c in conversations if c.get("ip")))

    subject = f"EdOpt Chatbot Daily Review — {date_str} ({len(conversations)} conversations, {unique_ips} unique users)"

    body = f"""EdOpt Chatbot — Daily Conversation Review
{'=' * 50}
Date: {date_str}
Conversations reviewed: {len(conversations)}
Unique users (by IP): {unique_ips}
Total user questions: {total_user_msgs}

{analysis}

---
Full transcripts of all {len(conversations)} conversation(s) in this period are attached
(transcripts-{date_str}.html for reading, transcripts-{date_str}.csv for analysis).

This is an automated daily review of the EdOpt chatbot at chatbot.edopt.org.
View all conversations: https://chatbot.edopt.org/conversations
"""

    # Attach the full transcripts alongside the analysis
    attachments = [
        (
            f"transcripts-{date_str}.html",
            build_transcript_html(
                conversations, title=f"EdOpt chatbot transcripts — {date_str}"
            ),
        ),
        (f"transcripts-{date_str}.csv", build_transcript_csv(conversations)),
    ]

    send_email(subject, body, attachments=attachments)
    logger.info("Daily review complete")


if __name__ == "__main__":
    main()
