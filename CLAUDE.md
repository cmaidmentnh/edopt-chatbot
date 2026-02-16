# EdOpt Chatbot

## Overview
Chatbot for EdOpt.org — helps NH families explore education options. Uses Claude API with tool-calling to search providers, look up RSA statutes, and track legislation.

## Data Sources
- **EdOpt.org WordPress API**: Providers, posts, pages, style taxonomy
- **GenCourt SQL Server** (66.211.150.69:1433): RSA statutes, legislation, sponsors, committees

## Stack
- FastAPI + uvicorn
- SQLite (edopt_chatbot.db) + SQLAlchemy
- Claude API (anthropic SDK) with tool-use
- sentence-transformers (all-MiniLM-L6-v2) for embeddings
- Embeddable JS chat widget

## Server
- **Primary VPS**: 138.197.20.97, port 5012
- **Domain**: chatbot.edopt.org
- **Service**: systemd `edopt-chatbot`
- **Deploy**: `ssh -i ~/ubuntu-key root@138.197.20.97 "cd /opt/edopt-chatbot && git pull && systemctl restart edopt-chatbot"`

## Key Commands
- **Ingest content**: `python3 ingest.py`
- **Run locally**: `uvicorn app:app --host 0.0.0.0 --port 5012 --reload`
- **Test widget**: Open http://localhost:5012/demo
