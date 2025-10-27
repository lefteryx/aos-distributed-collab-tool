# Distributed Real-time Collaboration Platform — Milestone 1

**Author:** Yatharth Singh  
**Project:** Distributed Real-time Collaboration Platform (AOS course - CS G623)  

---

## Summary (Milestone 1)

This submission is a working **skeleton** of a distributed real-time collaboration platform with:

- A gRPC-based **App Server** (`server/app_server.py`) implementing:
  - `Login` / `Logout` (JWT-based tokens)
  - `Get` / `Post` (document fetch & update)
  - `Lock` / `Unlock` (per-document exclusive lock with TTL)
  - `SubscribePresence` (server-streaming presence updates) + `Heartbeat`
  - `AskSuggestion` / `ApplySuggestion` (LLM suggestion workflow: request suggestion, accept/reject)

- A separate **LLM Server** (`llm_server/llm_server.py`) exposing `LLMService` gRPC API that:
  - Uses an instruction-tuned text2text model by default (`google/flan-t5-small`) for grammar correction, summarization, and rewriting (CPU-friendly).
  - Sanitizes input to avoid repeating or adding timestamps/author metadata.
  - Provides deterministic generation settings to avoid repetition.

- A demo **Client** (`client/client_demo.py`) that demonstrates the full flow:
  1. Login as `alice` (password `password`)
  2. Subscribe to presence updates
  3. Get `doc1`
  4. Lock `doc1`
  5. Post an edit
  6. Ask LLM for a suggestion (rewrite/grammar/summarize)
  7. Display suggestion and prompt user Y/N to accept
  8. Apply suggestion if accepted
  9. Unlock and logout


---

## Requirements

Tested on Python 3.12 on Ubuntu 24.0.3 LTS. Use ```uv``` for managing packages and env.

```bash
git clone https://github.com/lefteryx/aos-distributed-collab-tool.git

cd aos-distributed-collab-tool

uv sync

python -m server.app_server
python -m llm_server.llm_server
python -m client.client_demo
```