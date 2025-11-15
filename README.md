# Distributed Real-time Collaboration Platform

**Author:** Yatharth Singh  
**Project:** Distributed Real-time Collaboration Platform (AOS course - CS G623)  

---

## Summary

The system simulates a simplified version of tools like Google Docs, where multiple clients can collaboratively edit documents while maintaining consistency across distributed nodes.


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

- **Raft Consensus** (`raft/raft.py`):
  - Leader election and replication - `RequestVote` and `AppendEntries` implemented.
  - Persistent state (`currentTerm`, `votedFor`, `log[]`) stored in `states/raft_state_{port}.json`.
  - Leader applies updates only after majority commit → **strong consistency**.


- A demo **Client** that demonstrates the full flow:
  1. Login as `alice` (password `password`)
  2. Subscribe to presence updates
  3. Get `doc1`
  4. Lock `doc1`
  5. Post an edit
  6. Ask LLM for a suggestion to fix current text
  7. Display suggestion and prompt user Y/N to accept
  8. Apply suggestion if accepted
  9. Unlock and logout
  10. Kill the current leader
  11. Show new leader election process
  12. Run client_demo.py again showing successful process again

---
Checklist

* [x] gRPC framework used for all inter-service communication
* [x] Raft consensus implemented — leader election, replication, failure detection
* [x] LLM integration on a separate node (Flan-T5)
* [x] Real-time presence & editing simulation
* [x] Simplified distributed locking mechanism
* [x] Version history with consistent replication
* [x] Persistent recovery via state files
* [x] Leader failover and continued operation after crash

---

## Requirements

Tested on Python 3.12 on Ubuntu 24.0.3 LTS. Use ```uv``` for managing packages and env.

```bash
git clone https://github.com/lefteryx/aos-distributed-collab-tool.git

cd aos-distributed-collab-tool

uv sync

python -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/collab.proto

python -m llm_server.llm_server
python -m server.app_server -- port 50051
python -m server.app_server -- port 50052
python -m server.app_server -- port 50053
python -m client.client_demo
```