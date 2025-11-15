## Reflections

### What I learned
- Implementing Raft end-to-end (leader election, RequestVote, AppendEntries, log replication) revealed many subtle concurrency and timing issues. Simple things like election timers, heartbeat resets, and the boundary handling of committed/applied indices sounded easy but I would often run into race conditions and bugs when implementing them.
- Designing the actual business logic would run on top of Raft (documents, suggestions, locks) helped me learn how transitions need to be deterministic and idempotent. Eg. using the Raft log index.
- Utilising tiny domain specific language models for very specific use cases. I ended up using a very specific Encoder-Decoder Transformer for my usecase rather than more common GPT-based SLMs for low response times.
- Practical debugging experience: adding targeted debug logs, scanning server vs client logs, et cetera to avoid off-by-one errors and race conditions.
- Reading other people's code -- found existing Python codebases implementing Raft through https://raft.github.io (like https://github.com/adsharma/raft) which helped me on how to get started with implementing Raft for my usecase.

### Key Design Choices
- **Use log-index as suggestion ID**: for deterministic, globally unique IDs across nodes removing race conditions between log-commit and suggestion creation.
- **APPLIED_INDEXES set (per-node)**: prevented re-applying or skipping entries when the background apply loop and synchronous request handlers get interleaved.

### Challenges faced
- **Leader apply vs follower apply race**: followers sometimes applied entries earlier than the leader replied to the client. Addressed by applying committed-but-not-yet-applied entries directly on the leader before responding, and by tracking `APPLIED_INDEXES`.
- **LLM repetition**: initial generation often repeated lines. Solved by switching to a better prompt format and using the seq2seq T5 model with deterministic generation settings. For more robust behavior a conversational or QA model would be used in production.

### How I would extend this project given more time
1. Replace JSON persistence with SQLite, MongoDB or PostgreSQL.
2. Implement Raft snapshotting and log compaction to bound storage and speed recovery for more real-life-like situations demanding scalability.
3. Do more extensive bug and performance testing.
4. Improve suggestion and accept/reject UI flow for the LLM.
5. Check feasibility of more real-time and character-level collaborative editing similar to Google Docs.
6. Improve concurrency switching away from simple locking mechanism.