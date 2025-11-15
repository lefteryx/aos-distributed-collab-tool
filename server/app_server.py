import asyncio
import time
import uuid
import jwt
import grpc
import json
import os
import argparse
import logging
import collab_pb2, collab_pb2_grpc

from raft.raft import RaftNode

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app_server")

JWT_SECRET = open("SECRET.txt", "r").read().strip()
JWT_ALGO = "HS256"
LOCK_TTL = 30  # seconds

USERS = {"alice": "password", "bob": "password"}
TOKENS = {}
PRESENCE = {}
PRESENCE_SUBSCRIBERS = []

SUGGESTIONS = {}
SUGGESTION_COUNTER = 1
SUGGESTION_LOCK = asyncio.Lock()

STATES_DIR = "states"
os.makedirs(STATES_DIR, exist_ok=True)
STATE_FILE_TEMPLATE = os.path.join(STATES_DIR, "state_{}.json")
DEFAULT_CLUSTER_FILE = "cluster_config.json"

def load_state(port):
    fname = STATE_FILE_TEMPLATE.format(port)
    default_docs = {"doc1": {"content": "This are the initial document.\n", "version": 1, "lock": None}}
    if not os.path.exists(fname):
        log.info(f"No state file {fname}; using default document")
        return default_docs
    try:
        with open(fname, "r") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            log.warning(f"State file {fname} malformed (not a dict); using default")
            return default_docs
        docs = data.get("documents")
        if not docs or not isinstance(docs, dict) or len(docs) == 0:
            log.info(f"State file {fname} contains no documents; using default")
            return default_docs
        # basic integrity check
        for k, v in list(docs.items()):
            if not isinstance(v, dict) or "content" not in v or "version" not in v:
                log.warning(f"Document {k} in {fname} looks malformed; dropping it")
                docs.pop(k, None)
        if len(docs) == 0:
            return default_docs
        return docs
    except Exception as e:
        log.warning(f"Failed to load state file {fname}: {e}; using default")
        return default_docs

def save_state(port, documents):
    fname = STATE_FILE_TEMPLATE.format(port)
    try:
        with open(fname, "w") as f:
            json.dump({"documents": documents}, f, indent=2)
    except Exception as e:
        log.warning(f"Failed to save state to {fname}: {e}")

async def ask_llm(query, mode="rewrite"):
    async with grpc.aio.insecure_channel("localhost:50061") as ch:
        stub = collab_pb2_grpc.LLMServiceStub(ch)
        req = collab_pb2.LLMRequest(request_id=str(uuid.uuid4()), query=query, context="", mode=mode)
        resp = await stub.GetLLMAnswer(req)
        return resp.answer

RAFT_NODE: RaftNode = None
SERVER_PORT = None
DOCUMENTS = {}

# Presence helpers
APPLIED_INDEXES = set()
async def broadcast_presence_update(username=None, online=True):
    update = collab_pb2.PresenceUpdate(username=username or "", online=online, last_seen=int(time.time()))
    for q in list(PRESENCE_SUBSCRIBERS):
        try:
            q.put_nowait(update)
        except asyncio.QueueFull:
            pass

# Apply a committed log entry to the state machine (documents/suggestions)
async def apply_committed_entry(entry):
    global SUGGESTIONS, SUGGESTION_COUNTER, DOCUMENTS, APPLIED_INDEXES
    op = entry["op"]
    payload = json.loads(entry["payload"])
    idx = entry.get("index")
    if op == "post":
        doc_id = payload["doc_id"]
        content = payload["content"]
        doc = DOCUMENTS.get(doc_id)
        if not doc:
            DOCUMENTS[doc_id] = {"content": content, "version": 1, "lock": None}
        else:
            DOCUMENTS[doc_id]["content"] = content
            DOCUMENTS[doc_id]["version"] = DOCUMENTS[doc_id].get("version", 0) + 1
        save_state(SERVER_PORT, DOCUMENTS)
        log.info(f"[APPLY] post applied for {doc_id} -> v{DOCUMENTS[doc_id]['version']}")
    elif op == "create_suggestion":
        sid = idx  # deterministic id: raft log index
        SUGGESTIONS[sid] = {
            "id": sid,
            "doc_id": payload["doc_id"],
            "mode": payload.get("mode"),
            "prompt": payload.get("prompt", ""),
            "suggestion_text": payload.get("suggestion_text"),
            "base_version": payload.get("base_version", 0),
            "author": payload.get("author"),
            "created_at": int(time.time()),
            "status": "pending"
        }
        # keep SUGGESTION_COUNTER non-decreasing for compatibility
        async with SUGGESTION_LOCK:
            try:
                if sid >= SUGGESTION_COUNTER:
                    SUGGESTION_COUNTER = sid + 1
            except Exception:
                pass
        log.info(f"[APPLY] create_suggestion {sid} for doc {payload.get('doc_id')}")
    elif op == "apply_suggestion":
        sid = payload["suggestion_id"]
        suggestion = SUGGESTIONS.get(sid)
        if not suggestion:
            log.warning(f"[APPLY] apply_suggestion: suggestion {sid} not found")
            # still mark the entry as applied to avoid infinite retries
            APPLIED_INDEXES.add(idx)
            return
        if payload.get("accept"):
            doc_id = suggestion["doc_id"]
            doc = DOCUMENTS.get(doc_id)
            if suggestion["base_version"] != doc.get("version", 0):
                suggestion["status"] = "stale"
                log.info(f"[APPLY] suggestion {sid} stale (base {suggestion['base_version']} vs current {doc.get('version')})")
                APPLIED_INDEXES.add(idx)
                return
            DOCUMENTS[doc_id]["content"] = suggestion["suggestion_text"]
            DOCUMENTS[doc_id]["version"] = DOCUMENTS[doc_id].get("version", 0) + 1
            suggestion["status"] = "accepted"
            suggestion["applied_by"] = payload.get("author")
            suggestion["applied_at"] = int(time.time())
            save_state(SERVER_PORT, DOCUMENTS)
            log.info(f"[APPLY] suggestion {sid} applied -> doc {doc_id} v{DOCUMENTS[doc_id]['version']}")
        else:
            suggestion["status"] = "rejected"
            suggestion["rejected_by"] = payload.get("author")
            suggestion["rejected_at"] = int(time.time())
            log.info(f"[APPLY] suggestion {sid} rejected by {payload.get('author')}")
    else:
        log.warning(f"[APPLY] unknown op {op}")

    # Mark this index as applied for this node
    if idx is not None:
        APPLIED_INDEXES.add(idx)


# gRPC servicers
class RaftServicer(collab_pb2_grpc.RaftServicer):
    async def RequestVote(self, request, context):
        resp = await RAFT_NODE.handle_RequestVote(request)
        return resp

    async def AppendEntries(self, request, context):
        resp = await RAFT_NODE.handle_AppendEntries(request)
        return resp

class ClientServicer(collab_pb2_grpc.ClientServiceServicer):
    async def Login(self, request, context):
        u = request.username
        p = request.password
        if USERS.get(u) == p:
            token = jwt.encode({"sub": u, "iat": int(time.time()), "exp": int(time.time()) + 3600, "jti": str(uuid.uuid4())}, JWT_SECRET, algorithm=JWT_ALGO)
            TOKENS[token] = u
            PRESENCE[u] = int(time.time())
            await broadcast_presence_update(username=u, online=True)
            return collab_pb2.LoginResponse(ok=True, token=token, message="login ok")
        return collab_pb2.LoginResponse(ok=False, message="invalid credentials")

    async def Logout(self, request, context):
        token = request.token
        if token in TOKENS:
            user = TOKENS.pop(token)
            PRESENCE.pop(user, None)
            await broadcast_presence_update(username=user, online=False)
            return collab_pb2.Status(ok=True, message="logged out")
        return collab_pb2.Status(ok=False, message="invalid token")

    async def Health(self, request, context):
        return collab_pb2.Status(ok=True, message="ok")

    async def Get(self, request, context):
        try:
            user = jwt.decode(request.token, JWT_SECRET, algorithms=[JWT_ALGO])["sub"]
        except Exception:
            return collab_pb2.GetResponse(ok=False, message="auth failed")
        doc_id = request.doc_id
        doc = DOCUMENTS.get(doc_id)
        if not doc:
            return collab_pb2.GetResponse(ok=False, message="doc not found")
        return collab_pb2.GetResponse(ok=True, content=doc["content"], version=doc["version"])

    async def Post(self, request, context):
        # Only leader handles writes
        if RAFT_NODE.role != "leader":
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            # we don't always know the leader address here; use unknown if not known
            context.set_details(f"redirect:unknown")
            return collab_pb2.PostResponse(ok=False, message="redirect")

        try:
            user = jwt.decode(request.token, JWT_SECRET, algorithms=[JWT_ALGO])["sub"]
        except Exception:
            return collab_pb2.PostResponse(ok=False, message="auth failed")

        doc_id = request.doc_id
        payload = {"doc_id": doc_id, "content": request.content, "author": user}
        payload_json = json.dumps(payload)

        # Append to Raft log and wait for commit (submit_command raises on timeout/failure)
        try:
            idx = await RAFT_NODE.submit_command(op="post", payload=payload_json)
        except Exception as e:
            return collab_pb2.PostResponse(ok=False, message=f"replication failed: {e}")

        log.info(f"[POST] submit idx={idx} commitIndex={RAFT_NODE.commitIndex} lastLogIndex={RAFT_NODE.last_log_index()} applied={sorted(APPLIED_INDEXES)}")

        # Determine committed entries not applied on this node
        # (apply all entries with index <= commitIndex that are not in APPLIED_INDEXES)
        entries_to_apply = [e for e in RAFT_NODE.log if e["index"] <= RAFT_NODE.commitIndex and e["index"] not in APPLIED_INDEXES]
        entries_to_apply = sorted(entries_to_apply, key=lambda x: x["index"])
        log.info(f"[POST] need_apply_indices={[e['index'] for e in entries_to_apply]}")

        # Apply them in order
        for e in entries_to_apply:
            try:
                await apply_committed_entry(e)
            except Exception as ex:
                log.exception(f"Error applying committed entry {e.get('index')}: {ex}")
            # apply_committed_entry will add index to APPLIED_INDEXES, but ensure idempotency:
            if e.get("index") is not None:
                APPLIED_INDEXES.add(e["index"])

        # After applying, return the latest document version
        new_ver = DOCUMENTS.get(doc_id, {}).get("version", 0)
        log.info(f"[POST] replied new_version={new_ver} for doc {doc_id}")
        return collab_pb2.PostResponse(ok=True, message="updated (committed)", new_version=new_ver)


    async def Lock(self, request, context):
        # Lock must be a replicated operation too (to avoid split brain), but for simplicity we allow leader-only ephemeral locks
        if RAFT_NODE.role != "leader":
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"redirect:unknown")
            return collab_pb2.LockResponse(ok=False, message="redirect")
        try:
            user = jwt.decode(request.token, JWT_SECRET, algorithms=[JWT_ALGO])["sub"]
        except Exception:
            return collab_pb2.LockResponse(ok=False, message="auth failed")
        doc = DOCUMENTS.get(request.doc_id)
        now = time.time()
        if not doc:
            return collab_pb2.LockResponse(ok=False, message="doc not found")
        lock = doc.get("lock")
        if lock and lock["expires_at"] > now:
            return collab_pb2.LockResponse(ok=False, message=f"doc locked by {lock['owner']}")
        token = str(uuid.uuid4())
        doc["lock"] = {"token": token, "owner": user, "expires_at": now + LOCK_TTL}
        save_state(SERVER_PORT, DOCUMENTS)
        await broadcast_presence_update(username=user, online=True)
        return collab_pb2.LockResponse(ok=True, lock_token=token, expires_at=int(now + LOCK_TTL), message="lock acquired")

    async def Unlock(self, request, context):
        if RAFT_NODE.role != "leader":
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"redirect:unknown")
            return collab_pb2.Status(ok=False, message="redirect")
        try:
            user = jwt.decode(request.token, JWT_SECRET, algorithms=[JWT_ALGO])["sub"]
        except Exception:
            return collab_pb2.Status(ok=False, message="auth failed")
        doc = DOCUMENTS.get(request.doc_id)
        if not doc:
            return collab_pb2.Status(ok=False, message="doc not found")
        lock = doc.get("lock")
        if not lock or lock["token"] != request.lock_token:
            return collab_pb2.Status(ok=False, message="invalid lock token")
        if lock["owner"] != user:
            return collab_pb2.Status(ok=False, message="not lock owner")
        doc["lock"] = None
        save_state(SERVER_PORT, DOCUMENTS)
        await broadcast_presence_update(username=user, online=True)
        return collab_pb2.Status(ok=True, message="unlocked")

    async def SubscribePresence(self, request, context):
        try:
            user = jwt.decode(request.token, JWT_SECRET, algorithms=[JWT_ALGO])["sub"]
        except Exception:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid token")
        q = asyncio.Queue(maxsize=20)
        PRESENCE_SUBSCRIBERS.append(q)
        log.info(f"[SubscribePresence] new subscriber for {user}")
        try:
            for u, last in list(PRESENCE.items()):
                yield collab_pb2.PresenceUpdate(username=u, online=True, last_seen=int(last))
            while True:
                update = await q.get()
                yield update
        except asyncio.CancelledError:
            pass
        finally:
            try:
                PRESENCE_SUBSCRIBERS.remove(q)
            except ValueError:
                pass
            log.info(f"[SubscribePresence] subscriber disconnected for {user}")

    async def Heartbeat(self, request, context):
        try:
            user = jwt.decode(request.token, JWT_SECRET, algorithms=[JWT_ALGO])["sub"]
        except Exception:
            return collab_pb2.Status(ok=False, message="auth failed")
        PRESENCE[user] = int(time.time())
        await broadcast_presence_update(username=user, online=True)
        return collab_pb2.Status(ok=True, message="heartbeat ok")

    async def AskSuggestion(self, request, context):
        # Only leader handles LLM generation and suggestion creation as a replicated op
        if RAFT_NODE.role != "leader":
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"redirect:unknown")
            return collab_pb2.AskSuggestionResponse(ok=False, message="redirect", suggestion_id=0)
        try:
            user = jwt.decode(request.token, JWT_SECRET, algorithms=[JWT_ALGO])["sub"]
        except Exception:
            return collab_pb2.AskSuggestionResponse(ok=False, message="auth failed", suggestion_id=0)
        # call LLM synchronously (leader must do this before replicating suggestion)
        try:
            suggestion_text = await asyncio.wait_for(ask_llm(query=request.prompt, mode=request.mode or "rewrite"), timeout=15.0)
        except Exception as e:
            return collab_pb2.AskSuggestionResponse(ok=False, message=f"LLM error: {e}", suggestion_id=0)
        # prepare payload
        base_version = DOCUMENTS.get(request.doc_id, {}).get("version", 0)
        # pack suggestion into raft log so it's replicated
        payload = {
            "doc_id": request.doc_id,
            "suggestion_text": suggestion_text,
            "mode": request.mode,
            "prompt": request.prompt,
            "author": user,
            "base_version": base_version
        }
        payload_json = json.dumps(payload)
        try:
            idx = await RAFT_NODE.submit_command(op="create_suggestion", payload=payload_json)
        except Exception as e:
            return collab_pb2.AskSuggestionResponse(ok=False, message=f"replication failed: {e}", suggestion_id=0)
        # apply committed entries (apply entries up to commitIndex)
        entries_to_apply = [e for e in RAFT_NODE.log if e["index"] <= RAFT_NODE.commitIndex and e["index"] not in APPLIED_INDEXES]
        for e in sorted(entries_to_apply, key=lambda x: x["index"]):
            await apply_committed_entry(e)

        # Now the suggestion should be present with id == idx
        sid = idx
        suggestion = SUGGESTIONS.get(sid)
        if not suggestion:
            # Defensive fallback: wait briefly for apply loop (shouldn't usually be needed)
            await asyncio.sleep(0.05)
            suggestion = SUGGESTIONS.get(sid)
            if not suggestion:
                return collab_pb2.AskSuggestionResponse(ok=False, message="suggestion not applied yet", suggestion_id=0)
        preview = suggestion["suggestion_text"][:200].replace("\n", " ")
        return collab_pb2.AskSuggestionResponse(ok=True, suggestion_id=sid, message="suggestion created", suggestion_preview=preview, suggestion_text=suggestion["suggestion_text"])
    
    async def ApplySuggestion(self, request, context):
        if RAFT_NODE.role != "leader":
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"redirect:unknown")
            return collab_pb2.ApplySuggestionResponse(ok=False, message="redirect", new_version=0)
        try:
            user = jwt.decode(request.token, JWT_SECRET, algorithms=[JWT_ALGO])["sub"]
        except Exception:
            return collab_pb2.ApplySuggestionResponse(ok=False, message="auth failed", new_version=0)
        # prepare payload
        payload = {"suggestion_id": request.suggestion_id, "accept": request.accept, "author": user}
        payload_json = json.dumps(payload)
        try:
            idx = await RAFT_NODE.submit_command(op="apply_suggestion", payload=payload_json)
        except Exception as e:
            return collab_pb2.ApplySuggestionResponse(ok=False, message=f"replication failed: {e}", new_version=0)
        # apply committed entries
        entries_to_apply = [e for e in RAFT_NODE.log if e["index"] <= RAFT_NODE.commitIndex and e["index"] not in APPLIED_INDEXES]
        for e in sorted(entries_to_apply, key=lambda x: x["index"]):
            await apply_committed_entry(e)

        # return new_version for the doc
        suggestion = SUGGESTIONS.get(request.suggestion_id)
        if suggestion:
            docv = DOCUMENTS.get(suggestion["doc_id"], {}).get("version", 0)
        else:
            docv = 0
        return collab_pb2.ApplySuggestionResponse(ok=True, message="applied", new_version=docv)

async def periodic_apply_committed():
    while True:
        try:
            entries = [e for e in RAFT_NODE.log if e["index"] <= RAFT_NODE.commitIndex and e["index"] not in APPLIED_INDEXES]
            if entries:
                for e in sorted(entries, key=lambda x: x["index"]):
                    await apply_committed_entry(e)
            await asyncio.sleep(0.2)
        except Exception as e:
            log.warning(f"periodic_apply error: {e}")
            await asyncio.sleep(1.0)


async def serve(port: int, peers: list):
    global RAFT_NODE, SERVER_PORT, DOCUMENTS
    SERVER_PORT = port
    DOCUMENTS = load_state(port)
    node_addr = f"localhost:{port}"
    RAFT_NODE = RaftNode(node_addr=node_addr, peers=peers, port=port)
    await RAFT_NODE.start()
    asyncio.create_task(periodic_apply_committed())

    server = grpc.aio.server()
    collab_pb2_grpc.add_ClientServiceServicer_to_server(ClientServicer(), server)
    collab_pb2_grpc.add_RaftServicer_to_server(RaftServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    log.info(f"App server listening at 0.0.0.0:{port}")

    await server.start()
    await server.wait_for_termination()

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--peers", type=str, default=None, help="comma-separated peers, e.g. localhost:50052,localhost:50053")
    ap.add_argument("--cluster", type=str, default=DEFAULT_CLUSTER_FILE, help="path to cluster_config.json listing peers")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    peers = []
    if args.cluster:
        try:
            with open(args.cluster, "r") as f:
                cfg = json.load(f)
            peers = [p for p in cfg.get("peers", []) if p != f"localhost:{args.port}"]
        except Exception as e:
            log.warning(f"Failed to read cluster config {args.cluster}: {e}")
    elif args.peers:
        peers = [p.strip() for p in args.peers.split(",") if p.strip() and p.strip() != f"localhost:{args.port}"]

    try:
        asyncio.run(serve(args.port, peers))
    except KeyboardInterrupt:
        log.info("Shutting down server")
