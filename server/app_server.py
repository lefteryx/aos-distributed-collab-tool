import asyncio
import time
import uuid
import jwt
import grpc
import collab_pb2, collab_pb2_grpc

JWT_SECRET = "replace_this_with_a_strong_secret"
JWT_ALGO = "HS256"
LOCK_TTL = 30  # seconds

# TODO: switch to proper db
USERS = {
    "alice": "password", 
    "bob": "password"
    }

TOKENS = {}  # token -> username  (we do issue JWT but also keep small mapping to demo logout)
DOCUMENTS = {
    "doc1": {"content": "This is the initial document.\n", "version": 1, "lock": None}
}
PRESENCE = {}  # username -> last_seen (epoch)
PRESENCE_SUBSCRIBERS = []  # list of asyncio.Queue()

# util jwt
def issue_token(username):
    now = int(time.time())
    payload = {"sub": username, "iat": now, "exp": now + 3600}
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)
    TOKENS[token] = username
    return token

def verify_token(token):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        username = payload.get("sub")
        # optional: check in TOKENS mapping for logout
        if token not in TOKENS:
            raise Exception("token not found (maybe logged out)")
        return username
    except Exception as e:
        raise

# background: expire locks
async def lock_gc_loop():
    while True:
        now = time.time()
        for doc_id, doc in list(DOCUMENTS.items()):
            lock = doc.get("lock")
            if lock and lock["expires_at"] <= now:
                print(f"[LOCK EXPIRE] {doc_id} lock expired for {lock['owner']}")
                doc["lock"] = None
                await broadcast_presence_update(lock["owner"], online=True)  # optional notify
        await asyncio.sleep(1)

# presence broadcast helper
async def broadcast_presence_update(username=None, online=True):
    update = collab_pb2.PresenceUpdate(username=username or "", online=online, last_seen=int(time.time()))
    # put to all subscriber queues (non-blocking)
    for q in list(PRESENCE_SUBSCRIBERS):
        try:
            q.put_nowait(update)
        except asyncio.QueueFull:
            # if subscriber is slow, skip now
            pass

# LLM call helper (calls separate LLM server)
async def ask_llm(query, context=""):
    async with grpc.aio.insecure_channel("localhost:50061") as ch:
        stub = collab_pb2_grpc.LLMServiceStub(ch)
        req = collab_pb2.LLMRequest(request_id=str(uuid.uuid4()), query=query, context=context)
        resp = await stub.GetLLMAnswer(req)
        return resp.answer

# Implement servicer
class ClientServicer(collab_pb2_grpc.ClientServiceServicer):
    async def Login(self, request, context):
        u = request.username
        p = request.password
        if USERS.get(u) == p:
            token = issue_token(u)
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
            user = verify_token(request.token)
        except Exception:
            return collab_pb2.GetResponse(ok=False, message="auth failed")
        doc_id = request.doc_id
        doc = DOCUMENTS.get(doc_id)
        if not doc:
            return collab_pb2.GetResponse(ok=False, message="doc not found")
        return collab_pb2.GetResponse(ok=True, content=doc["content"], version=doc["version"])

    async def Post(self, request, context):
        # apply full content (simple)
        try:
            user = verify_token(request.token)
        except Exception:
            return collab_pb2.PostResponse(ok=False, message="auth failed")
        doc_id = request.doc_id
        doc = DOCUMENTS.get(doc_id)
        if not doc:
            return collab_pb2.PostResponse(ok=False, message="doc not found")
        # if doc locked, lock_token must match
        lock = doc.get("lock")
        if lock:
            if request.lock_token != lock["token"]:
                return collab_pb2.PostResponse(ok=False, message="lock required or invalid lock_token")
            # refresh lock expiry
            lock["expires_at"] = time.time() + LOCK_TTL
        else:
            # optional: optimistic version check
            if request.base_version and request.base_version != doc["version"]:
                return collab_pb2.PostResponse(ok=False, message="version conflict", new_version=doc["version"])
        # apply
        doc["content"] = request.content
        doc["version"] += 1
        llm_suggestion = ""
        if request.ask_llm:
            # call llm (non-blocking)
            try:
                llm_suggestion = await ask_llm(query=request.content, context=doc["content"])
            except Exception as e:
                llm_suggestion = f"LLM error: {e}"
        return collab_pb2.PostResponse(ok=True, message="updated", new_version=doc["version"], llm_suggestion=llm_suggestion)

    async def Lock(self, request, context):
        try:
            user = verify_token(request.token)
        except Exception:
            return collab_pb2.LockResponse(ok=False, message="auth failed")
        doc_id = request.doc_id
        doc = DOCUMENTS.get(doc_id)
        if not doc:
            return collab_pb2.LockResponse(ok=False, message="doc not found")
        lock = doc.get("lock")
        now = time.time()
        if lock and lock["expires_at"] > now:
            return collab_pb2.LockResponse(ok=False, message=f"doc locked by {lock['owner']}")
        token = str(uuid.uuid4())
        doc["lock"] = {"token": token, "owner": user, "expires_at": now + LOCK_TTL}
        await broadcast_presence_update(username=user, online=True)
        return collab_pb2.LockResponse(ok=True, lock_token=token, expires_at=int(now + LOCK_TTL), message="lock acquired")

    async def Unlock(self, request, context):
        try:
            user = verify_token(request.token)
        except Exception:
            return collab_pb2.Status(ok=False, message="auth failed")
        doc_id = request.doc_id
        doc = DOCUMENTS.get(doc_id)
        if not doc:
            return collab_pb2.Status(ok=False, message="doc not found")
        lock = doc.get("lock")
        if not lock or lock["token"] != request.lock_token:
            return collab_pb2.Status(ok=False, message="invalid lock token")
        if lock["owner"] != user:
            return collab_pb2.Status(ok=False, message="not lock owner")
        doc["lock"] = None
        await broadcast_presence_update(username=user, online=True)
        return collab_pb2.Status(ok=True, message="unlocked")

    async def SubscribePresence(self, request, context):
        # verify token
        try:
            user = verify_token(request.token)
        except Exception:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid token")
        q = asyncio.Queue(maxsize=20)
        PRESENCE_SUBSCRIBERS.append(q)
        print(f"[SubscribePresence] new subscriber for {user}")
        try:
            # immediately send current presence snapshot (as separate messages)
            for u, last in list(PRESENCE.items()):
                yield collab_pb2.PresenceUpdate(username=u, online=True, last_seen=int(last))
            # keep streaming updates
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
            print(f"[SubscribePresence] subscriber disconnected for {user}")

    async def Heartbeat(self, request, context):
        try:
            user = verify_token(request.token)
        except Exception:
            return collab_pb2.Status(ok=False, message="auth failed")
        PRESENCE[user] = int(time.time())
        await broadcast_presence_update(username=user, online=True)
        return collab_pb2.Status(ok=True, message="heartbeat ok")


async def serve():
    server = grpc.aio.server()
    collab_pb2_grpc.add_ClientServiceServicer_to_server(ClientServicer(), server)
    server.add_insecure_port("[::]:50051")
    print("App server listening at 0.0.0.0:50051")
    # start lock gc task
    asyncio.create_task(lock_gc_loop())
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())

