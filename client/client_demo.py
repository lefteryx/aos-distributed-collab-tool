import asyncio
import grpc
import time
from datetime import datetime
import collab_pb2, collab_pb2_grpc

async def run():
    async with grpc.aio.insecure_channel("localhost:50051") as ch:
        stub = collab_pb2_grpc.ClientServiceStub(ch)

        # Login as alice
        resp = await stub.Login(collab_pb2.LoginRequest(username="alice", password="password"))
        if not resp.ok:
            print("login failed:", resp.message)
            return
        token = resp.token
        masked = token[:8] + "..." + token[-8:]
        print("Logged in.")
        print("Token:", masked)

        seen_last = {}
        # Start subscribe presence reader
        async def presence_reader():
            try:
                async for update in stub.SubscribePresence(collab_pb2.HeartbeatRequest(token=token)):
                    uname = update.username
                    last = int(update.last_seen)
                    prev = seen_last.get(uname)
                    if prev == last:
                        continue
                    seen_last[uname] = last
                    ts = datetime.fromtimestamp(last).strftime("%Y-%m-%d %H:%M:%S")
                    status = "online" if update.online else "offline"
                    print(f"\n[PRESENCE] {uname} is {status} (last_seen: {ts})")
            except grpc.aio.AioRpcError as e:
                print("presence stream closed:", e)

        pres_task = asyncio.create_task(presence_reader())

        # send a heartbeat
        await stub.Heartbeat(collab_pb2.HeartbeatRequest(token=token))

        # Try to get doc
        g = await stub.Get(collab_pb2.GetRequest(token=token, doc_id="doc1"))
        print("Current doc v", g.version, ":\n", g.content)

        # Acquire lock
        lock_resp = await stub.Lock(collab_pb2.LockRequest(token=token, doc_id="doc1"))
        if not lock_resp.ok:
            print("couldn't lock:", lock_resp.message)
        else:
            lock_token = lock_resp.lock_token
            print("Lock acquired:", lock_token)

            # Edit content (append a line)
            new_content = g.content + f"\nEdit by alice at {int(time.time())}\n"
            post_resp = await stub.Post(collab_pb2.PostRequest(token=token, doc_id="doc1", content=new_content, lock_token=lock_token, ask_llm=False))
            print("Post response:", post_resp.ok, post_resp.message, "new_version", post_resp.new_version)

            ask_resp = await stub.AskSuggestion(collab_pb2.AskSuggestionRequest(token=token, doc_id="doc1", mode="rewrite", prompt=new_content))
            if not ask_resp.ok:
                print("AskSuggestion failed:", ask_resp.message)
            else:
                sid = ask_resp.suggestion_id
                print("Suggestion created id", sid, "preview:\n", ask_resp.suggestion_preview)
                loop = asyncio.get_running_loop()
                print("Full suggestion:\n", ask_resp.suggestion_text)
                ans = await loop.run_in_executor(None, input, "Accept suggestion? (Y/N):\n")
                accept = ans.strip().lower().startswith("y")
                apply_resp = await stub.ApplySuggestion(collab_pb2.ApplySuggestionRequest(token=token, suggestion_id=sid, accept=accept))
                print("ApplySuggestion:", apply_resp.ok, apply_resp.message, "new_version", apply_resp.new_version)

            # Unlock
            await stub.Unlock(collab_pb2.UnlockRequest(token=token, doc_id="doc1", lock_token=lock_token))
            print("Unlocked")

        # wait a bit for presence updates
        await asyncio.sleep(2)

        # Logout
        await stub.Logout(collab_pb2.LogoutRequest(token=token))
        print("Logged out")

        # cancel presence task
        pres_task.cancel()
        try:
            await pres_task
        except:
            pass

if __name__ == "__main__":
    asyncio.run(run())
