import asyncio
import grpc
import time
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
        print("Logged in. token:", token[:10], "...")

        # Start subscribe presence reader
        async def presence_reader():
            try:
                async for update in stub.SubscribePresence(collab_pb2.HeartbeatRequest(token=token)):
                    print("[PRESENCE UPDATE]", update.username, "online=", update.online, "last_seen=", update.last_seen)
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
            post_resp = await stub.Post(collab_pb2.PostRequest(token=token, doc_id="doc1", content=new_content, lock_token=lock_token, ask_llm=True))
            print("Post response:", post_resp.ok, post_resp.message, "new_version", post_resp.new_version)
            if post_resp.llm_suggestion:
                print("LLM suggestion:", post_resp.llm_suggestion)

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

