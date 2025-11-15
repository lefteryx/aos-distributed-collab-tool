import grpc
import collab_pb2, collab_pb2_grpc
import argparse
import json

DEFAULT_CLUSTER_FILE = "cluster_config.json"

def load_peers_from_args_or_file(args):
    peers = []
    if args.cluster and args.cluster != "":
        try:
            with open(args.cluster, "r") as f:
                cfg = json.load(f)
            peers = cfg.get("peers", [])
        except Exception as e:
            print("Failed to read cluster config:", e)
    elif args.peers:
        peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    if not peers:
        peers = ["localhost:50051", "localhost:50052", "localhost:50053"]
    return peers

class SeedClient:
    def __init__(self, peers):
        self.peers = peers[:]
        self.last_success = None

    def _try_rpc(self, func_name, build_req, parse_resp, retry_on_redirect=True):
        last_exc = None
        order = self.peers[:]
        if self.last_success and self.last_success in order:
            order.remove(self.last_success)
            order = [self.last_success] + order
        for target in order:
            try:
                with grpc.insecure_channel(target) as ch:
                    stub = collab_pb2_grpc.ClientServiceStub(ch)
                    method = getattr(stub, func_name)
                    req = build_req()
                    resp = method(req, timeout=8)
                    self.last_success = target
                    return parse_resp(resp)
            except grpc.RpcError as e:
                details = ""
                try:
                    details = e.details()
                except Exception:
                    pass
                if retry_on_redirect and isinstance(details, str) and details.startswith("redirect:"):
                    leader = details.split("redirect:", 1)[1]
                    if leader and leader != "unknown":
                        try:
                            with grpc.insecure_channel(leader) as ch2:
                                stub2 = collab_pb2_grpc.ClientServiceStub(ch2)
                                resp2 = getattr(stub2, func_name)(req, timeout=8)
                                self.last_success = leader
                                if leader not in self.peers:
                                    self.peers.insert(0, leader)
                                return parse_resp(resp2)
                        except Exception as e2:
                            last_exc = e2
                            continue
                last_exc = e
                continue
        raise last_exc if last_exc is not None else RuntimeError("No peers available.")

    # Wrappers
    def login(self, username, password):
        def build():
            return collab_pb2.LoginRequest(username=username, password=password)
        def parse(r):
            return r
        return self._try_rpc("Login", build, parse)

    def logout(self, token):
        def build():
            return collab_pb2.LogoutRequest(token=token)
        def parse(r):
            return r
        return self._try_rpc("Logout", build, parse)

    def get(self, token, doc_id):
        def build():
            return collab_pb2.GetRequest(token=token, doc_id=doc_id)
        def parse(r):
            return r
        return self._try_rpc("Get", build, parse)

    def lock(self, token, doc_id):
        def build():
            return collab_pb2.LockRequest(token=token, doc_id=doc_id)
        def parse(r):
            return r
        return self._try_rpc("Lock", build, parse)

    def unlock(self, token, doc_id, lock_token):
        def build():
            return collab_pb2.UnlockRequest(token=token, doc_id=doc_id, lock_token=lock_token)
        def parse(r):
            return r
        return self._try_rpc("Unlock", build, parse)

    def post(self, token, doc_id, content, lock_token="", ask_llm=False, base_version=0):
        def build():
            return collab_pb2.PostRequest(token=token, doc_id=doc_id, content=content, lock_token=lock_token, ask_llm=ask_llm, base_version=base_version)
        def parse(r):
            return r
        return self._try_rpc("Post", build, parse)

    def ask_suggestion(self, token, doc_id, mode, prompt):
        def build():
            return collab_pb2.AskSuggestionRequest(token=token, doc_id=doc_id, mode=mode, prompt=prompt)
        def parse(r):
            return r
        return self._try_rpc("AskSuggestion", build, parse)

    def apply_suggestion(self, token, suggestion_id, accept):
        def build():
            return collab_pb2.ApplySuggestionRequest(token=token, suggestion_id=suggestion_id, accept=accept)
        def parse(r):
            return r
        return self._try_rpc("ApplySuggestion", build, parse)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cluster", type=str, default=DEFAULT_CLUSTER_FILE, help="cluster config JSON file")
    ap.add_argument("--peers", type=str, default=None, help="comma-separated peers as fallback")
    args = ap.parse_args()
    peers = load_peers_from_args_or_file(args)
    client = SeedClient(peers)

    r = client.login("alice", "password")
    if not r.ok:
        print("Login failed", r.message)
        return
    token = r.token
    token_small = token[:3] + "..." + token[-3:]
    print("Logged in.\nToken:", token_small)

    g = client.get(token, "doc1")
    if g.ok:
        print(f"Current Doc Version: v{g.version}:\n {g.content}")
    L = client.lock(token, "doc1")

    if L.ok:
        print("Lock acquired:", L.lock_token)
        lock_token = L.lock_token
    else:
        print("Lock failed:", L.message)
        return
    
    P = client.post(token, "doc1", "ADD the word 'EDITED' to the end of the document.\n", lock_token=lock_token)
    print("Post response:", P.ok, "updated", P.message, "new_version", getattr(P, "new_version", None))
    
    S = client.ask_suggestion(token, "doc1", "rewrite", "Fix spellings and grammar:\nThis are the initial document.\n")
    if S.ok:
        print("Suggestion created id", S.suggestion_id, "preview:\n", S.suggestion_preview, sep="")
        print("Full suggestion:\n", S.suggestion_text, sep="")
        yn = input("Accept suggestion? (Y/N): ")
        if yn.strip().lower().startswith("y"):
            A = client.apply_suggestion(token, S.suggestion_id, True)
            print("ApplySuggestion: True")
            print("Applied New Version", getattr(A, "new_version", None))
        else:
            A = client.apply_suggestion(token, S.suggestion_id, False)
            print("ApplySuggestion: False\n", A.message)

    U = client.unlock(token, "doc1", lock_token)
    print("Unlocked")
    client.logout(token)
    print("Logged out")

if __name__ == "__main__":
    main()
