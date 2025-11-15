import asyncio
import json
import os
import random
import logging
from typing import Optional, List, Dict
import collab_pb2, collab_pb2_grpc
import grpc

log = logging.getLogger("raft.raft")

RAFT_STATES_DIR = "states"
os.makedirs(RAFT_STATES_DIR, exist_ok=True)
RAFT_STATE_TEMPLATE = os.path.join(RAFT_STATES_DIR, "raft_state_{}.json")

ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0

HEARTBEAT_INTERVAL = 0.8

def load_raft_state(port: int):
    fname = RAFT_STATE_TEMPLATE.format(port)
    if not os.path.exists(fname):
        return {"currentTerm": 0, "votedFor": None, "log": []}
    try:
        with open(fname, "r") as f:
            data = json.load(f)
        # basic normalization
        if "currentTerm" not in data:
            data["currentTerm"] = 0
        if "votedFor" not in data:
            data["votedFor"] = None
        if "log" not in data:
            data["log"] = []
        return data
    except Exception as e:
        log.warning(f"Failed to load raft state {fname}: {e}")
        return {"currentTerm": 0, "votedFor": None, "log": []}

def save_raft_state(port: int, state: dict):
    fname = RAFT_STATE_TEMPLATE.format(port)
    tmp = fname + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, fname)

class RaftNode:
    def __init__(self, node_addr: str, peers: List[str], port: int):
        self.node_addr = node_addr
        self.port = port
        self.peers = peers[:]
        self.state = load_raft_state(port)
        self.currentTerm: int = int(self.state.get("currentTerm", 0))
        self.votedFor: Optional[str] = self.state.get("votedFor")
        self.log: List[Dict] = self.state.get("log", [])
        self.commitIndex: int = 0
        self.lastApplied: int = 0

        self.nextIndex: Dict[str, int] = {}
        self.matchIndex: Dict[str, int] = {}

        # role: 'follower', 'candidate', 'leader'
        self.role = "follower"

        self._election_timer_handle = None
        self._election_timeout = self._gen_election_timeout()
        self._election_event = asyncio.Event()

        self._heartbeat_task = None

        self._running = False

        self._commit_waiters: Dict[int, asyncio.Future] = {}

        self._lock = asyncio.Lock()

        log.info(f"RaftNode init: {self.node_addr}, term={self.currentTerm}, role={self.role}, peers={self.peers}")


    def _gen_election_timeout(self):
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def persist_state(self):
        self.state["currentTerm"] = self.currentTerm
        self.state["votedFor"] = self.votedFor
        self.state["log"] = self.log
        save_raft_state(self.port, self.state)

    def last_log_index(self):
        if not self.log:
            return 0
        return self.log[-1]["index"]

    def last_log_term(self):
        if not self.log:
            return 0
        return self.log[-1]["term"]

    async def start(self):
        self._running = True
        self._election_event.clear()
        asyncio.create_task(self._run_election_timer())
        log.info("RaftNode started election timer")


    async def stop(self):
        self._running = False
        log.info("RaftNode stopped")

    async def handle_RequestVote(self, req: collab_pb2.RequestVoteRequest):
        async with self._lock:
            resp = collab_pb2.RequestVoteResponse()
            if req.term < self.currentTerm:
                resp.term = self.currentTerm
                resp.voteGranted = False
                return resp
            # if req.term > currentTerm then update term and convert to follower
            if req.term > self.currentTerm:
                self.currentTerm = req.term
                self.votedFor = None
                self.role = "follower"
                self.persist_state()
                self._election_event.set()
                self._election_event.clear()
            # grant vote if not voted or votedFor is candidateId and candidate's log is at least up-to-date
            up_to_date = (req.lastLogTerm > self.last_log_term()) or \
                         (req.lastLogTerm == self.last_log_term() and req.lastLogIndex >= self.last_log_index())
            if (self.votedFor is None or self.votedFor == req.candidateId) and up_to_date:
                self.votedFor = req.candidateId
                resp.voteGranted = True

                self._election_timeout = self._gen_election_timeout()
                self.persist_state()

                self._election_event.set()
                self._election_event.clear()

            resp.term = self.currentTerm
            return resp

    async def handle_AppendEntries(self, req: collab_pb2.AppendEntriesRequest):
        async with self._lock:
            resp = collab_pb2.AppendEntriesResponse()
            # 1) Reply false if term < currentTerm
            if req.term < self.currentTerm:
                resp.term = self.currentTerm
                resp.success = False
                resp.matchIndex = self.last_log_index()
                return resp
            # 2) If term >= currentTerm, accept and become follower
            if req.term > self.currentTerm:
                self.currentTerm = req.term
                self.votedFor = None
                self.role = "follower"
                self.persist_state()
            # Wake/reset election timer to avoid immediate election
            self._election_event.set()
            self._election_event.clear()
            # Also refresh randomized timeout for next cycle
            self._election_timeout = self._gen_election_timeout()

            # Check consistency: prevLogIndex and prevLogTerm must match
            prev_index = req.prevLogIndex
            prev_term = req.prevLogTerm
            if prev_index > 0:
                if prev_index > self.last_log_index():
                    # missing entry
                    resp.term = self.currentTerm
                    resp.success = False
                    resp.matchIndex = self.last_log_index()
                    return resp
                # check term match
                local_term = 0
                if prev_index - 1 < len(self.log):
                    local_term = self.log[prev_index - 1]["term"]
                if local_term != prev_term:
                    # conflict: delete the entry and all that follow it
                    # Keep log up to prev_index -1
                    self.log = self.log[:prev_index - 1]
                    self.persist_state()
                    resp.term = self.currentTerm
                    resp.success = False
                    resp.matchIndex = self.last_log_index()
                    return resp
            # Append any new entries (possibly zero entries = heartbeat)
            if req.entries:
                for e in req.entries:
                    # if entry index already exists and term mismatch, delete from that index onwards
                    idx = e.index
                    if idx <= self.last_log_index():
                        # existing entry; check term
                        existing = self.log[idx - 1]
                        if existing["term"] != e.term:
                            # delete conflict and append
                            self.log = self.log[: idx - 1]
                            self.log.append({"index": e.index, "term": e.term, "op": e.op, "payload": e.payload})
                        else:
                            # already present, skip
                            pass
                    else:
                        # append new
                        self.log.append({"index": e.index, "term": e.term, "op": e.op, "payload": e.payload})
                self.persist_state()
            # Update commitIndex
            if req.leaderCommit > self.commitIndex:
                old_commit = self.commitIndex
                self.commitIndex = min(req.leaderCommit, self.last_log_index())
                # Apply entries up to commitIndex
                # application to state machine is handled by higher layer: they will call apply_committed_entries()
            resp.term = self.currentTerm
            resp.success = True
            resp.matchIndex = self.last_log_index()
            return resp

    # ----------------------
    # RPC client helpers used by candidate/leader to contact peers
    # ----------------------
    async def send_RequestVote(self, peer: str, req: collab_pb2.RequestVoteRequest, timeout=1.0):
        try:
            async with grpc.aio.insecure_channel(peer) as ch:
                stub = collab_pb2_grpc.RaftStub(ch)
                resp = await stub.RequestVote(req, timeout=timeout)
                return resp
        except Exception as e:
            log.debug(f"RequestVote to {peer} failed: {e}")
            return None

    async def send_AppendEntries(self, peer: str, req: collab_pb2.AppendEntriesRequest, timeout=2.0):
        try:
            async with grpc.aio.insecure_channel(peer) as ch:
                stub = collab_pb2_grpc.RaftStub(ch)
                resp = await stub.AppendEntries(req, timeout=timeout)
                return resp
        except Exception as e:
            log.debug(f"AppendEntries to {peer} failed: {e}")
            return None

    # ----------------------
    # Candidate election loop
    # ----------------------
    async def _run_election_timer(self):
        while self._running:
            # choose randomized timeout
            self._election_timeout = self._gen_election_timeout()
            try:
                # Wait until event is set or timeout occurs
                await asyncio.wait_for(self._election_event.wait(), timeout=self._election_timeout)
                # event set => clear and loop to wait again
                self._election_event.clear()
                # do not start election; continue loop to wait again
                continue
            except asyncio.TimeoutError:
                # timeout occurred -> start election if not leader
                async with self._lock:
                    if self.role == "leader":
                        # leader should not start election
                        continue
                    # start election by becoming candidate
                    self.role = "candidate"
                    self.currentTerm += 1
                    self.votedFor = self.node_addr
                    self.persist_state()
                    term_at_start = self.currentTerm
                log.info(f"[Election] node {self.node_addr} starting election for term {term_at_start}")
                # proceed to request votes
                votes = 1  # vote for self
                # prepare RequestVote request
                req = collab_pb2.RequestVoteRequest(
                    term=term_at_start,
                    candidateId=self.node_addr,
                    lastLogIndex=self.last_log_index(),
                    lastLogTerm=self.last_log_term()
                )
                coros = [self.send_RequestVote(peer, req) for peer in self.peers]
                results = await asyncio.gather(*coros, return_exceptions=True)
                # process results
                for r in results:
                    if isinstance(r, collab_pb2.RequestVoteResponse):
                        if r.voteGranted:
                            votes += 1
                        async with self._lock:
                            if r.term > self.currentTerm:
                                # higher term seen -> step down
                                self.currentTerm = r.term
                                self.role = "follower"
                                self.votedFor = None
                                self.persist_state()
                # check majority
                if votes > (len(self.peers) + 1) // 2:
                    async with self._lock:
                        if self.role == "candidate" and self.currentTerm == term_at_start:
                            self.role = "leader"
                            log.info(f"[Election] node {self.node_addr} became leader for term {self.currentTerm}")
                            # initialize leader volatile state
                            next_index = self.last_log_index() + 1
                            for p in self.peers:
                                self.nextIndex[p] = next_index
                                self.matchIndex[p] = 0
                            # start heartbeat loop
                            if self._heartbeat_task is None or self._heartbeat_task.done():
                                self._heartbeat_task = asyncio.create_task(self._leader_heartbeat_loop())
                            # Ensure election timer doesn't immediately fire again:
                            self._election_event.set()
                            self._election_event.clear()
                else:
                    # election failed; revert to follower and continue waiting
                    async with self._lock:
                        self.role = "follower"
                        self._election_event.clear()
                        # continue loop and wait for next event or timeout
                        continue


    # ----------------------
    # Leader heartbeat + replication loop
    # ----------------------
    async def _leader_heartbeat_loop(self):
        while self._running and self.role == "leader":
            # send AppendEntries as heartbeat and also replicate pending entries
            await self._replicate_once()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _replicate_once(self):
        for peer in self.peers:
            next_idx = self.nextIndex.get(peer, self.last_log_index() + 1)
            # prevLogIndex is next_idx -1
            prev_index = next_idx - 1
            prev_term = 0
            if prev_index > 0 and prev_index - 1 < len(self.log):
                prev_term = self.log[prev_index - 1]["term"]
            # prepare entries
            entries = []
            for e in self.log[next_idx - 1:]:
                le = collab_pb2.LogEntry(index=e["index"], term=e["term"], op=e["op"], payload=e["payload"])
                entries.append(le)
            req = collab_pb2.AppendEntriesRequest(
                term=self.currentTerm,
                leaderId=self.node_addr,
                prevLogIndex=prev_index,
                prevLogTerm=prev_term,
                entries=entries,
                leaderCommit=self.commitIndex
            )
            resp = await self.send_AppendEntries(peer, req)
            if isinstance(resp, collab_pb2.AppendEntriesResponse):
                if resp.success:
                    # update nextIndex and matchIndex
                    self.matchIndex[peer] = resp.matchIndex
                    self.nextIndex[peer] = resp.matchIndex + 1
                else:
                    # decrement nextIndex and retry later (backoff)
                    self.nextIndex[peer] = max(1, self.nextIndex.get(peer, self.last_log_index() + 1) - 1)
            else:
                # peer didn't respond; leave nextIndex unchanged
                pass
        # After polling followers, update commitIndex: find highest N > commitIndex such that
        # a majority have matchIndex >= N and log[N].term == currentTerm
        N = self.last_log_index()
        while N > self.commitIndex:
            count = 1  # leader itself
            for peer in self.peers:
                if self.matchIndex.get(peer, 0) >= N:
                    count += 1
            if count > (len(self.peers) + 1) // 2:
                # check term of entry N
                entry_term = 0
                if N - 1 < len(self.log):
                    entry_term = self.log[N - 1]["term"]
                if entry_term == self.currentTerm:
                    self.commitIndex = N
                    log.info(f"[Leader] commitIndex advanced to {self.commitIndex}")
                    # Notify waiters up to commitIndex
                    await self._notify_commits()
                    break
            N -= 1

    # ----------------------
    # API for application to submit command
    # ----------------------
    async def submit_command(self, op: str, payload: str) -> int:
        async with self._lock:
            # create new log entry
            new_index = self.last_log_index() + 1
            entry = {"index": new_index, "term": self.currentTerm, "op": op, "payload": payload}
            self.log.append(entry)
            self.persist_state()
            fut = asyncio.get_running_loop().create_future()
            self._commit_waiters[new_index] = fut
        await self._replicate_once()
        try:
            res = await asyncio.wait_for(fut, timeout=10.0)
            return new_index
        except asyncio.TimeoutError:
            raise TimeoutError("submit_command timed out waiting for commit")
        finally:
            # cleanup waiter if still present
            self._commit_waiters.pop(new_index, None)

    async def _notify_commits(self):
        # apply entries up to commitIndex (and notify waiters)
        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            idx = self.lastApplied
            # notify waiter if present
            fut = self._commit_waiters.get(idx)
            if fut and not fut.done():
                fut.set_result(True)

    # ----------------------
    # Utilities for RPC server to call apply committed entries
    # ----------------------
    def get_committed_entries_since(self, last_applied: int):
        res = []
        # no new entries if commitIndex <= last_applied
        if self.commitIndex <= last_applied:
            return res
        # Clamp upper bound to last_log_index for safety
        upper = min(self.commitIndex, self.last_log_index())
        # iterate indices from last_applied+1 to upper, inclusive
        for idx in range(last_applied + 1, upper + 1):
            # python list index is idx-1
            try:
                e = self.log[idx - 1]
                res.append(e)
            except IndexError:
                # defensive: skip if inconsistent (shouldn't happen in correct code)
                log.warning(f"get_committed_entries_since: missing log entry for index {idx}")
                continue
        return res


    def get_state_snapshot(self):
        return {
            "currentTerm": self.currentTerm,
            "votedFor": self.votedFor,
            "log": self.log,
            "commitIndex": self.commitIndex,
            "lastApplied": self.lastApplied,
            "role": self.role
        }
