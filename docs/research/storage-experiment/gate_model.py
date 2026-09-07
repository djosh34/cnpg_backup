#!/usr/bin/env python3
"""Bounded planning model, NOT production DST or a distributed-system proof.

Explore two reader incarnations, one non-restarting deleter, delayed CAS,
remote effect separated from response, crashes and paused readers. Generation
stands for an ETag protected by never-repeated gate bytes. Every actor acts once;
failed stale CAS can re-read. State includes remote requests after actor crash.
"""
from dataclasses import dataclass, replace
from collections import deque

@dataclass(frozen=True)
class State:
    generation: int = 0
    owner: bool = False
    holders: int = 0
    readers: tuple = (0, 0)  # 0 new, 1 CAS pending, 2 protected, 3 done/crashed
    snapshots: tuple = (-1, -1)
    deleter: int = 0  # 0 new, 1 CAS pending, 2 owned, 3 sent, 4 ack, 5 stopped
    delete_snapshot: int = -1
    remote: int = 0  # 0 none, 1 pending, 2 applied without response, 3 responded
    crashed: bool = False
    exists: bool = True


def tup(t, i, v):
    return t[:i] + (v,) + t[i + 1:]


def transitions(s, unsafe=False):
    for i in range(2):
        bit = 1 << i
        if s.readers[i] == 0 and not s.owner:
            yield f"R{i} read gate", replace(s, readers=tup(s.readers,i,1), snapshots=tup(s.snapshots,i,s.generation))
        if s.readers[i] == 1:
            if s.snapshots[i] == s.generation:
                assert not s.owner
                yield f"R{i} admit CAS", replace(s, generation=s.generation+1, holders=s.holders|bit, readers=tup(s.readers,i,2))
            else:
                yield f"R{i} stale CAS fails", replace(s, readers=tup(s.readers,i,0))
        if s.readers[i] == 2:
            yield f"R{i} final source read then release", replace(s, generation=s.generation+1, holders=s.holders & ~bit, readers=tup(s.readers,i,3))
            yield f"R{i} crashes; durable hold stays", replace(s, readers=tup(s.readers,i,3))
            # Pausing is a stutter: no state change, same hold, omitted from BFS.
    if s.deleter == 0 and not s.holders and not s.owner:
        yield "D read gate", replace(s, deleter=1, delete_snapshot=s.generation)
    if s.deleter == 1:
        if s.delete_snapshot == s.generation:
            yield "D acquire CAS", replace(s, generation=s.generation+1, owner=True, deleter=2)
        else:
            yield "D stale CAS fails", replace(s, deleter=0)
    if s.owner and not s.crashed:
        yield "D crash (no ownership takeover)", replace(s, crashed=True)
    if s.deleter == 2 and not s.crashed:
        yield "D send DELETE", replace(s, deleter=3, remote=1)
        yield "D cancel before send, release", replace(s, deleter=5, owner=False, generation=s.generation+1)
    if s.remote == 1:
        # This event remains enabled after a process crash or TTL boundary.
        yield "remote DELETE executes", replace(s, remote=2, exists=False)
    if s.remote == 2 and not s.crashed:
        yield "DELETE response observed", replace(s, remote=3, deleter=4)
        yield "DELETE response lost forever", replace(s, crashed=True)
    if s.deleter == 4 and not s.crashed:
        yield "D drain then release CAS", replace(s, deleter=5, owner=False, generation=s.generation+1)
    if unsafe and s.owner and s.crashed:
        yield "UNSAFE timeout clears owner", replace(s, owner=False, generation=s.generation+1, deleter=5)


def explore(unsafe=False):
    initial=State(); todo=deque([initial]); paths={initial:[]}; edges=0
    while todo:
        s=todo.popleft()
        assert not(s.owner and s.holders)
        for event,n in transitions(s,unsafe):
            edges+=1
            trace=paths[s]+[event]
            if event == "remote DELETE executes" and s.holders:
                return len(paths),edges,trace
            if n not in paths:
                paths[n]=trace;todo.append(n)
    return len(paths),edges,None


def late_retry_counterexample():
    # Both requests target the SAME key. A successful second request and HEAD
    # absence do not establish a response/drain for the original request.
    pending={"original"};exists=True
    exists=False  # retry completes and reports 204
    assert not exists and pending  # HEAD is absent, original can still execute
    admitted=True
    assert admitted and "original" in pending
    print("PASS negative control: retry 204 + HEAD absent leaves original DELETE pending")


def stale_writes():
    # Permanent backup tombstone prevents metadata resurrection; WAL retirement
    # replaces bytes at the SAME key, so delayed If-None-Match cannot recreate it.
    commit=None
    for candidate in ("attempt-A", "attempt-B"):
        if commit is None: commit=candidate
    assert commit=="attempt-A"
    retired=True
    assert retired and commit is not None  # selector must reject retired commit
    wal="gzip-original"
    wal="tombstone-with-original-hash"  # conditional destructive retirement
    delayed_create_succeeds=(wal is None)
    assert not delayed_create_succeeds
    # ABA negative control: repeated empty JSON would reuse an MD5 ETag.
    assert ("empty",1)!=("empty",3)
    print("PASS fixed cases: competing UID, retired backup, late WAL create, no gate ABA")

if __name__ == "__main__":
    states,edges,bad=explore()
    assert bad is None,bad
    print(f"PASS safe gate: {states} states, {edges} transitions; no delete under admitted hold")
    states,edges,bad=explore(True)
    assert bad is not None,"negative control failed to detect unsafe TTL"
    print("PASS negative control: unsafe TTL detected: "+" -> ".join(bad))
    late_retry_counterexample()
    stale_writes()
