"""严格两阶段锁（Strict 2PL）确定性推演。

沿用与 RU / RC / SI 相同的初始键值与 2~3 条事务脚本，但全局调度不再预先
排定：T1~T3 轮流尝试各自队头操作——

- READ 需取得键上的共享锁（S），WRITE 需取得键上的排他锁（X）；
- 已持有共享锁的事务写同一键时可升级为排他锁（S→X，前提是没有其他事务
  持有该键的共享锁）；
- 锁一律持有到 COMMIT / ABORT 才释放（严格两阶段锁）。

冲突处理：取锁失败时记录持锁阻塞者，操作保留在队头，转试其他事务；
锁被释放后，被阻塞的事务在下一轮轮到自己时重试。

死锁处理：等待关系（等待者 → 持锁者）一旦成环，回滚环内编号最大的事务
（T3 > T2 > T1）：撤销其全部写入、跳过其剩余操作、释放其持有的全部锁，
然后继续调度，直至所有事务结束。

读取规则：自身写入优先（读己之写），否则取按提交顺序最新的已提交版本；
回滚事务的写入对任何后续读取不可见。
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

from .simulator import (
    SimError,
    Txn,
    Version,
    _annotate_cycles,
    _build_graph,
    _chain_dicts,
    _fmt,
    _initial_version,
    _own_latest,
    _rc_latest,
    parse_initial,
    parse_txn,
)

LOCK_LABEL = {"S": "共享锁(S)", "X": "排他锁(X)"}


def _txn_no(name: str) -> int:
    """事务编号：T3 > T2 > T1，用于死锁受害者选择。"""
    try:
        return int(name[1:])
    except (ValueError, IndexError):
        return 0


# ---------------------------------------------------------------------------
# 锁表
# ---------------------------------------------------------------------------
# locks[key] 为 {"X": 持有者} 或 {"S": {持有者集合}}；同一键上 X 与 S 不共存。


def _blockers(locks: Dict[str, dict], key: str, mode: str, me: str
              ) -> Tuple[bool, List[str]]:
    """检查 me 申请 key 上的 mode 锁：返回 (可否授予, 持锁阻塞者列表)。"""
    st = locks.get(key)
    if not st:
        return True, []
    if "X" in st:
        return (True, []) if st["X"] == me else (False, [st["X"]])
    if mode == "S":
        return True, []
    # X 申请（含 S→X 升级）：除自己外的共享锁持有者都是阻塞者
    others = sorted(h for h in st["S"] if h != me)
    return (False, others) if others else (True, [])


def _grant(locks: Dict[str, dict], key: str, mode: str, me: str) -> bool:
    """授予锁（调用前须确认无冲突）。返回是否发生了 S→X 升级。"""
    st = locks.get(key)
    if mode == "S":
        if st and st.get("X") == me:
            return False  # 已持有排他锁，无需再加共享锁
        if st is None:
            locks[key] = {"S": {me}}
        else:
            st.setdefault("S", set()).add(me)
        return False
    upgraded = bool(st and "S" in st and me in st["S"] and "X" not in st)
    locks[key] = {"X": me}
    return upgraded


def _release_all(locks: Dict[str, dict], me: str) -> List[dict]:
    """释放 me 持有的全部锁，返回释放清单。"""
    released = []
    for key in list(locks.keys()):
        st = locks[key]
        if st.get("X") == me:
            released.append({"key": key, "mode": "X"})
            del locks[key]
        elif me in st.get("S", ()):
            st["S"].discard(me)
            released.append({"key": key, "mode": "S"})
            if not st["S"]:
                del locks[key]
    return released


def _held(locks: Dict[str, dict], me: str) -> List[dict]:
    out = []
    for key, st in locks.items():
        if st.get("X") == me:
            out.append({"key": key, "mode": "X"})
        elif me in st.get("S", ()):
            out.append({"key": key, "mode": "S"})
    return out


def _lock_desc(info: dict) -> str:
    desc = f"{info['key']} 上的{LOCK_LABEL[info['mode']]}"
    if info.get("upgrade"):
        desc += "（S→X 升级）"
    return desc


def _locks_desc(released: List[dict]) -> str:
    if not released:
        return "无"
    return "、".join(f"{l['key']} 的{LOCK_LABEL[l['mode']]}" for l in released)


# ---------------------------------------------------------------------------
# 等待图与死锁
# ---------------------------------------------------------------------------


def _wait_edges(waits: Dict[str, dict], locks: Dict[str, dict]) -> List[dict]:
    """由当前等待请求与锁表推导等待边（等待者 → 持锁阻塞者）。"""
    edges = []
    for name, req in waits.items():
        _, blockers = _blockers(locks, req["key"], req["mode"], name)
        for b in blockers:
            edges.append({"from": name, "to": b,
                          "key": req["key"], "mode": req["mode"]})
    return edges


def _find_cycle(edges: List[dict]) -> Optional[List[str]]:
    """在等待图中找一条具体的环（节点按事务名顺序深度优先），无环返回 None。"""
    adj: Dict[str, List[str]] = {}
    for e in edges:
        if e["to"] not in adj.setdefault(e["from"], []):
            adj[e["from"]].append(e["to"])
    for k in adj:
        adj[k].sort()
    color: Dict[str, int] = {}
    stack: List[str] = []

    def dfs(u: str) -> Optional[List[str]]:
        color[u] = 1
        stack.append(u)
        for w in adj.get(u, []):
            if color.get(w, 0) == 0:
                found = dfs(w)
                if found:
                    return found
            elif color.get(w) == 1:
                return stack[stack.index(w):]
        stack.pop()
        color[u] = 2
        return None

    for n in sorted(adj):
        if color.get(n, 0) == 0:
            found = dfs(n)
            if found:
                return found
    return None


# ---------------------------------------------------------------------------
# 推演主体
# ---------------------------------------------------------------------------


def simulate_s2pl(initial: "OrderedDict[str, Any]",
                  txn_scripts: List[Tuple[str, str]]) -> dict:
    """严格两阶段锁完整推演：轮流尝试队头操作，冲突等待，成环回滚。"""
    txns: "OrderedDict[str, Txn]" = OrderedDict()
    for name, text in txn_scripts:
        txns[name] = Txn(name=name, ops=parse_txn(name, text))
    txn_list = list(txns.values())

    chains: Dict[str, List[Version]] = OrderedDict()
    next_vid = 1
    for key, value in initial.items():
        chains[key] = [_initial_version(next_vid, key, value)]
        next_vid += 1

    locks: "OrderedDict[str, dict]" = OrderedDict()
    waits: "OrderedDict[str, dict]" = OrderedDict()  # name -> {key, mode, upgrade, blockers}
    steps: List[dict] = []
    deadlocks: List[dict] = []
    deadlock_victims: set = set()
    seq = 0

    # ----- 快照 -----

    def locks_snapshot() -> dict:
        out: "OrderedDict[str, dict]" = OrderedDict()
        for key, st in locks.items():
            if "X" in st:
                out[key] = {"mode": "X", "holders": [st["X"]]}
            else:
                out[key] = {"mode": "S", "holders": sorted(st["S"])}
        return out

    def txns_snapshot() -> dict:
        return {
            x.name: {
                "status": x.status,
                "begin_seq": x.begin_seq,
                "commit_seq": x.commit_seq,
                "abort_seq": x.abort_seq,
                "writes": [{"key": k, "wseq": s} for k, s in x.writes],
                "locks": _held(locks, x.name),
                "waiting": x.name in waits,
            }
            for x in txn_list
        }

    # ----- 步骤记录 -----

    def base_step(event: str, txn: str, op: str,
                  key: Optional[str], value: Any) -> dict:
        return {
            "seq": seq,
            "event": event,            # exec | wait | deadlock
            "txn": txn,
            "op": op,
            "key": key,
            "value": value,
            "read_vid": None,
            "read_value": None,
            "read_source": None,       # own | committed | initial | missing
            "read_writer": None,
            "read_wseq": None,
            "new_vid": None,
            "txn_status_before": txns[txn].status,
            "txn_status_after": None,
            "snapshot": None,
            "conflict_key": None,
            "conflict_vid": None,
            "dirty_candidate": False,
            "lock_acquired": None,     # {key, mode, upgrade}
            "locks_released": None,    # [{key, mode}]
            "resumed": False,          # 等待后重试成功
            "info": "",
        }

    def finish_step(step: dict) -> None:
        step["chains_after"] = _chain_dicts(chains)
        step["txns_after"] = txns_snapshot()
        step["locks_after"] = locks_snapshot()
        step["wait_edges_after"] = _wait_edges(waits, locks)
        steps.append(step)

    def record_exec(t: Txn, op: str, key: Optional[str], value: Any,
                    resumed: bool, lock_info: Optional[dict]) -> None:
        nonlocal seq, next_vid
        seq += 1
        step = base_step("exec", t.name, op, key, value)
        step["resumed"] = resumed
        step["lock_acquired"] = lock_info
        t.pc += 1

        if op == "BEGIN":
            t.status = "active"
            t.begin_seq = seq
            step["info"] = (f"{t.name} 开始（严格两阶段锁：读前取共享锁、写前取排他锁，"
                            f"锁持有到提交或回滚才释放）")

        elif op == "READ":
            assert key is not None
            chains.setdefault(key, [])
            own = _own_latest(t, chains, key)
            if own is not None:
                chosen, source = own, "own"
            else:
                latest = _rc_latest(chains, key)
                if latest is None:
                    chosen, source = None, "missing"
                else:
                    chosen = latest
                    source = "initial" if latest.kind == "initial" else "committed"
            step["read_source"] = source
            if chosen is not None:
                step["read_vid"] = chosen.vid
                step["read_value"] = chosen.value
                step["read_writer"] = chosen.writer
                step["read_wseq"] = chosen.wseq
            t.reads.append((key, seq, chosen.vid if chosen else None))
            lock_desc = _lock_desc(lock_info)
            prefix = "等待结束，重新取锁成功。" if resumed else ""
            if source == "own":
                step["info"] = (f"{prefix}取得 {lock_desc}，读到本事务自己的写入 "
                                f"v{chosen.vid}={_fmt(chosen.value)}（自身写入优先）")
            elif source == "initial":
                step["info"] = f"{prefix}取得 {lock_desc}，读到初始值 v{chosen.vid}={_fmt(chosen.value)}"
            elif source == "committed":
                step["info"] = (f"{prefix}取得 {lock_desc}，读到 {chosen.writer} 已提交的版本 "
                                f"v{chosen.vid}={_fmt(chosen.value)}（按提交顺序最新）")
            else:
                step["info"] = f"{prefix}取得 {lock_desc}，键 {key} 不存在，读得 ∅"

        elif op == "WRITE":
            assert key is not None
            chains.setdefault(key, [])
            v = Version(vid=next_vid, key=key, value=value, kind="uncommitted",
                        writer=t.name, wseq=seq)
            next_vid += 1
            chains[key].append(v)
            t.writes.append((key, seq))
            step["new_vid"] = v.vid
            lock_desc = _lock_desc(lock_info)
            prefix = "等待结束，重新取锁成功。" if resumed else ""
            step["info"] = (f"{prefix}取得 {lock_desc}，写入新版本 v{v.vid}={_fmt(value)}"
                            f"（未提交；排他锁保证提交前他人无法读写该键）")

        elif op == "COMMIT":
            t.status = "committed"
            t.commit_seq = seq
            for wkey, wseq in t.writes:
                for v in chains[wkey]:
                    if v.wseq == wseq:
                        v.kind = "committed"
                        v.cseq = seq
            released = _release_all(locks, t.name)
            step["locks_released"] = released
            step["info"] = (f"提交成功，{len(t.writes)} 个写入转为已提交版本；"
                            f"释放 {len(released)} 把锁（{_locks_desc(released)}）")

        else:  # ABORT
            t.status = "aborted"
            t.abort_seq = seq
            for wkey, wseq in t.writes:
                for v in chains[wkey]:
                    if v.wseq == wseq:
                        v.kind = "aborted"
            released = _release_all(locks, t.name)
            step["locks_released"] = released
            step["info"] = (f"主动回滚，{len(t.writes)} 个写入全部作废；"
                            f"释放 {len(released)} 把锁（{_locks_desc(released)}）")

        step["txn_status_after"] = t.status
        finish_step(step)

    def record_wait(t: Txn, op: str, key: Optional[str], value: Any,
                    req: dict, changed: bool) -> None:
        nonlocal seq
        seq += 1
        step = base_step("wait", t.name, op, key, value)
        step["lock_request"] = {"key": req["key"], "mode": req["mode"],
                                "upgrade": req["upgrade"]}
        step["blockers"] = list(req["blockers"])
        step["changed"] = changed
        lock_desc = _lock_desc(req)
        blk = "、".join(req["blockers"])
        if changed:
            step["info"] = (f"{t.name} 重试申请 {lock_desc} 仍失败：持锁阻塞者变为 {blk}，"
                            f"继续保留队头等待")
        else:
            step["info"] = (f"{t.name} 申请 {lock_desc} 失败：{blk} 持有冲突锁，"
                            f"操作保留在队头，转试其他事务；锁释放后重试")
        step["txn_status_after"] = t.status
        finish_step(step)

    def record_deadlock(vt: Txn, cycle: List[str], undone: List[dict],
                        released: List[dict]) -> None:
        nonlocal seq
        seq += 1
        step = base_step("deadlock", vt.name, "DEADLOCK", None, None)
        vt.status = "aborted"
        vt.abort_seq = seq
        step["cycle"] = cycle
        step["victim"] = vt.name
        step["undone"] = undone
        step["locks_released"] = released
        cyc_label = " → ".join(cycle + [cycle[0]])
        undone_desc = "、".join(f"{u['key']}=v{u['vid']}" for u in undone) or "无"
        step["info"] = (f"等待关系成环（{cyc_label}），回滚环内编号最大的 {vt.name}："
                        f"撤销 {len(undone)} 个写入（{undone_desc}），"
                        f"释放 {len(released)} 把锁，跳过其剩余操作")
        step["txn_status_after"] = vt.status
        finish_step(step)
        deadlocks.append({
            "seq": seq,
            "cycle": cycle,
            "victim": vt.name,
            "undone": undone,
            "released_locks": released,
        })

    def resolve_deadlocks() -> None:
        """等待图成环时反复回滚环内编号最大的事务，直至无环。"""
        while True:
            cycle = _find_cycle(_wait_edges(waits, locks))
            if not cycle:
                return
            victim = max(cycle, key=_txn_no)
            vt = txns[victim]
            undone = []
            for wkey, wseq in vt.writes:
                for v in chains[wkey]:
                    if v.wseq == wseq and v.kind == "uncommitted":
                        v.kind = "aborted"
                        undone.append({"key": wkey, "vid": v.vid, "value": v.value})
            released = _release_all(locks, victim)
            waits.pop(victim, None)
            vt.pc = len(vt.ops)  # 跳过剩余操作
            deadlock_victims.add(victim)
            record_deadlock(vt, cycle, undone, released)

    # ----- 主循环：T1~T3 轮流尝试各自队头操作 -----

    guard = 0
    while True:
        if all(t.pc >= len(t.ops) or t.status in ("committed", "aborted")
               for t in txn_list):
            break
        guard += 1
        if guard > 1000:
            raise SimError("S2PL 推演异常：超过调度轮数上限")
        progressed = False
        for t in txn_list:
            if t.pc >= len(t.ops) or t.status in ("committed", "aborted"):
                continue
            op, key, value = t.ops[t.pc]

            if t.name in waits:
                # 队头操作此前被阻塞：本轮先重试取锁
                req = waits[t.name]
                ok, blockers = _blockers(locks, req["key"], req["mode"], t.name)
                if not ok:
                    if blockers != req["blockers"]:
                        # 持锁阻塞者发生变化：更新等待关系并重新检查死锁
                        req["blockers"] = blockers
                        record_wait(t, op, key, value, req, changed=True)
                        resolve_deadlocks()
                        progressed = True
                    continue
                upgraded = _grant(locks, req["key"], req["mode"], t.name)
                lock_info = {"key": req["key"], "mode": req["mode"],
                             "upgrade": upgraded}
                del waits[t.name]
                record_exec(t, op, key, value, resumed=True, lock_info=lock_info)
                progressed = True
                continue

            if op in ("READ", "WRITE"):
                assert key is not None
                mode = "S" if op == "READ" else "X"
                ok, blockers = _blockers(locks, key, mode, t.name)
                if not ok:
                    st = locks.get(key)
                    upgrade = mode == "X" and bool(st and t.name in st.get("S", ()))
                    waits[t.name] = {"key": key, "mode": mode,
                                     "upgrade": upgrade, "blockers": blockers}
                    record_wait(t, op, key, value, waits[t.name], changed=False)
                    resolve_deadlocks()
                    progressed = True
                    continue
                upgraded = _grant(locks, key, mode, t.name)
                record_exec(t, op, key, value, resumed=False,
                            lock_info={"key": key, "mode": mode, "upgrade": upgraded})
                progressed = True
            else:
                record_exec(t, op, key, value, resumed=False, lock_info=None)
                progressed = True

        if not progressed:
            # 理论上不可达：全部阻塞意味着等待图成环，而成环会立即被解除
            raise SimError("S2PL 推演异常：存在无法解决的等待")

    # ----- 汇总 -----

    graph = _build_graph(txn_list, chains)
    _annotate_cycles(graph, steps)

    final_state: "OrderedDict[str, Any]" = OrderedDict()
    for key in chains:
        latest = _rc_latest(chains, key)
        final_state[key] = None if latest is None else {
            "value": latest.value, "vid": latest.vid, "writer": latest.writer,
        }

    return {
        "mode": "S2PL",
        "schedule": [
            {"txn": s["txn"], "op": s["op"], "key": s["key"], "value": s["value"]}
            for s in steps if s["event"] == "exec"
        ],
        "steps": steps,
        "dirty_reads": [],
        "deadlocks": deadlocks,
        "graph": graph,
        "txn_final": {
            t.name: {
                "status": t.status,
                "begin_seq": t.begin_seq,
                "commit_seq": t.commit_seq,
                "abort_seq": t.abort_seq,
                "deadlock_victim": t.name in deadlock_victims,
            }
            for t in txn_list
        },
        "final_state": final_state,
    }


def run_s2pl(initial_text: str, txn_texts: List[str]) -> dict:
    initial = parse_initial(initial_text)
    if not 2 <= len(txn_texts) <= 3:
        raise SimError("需要 2~3 条事务脚本")
    txn_scripts = [(f"T{i+1}", text) for i, text in enumerate(txn_texts)]
    names = [n for n, _ in txn_scripts]
    if len(set(names)) != len(names):
        raise SimError("事务名重复")
    return {
        "initial": {k: v for k, v in initial.items()},
        "result": simulate_s2pl(initial, txn_scripts),
    }
