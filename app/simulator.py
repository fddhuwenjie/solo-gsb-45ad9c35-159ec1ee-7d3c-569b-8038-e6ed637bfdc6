"""事务交错确定性推演引擎。

输入初始键值与 2~3 条事务脚本，输出三种隔离级别下的逐步轨迹、
版本链、事务状态，以及基于已提交事务真实读写关系的 WR / WW / RW
依赖图与环信息。

全局调度规则：从各事务队头依次取一条操作（轮转调度，事务结束后跳过），
三种隔离级别共用同一份调度，保证差异只来自隔离语义。

另支持调度对照：同一组脚本可改用调用方给定的执行顺序（由事务名组成，
每次取对应事务尚未执行的队头操作），与默认轮转各自独立推演后，
按（事务名，事务内操作序号）对齐 READ / COMMIT / ABORT 比较差异。
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------

VALID_OPS = {"BEGIN", "READ", "WRITE", "COMMIT", "ABORT"}


@dataclass
class Version:
    vid: int
    key: str
    value: Any
    kind: str  # initial | uncommitted | committed | aborted
    writer: str  # "INIT" 或事务名
    wseq: Optional[int]  # WRITE 操作的全局序号
    # COMMIT 操作的全局序号（仅内部使用，不随响应输出）：
    # 初始版本为 0；未提交/已回滚为 None。版本链按写入顺序排列，
    # 而“最新已提交”按 cseq（提交顺序）判定——先写者可以晚提交。
    cseq: Optional[int] = None


@dataclass
class Txn:
    name: str
    ops: List[Tuple[str, Optional[str], Optional[Any]]]
    pc: int = 0
    status: str = "pending"  # pending | active | committed | aborted
    begin_seq: Optional[int] = None
    commit_seq: Optional[int] = None
    abort_seq: Optional[int] = None
    snapshot: Dict[str, Version] = field(default_factory=dict)
    # 写过的键（WRITE 全局序号按顺序）
    writes: List[Tuple[str, int]] = field(default_factory=list)
    # 读过的键 [(key, rseq, vid 或 None)]
    reads: List[Tuple[str, int, Optional[int]]] = field(default_factory=list)
    # RU 下读到的其他活跃事务的脏版本，结束后确认是否为脏读
    dirty_hits: List[Tuple[str, int, int, str, Any]] = field(default_factory=list)


class SimError(ValueError):
    """用户输入或脚本语义错误。"""


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------


def parse_txn(name: str, text: str) -> List[Tuple[str, Optional[str], Optional[Any]]]:
    ops: List[Tuple[str, Optional[str], Optional[Any]]] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        cmd = parts[0].upper()
        if cmd not in VALID_OPS:
            raise SimError(
                f"{name} 第 {lineno} 行：未知操作 “{parts[0]}”，"
                "只允许 BEGIN / READ / WRITE / COMMIT / ABORT"
            )
        if cmd == "READ":
            if len(parts) != 2:
                raise SimError(f"{name} 第 {lineno} 行：READ 用法为 READ key")
            ops.append(("READ", parts[1], None))
        elif cmd == "WRITE":
            if len(parts) != 3:
                raise SimError(f"{name} 第 {lineno} 行：WRITE 用法为 WRITE key value")
            ops.append(("WRITE", parts[1], _parse_value(parts[2], name, lineno)))
        elif cmd == "BEGIN":
            if len(parts) != 1:
                raise SimError(f"{name} 第 {lineno} 行：BEGIN 不带参数")
            ops.append(("BEGIN", None, None))
        elif cmd == "COMMIT":
            if len(parts) != 1:
                raise SimError(f"{name} 第 {lineno} 行：COMMIT 不带参数")
            ops.append(("COMMIT", None, None))
        else:  # ABORT
            if len(parts) != 1:
                raise SimError(f"{name} 第 {lineno} 行：ABORT 不带参数")
            ops.append(("ABORT", None, None))
    if not ops:
        raise SimError(f"{name} 脚本为空")
    if ops[0][0] != "BEGIN":
        raise SimError(f"{name}：第一条操作必须是 BEGIN")
    for op, _, _ in ops[1:]:
        if op == "BEGIN":
            raise SimError(f"{name}：BEGIN 只能出现一次且必须在开头")
    end = ops[-1][0]
    if end not in ("COMMIT", "ABORT"):
        raise SimError(f"{name}：最后一条操作必须是 COMMIT 或 ABORT")
    for i, (op, _, _) in enumerate(ops):
        if op in ("COMMIT", "ABORT") and i != len(ops) - 1:
            raise SimError(f"{name}：{op} 后不能再出现操作")
    return ops


def _parse_value(token: str, name: str, lineno: int) -> Any:
    low = token.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    if low in ("null", "none"):
        return None
    try:
        return int(token)
    except ValueError:
        pass
    try:
        return float(token)
    except ValueError:
        pass
    return token  # 其余视为字符串


def parse_initial(text: str) -> OrderedDict:
    """每行 key=value，# 开头注释，空行跳过。"""
    data: "OrderedDict[str, Any]" = OrderedDict()
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise SimError(f"初始键值第 {lineno} 行：格式应为 key=value（{line}）")
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        if not key:
            raise SimError(f"初始键值第 {lineno} 行：键不能为空")
        if key in data:
            raise SimError(f"初始键值第 {lineno} 行：键 {key} 重复定义")
        data[key] = _parse_value(val, "初始键值", lineno)
    return data


# ---------------------------------------------------------------------------
# 调度与执行
# ---------------------------------------------------------------------------


def build_schedule(txns: List[Txn]) -> List[Tuple[str, Tuple[str, Optional[str], Optional[Any]]]]:
    """轮转调度：按事务顺序循环，每轮从仍有剩余操作的事务队头取一条。"""
    alive = {t.name for t in txns}
    schedule: List[Tuple[str, Tuple[str, Optional[str], Optional[Any]]]] = []
    while alive:
        progressed = False
        for t in txns:
            if t.name not in alive:
                continue
            if t.pc < len(t.ops):
                schedule.append((t.name, t.ops[t.pc]))
                t.pc += 1
                progressed = True
                if t.pc >= len(t.ops):
                    alive.discard(t.name)
        if not progressed:
            break
    return schedule


def parse_order(raw: Any) -> List[str]:
    """解析自定义执行顺序：分隔字符串（空格/逗号/顿号等）或事务名数组。

    返回大写事务名列表；具体合法性（未知事务、次数与脚本操作数不符）
    由 run_compare 校验。
    """
    if isinstance(raw, str):
        text = raw
        for ch in (",", "，", "、", ";", "；", "|", "/"):
            text = text.replace(ch, " ")
        tokens = text.split()
    elif isinstance(raw, (list, tuple)):
        tokens = []
        for item in raw:
            tok = str(item).strip()
            if tok:
                tokens.append(tok)
    else:
        raise SimError("执行顺序格式错误：应为分隔字符串或事务名数组")
    order = [t.upper() for t in tokens]
    if not order:
        raise SimError("执行顺序不能为空")
    return order


def build_custom_schedule(
    txns: List[Txn],
    order: List[str],
) -> List[Tuple[str, Tuple[str, Optional[str], Optional[Any]]]]:
    """自定义顺序调度：按给定事务名序列，每次取对应事务尚未执行的队头操作。

    未知事务与次数校验已在 run_compare 中完成，这里保留防御性检查。
    """
    by_name = {t.name: t for t in txns}
    schedule: List[Tuple[str, Tuple[str, Optional[str], Optional[Any]]]] = []
    for i, name in enumerate(order, start=1):
        t = by_name.get(name)
        if t is None:
            raise SimError(f"执行顺序第 {i} 项：未知事务 “{name}”")
        if t.pc >= len(t.ops):
            raise SimError(f"执行顺序第 {i} 项：{name} 的操作已全部取完")
        schedule.append((t.name, t.ops[t.pc]))
        t.pc += 1
    return schedule


def _initial_version(vid: int, key: str, value: Any) -> Version:
    return Version(vid=vid, key=key, value=value, kind="initial",
                   writer="INIT", wseq=None, cseq=0)


def _snapshot(chains: Dict[str, List[Version]]) -> Dict[str, Version]:
    # “最新已提交”按提交顺序（cseq）判定，而非版本链的写入顺序：
    # 先写的事务可以更晚提交，晚提交者才是当前已提交版本。
    snap: Dict[str, Version] = {}
    for key, chain in chains.items():
        best: Optional[Version] = None
        for v in chain:
            if v.kind not in ("committed", "initial"):
                continue
            if best is None or (v.cseq, v.wseq or -1) > (best.cseq, best.wseq or -1):
                best = v
        if best is not None:
            snap[key] = best
    return snap


def _find_version(chains: Dict[str, List[Version]], key: str, vid: Optional[int]) -> Optional[Version]:
    if vid is None:
        return None
    for v in chains.get(key, []):
        if v.vid == vid:
            return v
    return None


def _own_latest(t: Txn, chains: Dict[str, List[Version]], key: str) -> Optional[Version]:
    for wkey, wseq in reversed(t.writes):
        if wkey == key:
            for v in chains.get(key, []):
                if v.wseq == wseq:
                    return v
    return None


def _ru_latest_visible(chains: Dict[str, List[Version]], key: str, current_txn: str,
                       txns: Dict[str, Txn]) -> Optional[Version]:
    """RU：取最新写入；已回滚事务写的版本对任何人不可见（回滚后即失效）。"""
    for v in reversed(chains.get(key, [])):
        if v.kind in ("committed", "initial"):
            return v
        if v.kind == "uncommitted":
            return v
        # aborted：继续向旧版本找
    return None


def _rc_latest(chains: Dict[str, List[Version]], key: str) -> Optional[Version]:
    """RC：最新已提交版本——按提交顺序（cseq）取，而非按写入顺序取链尾。

    同键交错提交（先写者晚提交）时，晚提交的版本即使 vid 较小也是最新；
    未提交（含本事务自己写的）与已回滚版本一律不可见。
    """
    best: Optional[Version] = None
    for v in chains.get(key, []):
        if v.kind not in ("committed", "initial"):
            continue
        if best is None or (v.cseq, v.wseq or -1) > (best.cseq, best.wseq or -1):
            best = v
    return best


def _si_read(t: Txn, chains: Dict[str, List[Version]], key: str) -> Tuple[Optional[Version], str]:
    """SI：BEGIN 时快照，自身写入优先。返回 (版本, 来源类型)。"""
    own = _own_latest(t, chains, key)
    if own is not None:
        return own, "own"
    if key in t.snapshot:
        return t.snapshot[key], "snapshot"
    return None, "missing"


def _chain_dicts(chains: Dict[str, List[Version]]) -> "OrderedDict[str, List[dict]]":
    out: "OrderedDict[str, List[dict]]" = OrderedDict()
    for key, chain in chains.items():
        out[key] = [
            {"vid": v.vid, "value": v.value, "kind": v.kind, "writer": v.writer,
             "wseq": v.wseq}
            for v in chain
        ]
    return out


def _snapshot_dict(t: Txn) -> Dict[str, dict]:
    return {
        k: {"vid": v.vid, "value": v.value, "writer": v.writer, "wseq": v.wseq}
        for k, v in t.snapshot.items()
    }


def simulate(mode: str,
             initial: "OrderedDict[str, Any]",
             txn_scripts: List[Tuple[str, str]],
             order: Optional[List[str]] = None) -> dict:
    """对单个隔离级别执行完整推演。

    order 为 None 时使用默认轮转调度；否则按给定事务名顺序，
    每次取对应事务尚未执行的队头操作。
    """
    if mode not in ("RU", "RC", "SI"):
        raise SimError(f"未知隔离级别 {mode}")

    txns: Dict[str, Txn] = OrderedDict()
    for name, text in txn_scripts:
        txns[name] = Txn(name=name, ops=parse_txn(name, text))
    txn_list = list(txns.values())

    if order is None:
        schedule = build_schedule(txn_list)
    else:
        schedule = build_custom_schedule(txn_list, order)

    # 重置调度游标造成的状态：Txn 刚构造时 pc 已被 build_schedule 推到末尾，
    # 重新初始化一个干净的执行环境。
    for t in txn_list:
        t.pc = 0
        t.status = "pending"

    # 版本链
    chains: Dict[str, List[Version]] = OrderedDict()
    next_vid = 1
    for key, value in initial.items():
        chains[key] = [_initial_version(next_vid, key, value)]
        next_vid += 1

    steps: List[dict] = []
    dirty_reads: List[dict] = []
    seq = 0

    for txn_name, (op, key, value) in schedule:
        t = txns[txn_name]
        seq += 1

        step: Dict[str, Any] = {
            "seq": seq,
            "txn": txn_name,
            "op": op,
            "key": key,
            "value": value,
            "read_vid": None,
            "read_value": None,
            "read_source": None,   # initial | committed | dirty | snapshot | own | missing
            "read_writer": None,
            "read_wseq": None,
            "new_vid": None,
            "txn_status_before": t.status,
            "txn_status_after": t.status,
            "snapshot": None,
            "conflict_key": None,
            "conflict_vid": None,
            "info": "",
            "dirty_candidate": False,
        }
        t.pc += 1

        if op == "BEGIN":
            if t.status != "pending":
                raise SimError(f"{txn_name}：重复 BEGIN")
            t.status = "active"
            t.begin_seq = seq
            if mode == "SI":
                t.snapshot = _snapshot(chains)
                step["snapshot"] = _snapshot_dict(t)
            step["txn_status_after"] = "active"
            if mode == "SI":
                snap_desc = ", ".join(
                    f"{k}={vv['value']}" for k, vv in step["snapshot"].items()
                ) or "空"
                step["info"] = f"{txn_name} 开始，建立快照：{snap_desc}"
            else:
                level_name = {"RU": "Read Uncommitted", "RC": "Read Committed"}[mode]
                step["info"] = f"{txn_name} 开始（{level_name}，无固定快照）"

        elif op == "READ":
            assert key is not None
            chains.setdefault(key, [])
            if t.status != "active":
                raise SimError(f"{txn_name} 第 {t.pc} 条：事务已结束，不能再 READ")

            chosen: Optional[Version] = None
            source = "missing"
            if mode == "RU":
                chosen = _ru_latest_visible(chains, key, txn_name, txns)
                if chosen is None:
                    source = "missing"
                elif chosen.kind in ("committed", "initial"):
                    source = "initial" if chosen.kind == "initial" else "committed"
                else:
                    source = "dirty"
            elif mode == "RC":
                chosen = _rc_latest(chains, key)
                if chosen is None:
                    source = "missing"
                else:
                    source = "initial" if chosen.kind == "initial" else "committed"
            else:  # SI
                chosen, source = _si_read(t, chains, key)

            step["read_source"] = source
            if chosen is not None:
                step["read_vid"] = chosen.vid
                step["read_value"] = chosen.value
                step["read_writer"] = chosen.writer
                step["read_wseq"] = chosen.wseq
            t.reads.append((key, seq, chosen.vid if chosen else None))

            if source == "dirty":
                step["dirty_candidate"] = True
                t.dirty_hits.append(
                    (key, seq, chosen.vid, chosen.writer, chosen.value)
                )
                step["info"] = (
                    f"读到 {chosen.writer} 尚未提交的写入 v{chosen.vid}="
                    f"{_fmt(chosen.value)}（脏读，若 {chosen.writer} 回滚则列入脏读清单）"
                )
            elif source == "initial":
                step["info"] = f"读到初始值 v{chosen.vid}={_fmt(chosen.value)}"
            elif source == "committed":
                step["info"] = (
                    f"读到 {chosen.writer} 已提交的版本 v{chosen.vid}="
                    f"{_fmt(chosen.value)}"
                )
            elif source == "snapshot":
                newer = [
                    v for v in chains[key]
                    if v.kind == "committed" and v is not chosen
                    and v.cseq > chosen.cseq
                ]
                extra = ""
                if newer:
                    latest = newer[-1]
                    extra = (
                        f"；键 {key} 在快照后已有 {latest.writer} 提交的新版本 "
                        f"v{latest.vid}={_fmt(latest.value)}，SI 仍读旧版本"
                    )
                tag = "初始版本" if chosen.kind == "initial" else f"{chosen.writer} 的已提交版本"
                step["info"] = (
                    f"读快照中的{tag} v{chosen.vid}={_fmt(chosen.value)}{extra}"
                )
            elif source == "own":
                step["info"] = (
                    f"读到本事务自己的写入 v{chosen.vid}={_fmt(chosen.value)}（自身写入优先）"
                )
            else:
                step["info"] = f"键 {key} 在可读范围内不存在，读得 ∅"

        elif op == "WRITE":
            assert key is not None
            chains.setdefault(key, [])
            if t.status != "active":
                raise SimError(f"{txn_name} 第 {t.pc} 条：事务已结束，不能再 WRITE")
            v = Version(vid=next_vid, key=key, value=value, kind="uncommitted",
                        writer=txn_name, wseq=seq)
            next_vid += 1
            chains[key].append(v)
            t.writes.append((key, seq))
            step["new_vid"] = v.vid
            step["info"] = f"写入新版本 v{v.vid}={_fmt(value)}（未提交）"

        elif op == "COMMIT":
            if t.status != "active":
                raise SimError(f"{txn_name}：事务已结束，不能再 COMMIT")
            t.commit_seq = seq

            if mode == "SI":
                # 首提交者胜：快照之后，若本事务写过的键已有其他事务提交新版本，
                # 则整笔回滚。按提交顺序（cseq）判定：先写晚提交者同样会冲突；
                # 快照中不存在的键视为初始版本（cseq=0）。
                conflict = None
                for wkey, _wseq in t.writes:
                    snap_cseq = t.snapshot[wkey].cseq if wkey in t.snapshot else 0
                    for v in chains[wkey]:
                        if v.kind == "committed" and v.cseq > snap_cseq:
                            conflict = (wkey, v)
                            break
                    if conflict:
                        break
                if conflict:
                    ckey, cv = conflict
                    step["conflict_key"] = ckey
                    step["conflict_vid"] = cv.vid
                    t.status = "aborted"
                    t.abort_seq = seq
                    for wkey, wseq in t.writes:
                        for v in chains[wkey]:
                            if v.wseq == wseq:
                                v.kind = "aborted"
                    step["txn_status_after"] = "aborted"
                    step["info"] = (
                        f"提交被拒：快照之后键 {ckey} 已由 {cv.writer} 提交新版本 "
                        f"v{cv.vid}={_fmt(cv.value)}，首提交者胜 → 整笔回滚，"
                        f"{len(t.writes)} 个写入全部作废"
                    )
                else:
                    t.status = "committed"
                    for wkey, wseq in t.writes:
                        for v in chains[wkey]:
                            if v.wseq == wseq:
                                v.kind = "committed"
                                v.cseq = seq
                    step["txn_status_after"] = "committed"
                    step["info"] = f"提交成功，{len(t.writes)} 个写入转为已提交版本"
            else:
                t.status = "committed"
                for wkey, wseq in t.writes:
                    for v in chains[wkey]:
                        if v.wseq == wseq:
                            v.kind = "committed"
                            v.cseq = seq
                step["txn_status_after"] = "committed"
                step["info"] = f"提交成功，{len(t.writes)} 个写入转为已提交版本"

            # 结算此前所有事务中“读到本事务未提交版本”的脏读候选
            for other in txn_list:
                if other.name == txn_name:
                    continue
                remaining = []
                for rkey, rseq, rvid, rwriter, rval in other.dirty_hits:
                    if rwriter == txn_name:
                        if t.status == "aborted":
                            dirty_reads.append({
                                "key": rkey,
                                "reader": other.name,
                                "writer": txn_name,
                                "read_seq": rseq,
                                "end_seq": seq,
                                "vid": rvid,
                                "value": rval,
                                "reason": "writer-aborted",
                                "detail": (
                                    f"{other.name} 在第 {rseq} 步读到 {txn_name} "
                                    f"未提交的 {rkey}={_fmt(rval)}，"
                                    f"{txn_name} 于第 {seq} 步回滚"
                                ),
                            })
                        # 若写者提交，则不再是脏读（RU 下读到后来提交的值）
                    else:
                        remaining.append((rkey, rseq, rvid, rwriter, rval))
                other.dirty_hits = remaining

        else:  # ABORT
            if t.status != "active":
                raise SimError(f"{txn_name}：事务已结束，不能再 ABORT")
            t.status = "aborted"
            t.abort_seq = seq
            for wkey, wseq in t.writes:
                for v in chains[wkey]:
                    if v.wseq == wseq:
                        v.kind = "aborted"
            step["txn_status_after"] = "aborted"
            step["info"] = f"主动回滚，{len(t.writes)} 个写入全部作废"

            for other in txn_list:
                if other.name == txn_name:
                    continue
                remaining = []
                for rkey, rseq, rvid, rwriter, rval in other.dirty_hits:
                    if rwriter == txn_name:
                        dirty_reads.append({
                            "key": rkey,
                            "reader": other.name,
                            "writer": txn_name,
                            "read_seq": rseq,
                            "end_seq": seq,
                            "vid": rvid,
                            "value": rval,
                            "reason": "writer-aborted",
                            "detail": (
                                f"{other.name} 在第 {rseq} 步读到 {txn_name} "
                                f"未提交的 {rkey}={_fmt(rval)}，"
                                f"{txn_name} 于第 {seq} 步回滚"
                            ),
                        })
                    else:
                        remaining.append((rkey, rseq, rvid, rwriter, rval))
                other.dirty_hits = remaining

        step["txn_status_after"] = t.status
        if mode == "SI" and t.status == "active" and op != "BEGIN":
            step["snapshot"] = _snapshot_dict(t)
        step["chains_after"] = _chain_dicts(chains)
        step["txns_after"] = {
            x.name: {
                "status": x.status,
                "begin_seq": x.begin_seq,
                "commit_seq": x.commit_seq,
                "abort_seq": x.abort_seq,
                "writes": [{"key": k, "wseq": s} for k, s in x.writes],
            }
            for x in txn_list
        }
        steps.append(step)

    # 残余脏读候选（写者既未提交也未回滚——正常脚本不会发生，防御性处理）
    for t in txn_list:
        for rkey, rseq, rvid, rwriter, rval in t.dirty_hits:
            dirty_reads.append({
                "key": rkey,
                "reader": t.name,
                "writer": rwriter,
                "read_seq": rseq,
                "end_seq": seq,
                "vid": rvid,
                "value": rval,
                "reason": "unresolved",
                "detail": f"{t.name} 读到 {rwriter} 未提交的 {rkey}={_fmt(rval)}，脚本结束仍无定论",
            })

    graph = _build_graph(txn_list, chains)
    _annotate_cycles(graph, steps)

    return {
        "mode": mode,
        "schedule": [
            {"txn": name, "op": op, "key": k, "value": v}
            for name, (op, k, v) in schedule
        ],
        "steps": steps,
        "dirty_reads": dirty_reads,
        "graph": graph,
        "txn_final": {
            t.name: {
                "status": t.status,
                "begin_seq": t.begin_seq,
                "commit_seq": t.commit_seq,
                "abort_seq": t.abort_seq,
            }
            for t in txn_list
        },
    }


# ---------------------------------------------------------------------------
# 依赖图（只统计已提交事务的真实读写）
# ---------------------------------------------------------------------------


def _build_graph(txn_list: List[Txn], chains: Dict[str, List[Version]]) -> dict:
    committed = {t.name for t in txn_list if t.status == "committed"}
    by_name = {t.name: t for t in txn_list}
    edges: List[dict] = []
    seen = set()

    def add_edge(kind: str, frm: str, to: str, key: str,
                 read_seq: Optional[int], write_seq: int):
        ekey = (kind, frm, to, key, read_seq, write_seq)
        if ekey in seen:
            return
        seen.add(ekey)
        edges.append({
            "kind": kind, "from": frm, "to": to, "key": key,
            "read_seq": read_seq, "write_seq": write_seq,
            "in_cycle": False,
        })

    for t in txn_list:
        if t.name not in committed:
            continue
        for key, rseq, rvid in t.reads:
            rv = _find_version(chains, key, rvid)
            if rv is None or rv.kind not in ("committed", "initial"):
                continue
            # WR（读己之写不算）：读到别人已提交版本
            if rv.writer != "INIT" and rv.writer != t.name and rv.writer in committed:
                add_edge("WR", rv.writer, t.name, key, rseq, rv.wseq)
            # RW 反依赖：该读发生之后，同一键才有别的已提交事务提交的新版本
            # （按提交序号 cseq 判定，而非版本编号 vid）。版本链按写入顺序排列，
            # 若写入者虽在链上更“新”却在读之前就已提交（先写晚提交场景中
            # 编号更大的版本可能先提交），则不构成读后写反依赖。
            for v in chains.get(key, []):
                if (v.kind == "committed" and v.writer in committed
                        and v.writer != t.name and v.cseq > rseq):
                    add_edge("RW", t.name, v.writer, key, rseq, v.wseq)

    # WW：同一键上先后写入并都提交的两个事务（按版本链顺序）
    for key, chain in chains.items():
        committed_writes = [
            v for v in chain
            if v.kind == "committed" and v.writer in committed
        ]
        for a, b in zip(committed_writes, committed_writes[1:]):
            if a.writer != b.writer:
                add_edge("WW", a.writer, b.writer, key, None, b.wseq)

    edges.sort(key=lambda e: ({"WR": 0, "WW": 1, "RW": 2}[e["kind"]],
                              e["from"], e["to"], e["key"]))
    return {
        "nodes": [{"name": t.name, "status": t.status} for t in txn_list
                  if t.name in committed],
        "nodes_all": [{"name": t.name, "status": t.status} for t in txn_list],
        "edges": edges,
        "has_cycle": False,
        "cycles": [],
    }


def _annotate_cycles(graph: dict, steps: List[dict]) -> None:
    nodes = [n["name"] for n in graph["nodes_all"]]
    # 环分析在已提交节点 + 相关边上进行（回滚事务没有任何依赖边）
    committed_nodes = {n["name"] for n in graph["nodes"]}
    adj: Dict[str, List[int]] = {n: [] for n in nodes}
    for i, e in enumerate(graph["edges"]):
        adj.setdefault(e["from"], []).append(i)

    # Tarjan 强连通分量
    index = {}
    low = {}
    onstack = set()
    stack: List[str] = []
    counter = [0]
    sccs: List[List[str]] = []

    def strongconnect(v: str) -> None:
        index[v] = counter[0]
        low[v] = counter[0]
        counter[0] += 1
        stack.append(v)
        onstack.add(v)
        for ei in adj.get(v, []):
            w = graph["edges"][ei]["to"]
            if w not in index:
                strongconnect(w)
                low[v] = min(low[v], low[w])
            elif w in onstack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            comp = []
            while True:
                w = stack.pop()
                onstack.discard(w)
                comp.append(w)
                if w == v:
                    break
            sccs.append(comp)

    import sys
    old_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(max(old_limit, 10000))
    try:
        for n in nodes:
            if n not in index:
                strongconnect(n)
    finally:
        sys.setrecursionlimit(old_limit)

    cycles: List[List[str]] = []
    cycle_nodes = set()
    for comp in sccs:
        if len(comp) > 1 and any(n in committed_nodes for n in comp):
            path = _cycle_path(comp, graph)
            if path:
                cycles.append(path)
                cycle_nodes.update(path)

    if not cycles:
        return

    graph["has_cycle"] = True
    graph["cycles"] = [{"nodes": c, "label": " → ".join(c + [c[0]])} for c in cycles]

    cycle_edges = set()
    for c in cycles:
        for a, b in zip(c, c[1:] + c[:1]):
            cycle_edges.add((a, b))
    for e in graph["edges"]:
        if (e["from"], e["to"]) in cycle_edges:
            e["in_cycle"] = True

    # 在步骤上标注需要高亮的操作
    edge_by_write: Dict[Tuple[str, str], List[dict]] = {}
    for e in graph["edges"]:
        if e["in_cycle"]:
            edge_by_write.setdefault((e["to"], e["write_seq"]), []).append(e)
    for step in steps:
        seq = step["seq"]
        tags = []
        for e in graph["edges"]:
            if not e["in_cycle"]:
                continue
            if e["write_seq"] == seq:
                tags.append(f"环上{e['kind']}边的写操作")
            if e.get("read_seq") == seq:
                tags.append(f"环上{e['kind']}边的读操作")
        if tags:
            step["in_cycle"] = True
            step["cycle_tags"] = tags
        else:
            step.setdefault("in_cycle", False)
            step.setdefault("cycle_tags", [])


def _cycle_path(component: List[str], graph: dict) -> List[str]:
    """在一个强连通分量里找一条具体的环路径。"""
    comp = set(component)
    start = sorted(component)[0]
    path = [start]
    visited_edges = set()

    def dfs(u: str) -> bool:
        for e in graph["edges"]:
            if e["from"] != u or e["to"] not in comp:
                continue
            eid = (e["from"], e["to"], e["key"], e.get("read_seq"), e["write_seq"])
            if eid in visited_edges:
                continue
            visited_edges.add(eid)
            if e["to"] == start and len(path) >= 2:
                return True
            if e["to"] not in path:
                path.append(e["to"])
                if dfs(e["to"]):
                    return True
                path.pop()
        return False

    if dfs(start):
        return path
    return sorted(component)


# ---------------------------------------------------------------------------
# 杂项
# ---------------------------------------------------------------------------


def _fmt(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def run_all(initial_text: str, txn_texts: List[str]) -> dict:
    initial = parse_initial(initial_text)
    if not 2 <= len(txn_texts) <= 3:
        raise SimError("需要 2~3 条事务脚本")
    txn_scripts = [(f"T{i+1}", text) for i, text in enumerate(txn_texts)]
    names = [n for n, _ in txn_scripts]
    if len(set(names)) != len(names):
        raise SimError("事务名重复")
    return {
        "initial": {k: v for k, v in initial.items()},
        "results": {mode: simulate(mode, initial, txn_scripts)
                    for mode in ("RU", "RC", "SI")},
    }


# ---------------------------------------------------------------------------
# 调度对照：默认轮转 vs 自定义顺序
# ---------------------------------------------------------------------------


def _align_keys(schedule: List[dict]) -> List[Tuple[str, int]]:
    """每个调度步的对齐键：（事务名，事务内操作序号），序号从 1 开始。"""
    counts: Dict[str, int] = {}
    keys: List[Tuple[str, int]] = []
    for item in schedule:
        name = item["txn"]
        counts[name] = counts.get(name, 0) + 1
        keys.append((name, counts[name]))
    return keys


def _diff_results(default_res: dict, custom_res: dict) -> dict:
    """按（事务名，事务内操作序号）对齐 READ / COMMIT / ABORT，
    依默认轮转的步序比较读值、来源类型、写入者与结束状态。

    两侧调度包含完全相同的多重操作集合（仅顺序不同），对齐键一一对应；
    各字段均取各侧实际运行结果——例如 SI 因顺序变化回滚后，
    后续 READ 的读值天然就是该侧真实推演出的值。
    """
    cindex = {k: i for i, k in enumerate(_align_keys(custom_res["schedule"]))}

    diffs: List[dict] = []
    for i, akey in enumerate(_align_keys(default_res["schedule"])):
        dstep = default_res["steps"][i]
        op = dstep["op"]
        if op not in ("READ", "COMMIT", "ABORT"):
            continue
        cstep = custom_res["steps"][cindex[akey]]
        fields: List[dict] = []
        if op == "READ":
            for f, label in (("read_value", "读值"),
                             ("read_source", "来源类型"),
                             ("read_writer", "写入者")):
                if dstep[f] != cstep[f]:
                    fields.append({"field": f, "label": label,
                                   "default": dstep[f], "custom": cstep[f]})
        elif dstep["txn_status_after"] != cstep["txn_status_after"]:
            fields.append({"field": "txn_status_after", "label": "结束状态",
                           "default": dstep["txn_status_after"],
                           "custom": cstep["txn_status_after"]})
        if fields:
            diffs.append({
                "txn": akey[0],
                "op_index": akey[1],
                "op": op,
                "key": dstep["key"],
                "default_seq": dstep["seq"],
                "custom_seq": cstep["seq"],
                "default_read_vid": dstep["read_vid"],
                "custom_read_vid": cstep["read_vid"],
                "fields": fields,
            })
    return {
        "identical": not diffs,
        "diff_count": len(diffs),
        "first_diff": diffs[0] if diffs else None,
        "diffs": diffs,
    }


def _side_summary(res: dict) -> dict:
    return {
        "dirty_count": len(res["dirty_reads"]),
        "has_cycle": res["graph"]["has_cycle"],
        "cycles": [c["label"] for c in res["graph"]["cycles"]],
    }


def run_compare(initial_text: str, txn_texts: List[str], order_raw: Any) -> dict:
    """同一组事务脚本的调度对照：默认轮转 vs 自定义顺序。

    两侧各自独立运行 RU / RC / SI，再按（事务名，事务内操作序号）
    对齐 READ / COMMIT / ABORT，依默认轮转的步序找出读值、来源类型、
    写入者或结束状态的首次差异。
    """
    initial = parse_initial(initial_text)
    if not 2 <= len(txn_texts) <= 3:
        raise SimError("需要 2~3 条事务脚本")
    txn_scripts = [(f"T{i+1}", text) for i, text in enumerate(txn_texts)]
    names = [n for n, _ in txn_scripts]
    if len(set(names)) != len(names):
        raise SimError("事务名重复")

    # 先校验脚本语法，再校验顺序：未知事务、各事务出现次数与脚本操作数不符
    parsed = {n: parse_txn(n, text) for n, text in txn_scripts}
    order = parse_order(order_raw)
    counts = {n: 0 for n in names}
    for i, name in enumerate(order, start=1):
        if name not in counts:
            raise SimError(
                f"执行顺序第 {i} 项：未知事务 “{name}”，当前只有 {'、'.join(names)}"
            )
        counts[name] += 1
    mismatches = [
        f"{n} 出现 {counts[n]} 次，但脚本有 {len(parsed[n])} 条操作"
        for n in names
        if counts[n] != len(parsed[n])
    ]
    if mismatches:
        raise SimError("执行顺序与脚本操作数不符：" + "；".join(mismatches))

    default_results = {m: simulate(m, initial, txn_scripts)
                       for m in ("RU", "RC", "SI")}
    custom_results = {m: simulate(m, initial, txn_scripts, order)
                      for m in ("RU", "RC", "SI")}

    compare = {}
    for m in ("RU", "RC", "SI"):
        entry = _diff_results(default_results[m], custom_results[m])
        entry["summary"] = {
            "default": _side_summary(default_results[m]),
            "custom": _side_summary(custom_results[m]),
        }
        compare[m] = entry

    return {
        "initial": {k: v for k, v in initial.items()},
        "order": order,
        "sides": {
            "default": {"label": "默认轮转", "results": default_results},
            "custom": {"label": "自定义顺序", "results": custom_results},
        },
        "compare": compare,
    }
