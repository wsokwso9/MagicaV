#!/usr/bin/env python3
"""
MagicaV — BeyondFinance companion and simulator.

This tool is a local, file-backed helper for the BeyondFinance onchain platform:
- Tracks offchain simulations of vault deposits / withdrawals and credit lines.
- Can mirror key configuration fields from an onchain deployment (optional).
- Provides CLI commands to explore, simulate, and export state.

All data is stored in a JSON state file; no database is required.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import os
import random
import sys
import textwrap
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APP_NAME = "MagicaV"
APP_VERSION = "1.0.0"
DEFAULT_STATE_FILE = "magica_v_state.json"
DEFAULT_CONFIG_FILE = "magica_v_config.json"

BFIN_BPS_BASE = 10_000
BFIN_MAX_MANAGEMENT_FEE_BPS = 700
BFIN_MAX_WITHDRAWAL_FEE_BPS = 350
BFIN_MAX_PROTOCOL_FEE_BPS = 1500
BFIN_MAX_RATE_BPS = 3_000
BFIN_MAX_VAULTS = 96
BFIN_MAX_LINES = 128


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def fmt_wei(wei: float) -> str:
    try:
        return f"{wei:.0f} wei"
    except Exception:
        return str(wei)


def fmt_eth(wei: float) -> str:
    try:
        return f"{wei / 1e18:.6f} ETH"
    except Exception:
        return str(wei)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def rand_hex(n: int) -> str:
    alphabet = "0123456789abcdef"
    return "".join(random.choice(alphabet) for _ in range(n))


def truncate(addr: str, head: int = 6, tail: int = 4) -> str:
    if not addr or len(addr) <= head + tail + 2:
        return addr
    if addr.startswith("0x"):
        return f"{addr[: head + 2]}…{addr[-tail:]}"
    return f"{addr[:head]}…{addr[-tail:]}"


def wrap(text: str, width: int = 78, indent: str = "") -> str:
    return "\n".join(indent + line for line in textwrap.wrap(text, width))


def percent(bps: int) -> str:
    return f"{bps / BFIN_BPS_BASE * 100:.2f}%"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class VaultSim:
    vault_id: int
    name: str
    asset_symbol: str
    deposit_cap_wei: float
    management_fee_bps: int
    withdrawal_fee_bps: int
    protocol_fee_bps: int
    enabled: bool = True
    total_assets_wei: float = 0.0
    total_shares: float = 0.0
    last_accrual_block: int = 0
    strategy_hint: str = ""
    created_at: str = field(default_factory=now_iso)


@dataclass
class VaultPosition:
    vault_id: int
    owner: str
    shares: float
    last_deposit_block: int


@dataclass
class LineSim:
    line_id: int
    borrower: str
    asset_symbol: str
    limit_wei: float
    rate_bps: int
    borrowed_wei: float = 0.0
    last_accrual_block: int = 0
    frozen: bool = False
    created_at: str = field(default_factory=now_iso)


@dataclass
class TagRecord:
    address: str
    tags_hash: str
    note: str = ""


@dataclass
class MagicaState:
    current_block: int = 0
    next_vault_id: int = 1
    next_line_id: int = 1
    vaults: Dict[int, VaultSim] = field(default_factory=dict)
    vault_positions: Dict[Tuple[int, str], VaultPosition] = field(default_factory=dict)
    lines: Dict[int, LineSim] = field(default_factory=dict)
    tags: Dict[str, TagRecord] = field(default_factory=dict)
    fee_collector: str = field(default_factory=lambda: "0x" + rand_hex(40))
    guardian: str = field(default_factory=lambda: "0x" + rand_hex(40))
    risk_council: str = field(default_factory=lambda: "0x" + rand_hex(40))
    protocol_fee_wei: float = 0.0


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def vault_to_dict(v: VaultSim) -> Dict[str, Any]:
    return dataclasses.asdict(v)


def vault_from_dict(d: Dict[str, Any]) -> VaultSim:
    return VaultSim(**d)


def pos_to_dict(p: VaultPosition) -> Dict[str, Any]:
    return dataclasses.asdict(p)


def pos_from_dict(d: Dict[str, Any]) -> VaultPosition:
    return VaultPosition(**d)


def line_to_dict(l: LineSim) -> Dict[str, Any]:
    return dataclasses.asdict(l)


def line_from_dict(d: Dict[str, Any]) -> LineSim:
    return LineSim(**d)


def tag_to_dict(t: TagRecord) -> Dict[str, Any]:
    return dataclasses.asdict(t)


def tag_from_dict(d: Dict[str, Any]) -> TagRecord:
    return TagRecord(**d)


def state_to_dict(s: MagicaState) -> Dict[str, Any]:
    return {
        "current_block": s.current_block,
        "next_vault_id": s.next_vault_id,
        "next_line_id": s.next_line_id,
        "vaults": {str(k): vault_to_dict(v) for k, v in s.vaults.items()},
        "vault_positions": {
            f"{vid}:{addr}": pos_to_dict(p)
            for (vid, addr), p in s.vault_positions.items()
        },
        "lines": {str(k): line_to_dict(v) for k, v in s.lines.items()},
        "tags": {addr: tag_to_dict(t) for addr, t in s.tags.items()},
        "fee_collector": s.fee_collector,
        "guardian": s.guardian,
        "risk_council": s.risk_council,
        "protocol_fee_wei": s.protocol_fee_wei,
    }


def state_from_dict(d: Dict[str, Any]) -> MagicaState:
    s = MagicaState()
    s.current_block = int(d.get("current_block", 0))
    s.next_vault_id = int(d.get("next_vault_id", 1))
    s.next_line_id = int(d.get("next_line_id", 1))
    s.fee_collector = d.get("fee_collector", s.fee_collector)
    s.guardian = d.get("guardian", s.guardian)
    s.risk_council = d.get("risk_council", s.risk_council)
    s.protocol_fee_wei = float(d.get("protocol_fee_wei", 0.0))

    for k, v in d.get("vaults", {}).items():
        s.vaults[int(k)] = vault_from_dict(v)

    for k, v in d.get("vault_positions", {}).items():
        vid_str, addr = k.split(":", 1)
        s.vault_positions[(int(vid_str), addr)] = pos_from_dict(v)

    for k, v in d.get("lines", {}).items():
        s.lines[int(k)] = line_from_dict(v)

    for addr, v in d.get("tags", {}).items():
        s.tags[addr] = tag_from_dict(v)

    return s


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------


def state_path(custom: Optional[str] = None) -> Path:
    if custom:
        return Path(custom)
    env = os.environ.get("MAGICAV_STATE_FILE")
    if env:
        return Path(env)
    return Path.cwd() / DEFAULT_STATE_FILE


def config_path(custom: Optional[str] = None) -> Path:
    if custom:
        return Path(custom)
    env = os.environ.get("MAGICAV_CONFIG_FILE")
    if env:
        return Path(env)
    return Path.cwd() / DEFAULT_CONFIG_FILE


def load_state(path: Optional[str] = None) -> MagicaState:
    p = state_path(path)
    if not p.exists():
        return MagicaState()
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return state_from_dict(data)
    except (OSError, json.JSONDecodeError):
        return MagicaState()


def save_state(state: MagicaState, path: Optional[str] = None) -> None:
    p = state_path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8") as f:
            json.dump(state_to_dict(state), f, indent=2)
    except OSError as exc:
        print(f"[WARN] failed to save state to {p}: {exc}", file=sys.stderr)


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    p = config_path(path)
    if not p.exists():
        return {}
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def save_config(cfg: Dict[str, Any], path: Optional[str] = None) -> None:
    p = config_path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
    except OSError as exc:
        print(f"[WARN] failed to save config to {p}: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Simulation primitives
# ---------------------------------------------------------------------------


def accrue_vault_fees(v: VaultSim, current_block: int) -> Tuple[VaultSim, float]:
    if v.management_fee_bps <= 0:
        v.last_accrual_block = current_block
        return v, 0.0
    if v.total_assets_wei <= 0.0 or v.total_shares <= 0.0:
        v.last_accrual_block = current_block
        return v, 0.0
    elapsed = max(0, current_block - v.last_accrual_block)
    if elapsed == 0:
        return v, 0.0
    annual_blocks = 15_768_000
    fraction = (v.management_fee_bps / BFIN_BPS_BASE) * (elapsed / annual_blocks)
    fee = v.total_assets_wei * fraction
    fee = max(0.0, min(fee, v.total_assets_wei))
    v.total_assets_wei -= fee
    v.last_accrual_block = current_block
    return v, fee


def convert_to_shares(v: VaultSim, assets_wei: float) -> float:
    if v.total_shares <= 0.0 or v.total_assets_wei <= 0.0:
        return assets_wei
    if assets_wei <= 0.0:
        return 0.0
    return assets_wei * (v.total_shares / v.total_assets_wei)


def convert_to_assets(v: VaultSim, shares: float) -> float:
    if v.total_shares <= 0.0 or v.total_assets_wei <= 0.0:
        return shares
    if shares <= 0.0:
        return 0.0
    return shares * (v.total_assets_wei / v.total_shares)


def accrue_line_interest(line: LineSim, current_block: int) -> Tuple[LineSim, float]:
    if line.rate_bps <= 0 or line.borrowed_wei <= 0.0:
        line.last_accrual_block = current_block
        return line, 0.0
    elapsed = max(0, current_block - line.last_accrual_block)
    if elapsed == 0:
        return line, 0.0
    annual_blocks = 15_768_000
    fraction = (line.rate_bps / BFIN_BPS_BASE) * (elapsed / annual_blocks)
    interest = line.borrowed_wei * fraction
    interest = max(0.0, interest)
    line.borrowed_wei += interest
    line.last_accrual_block = current_block
    return line, interest


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def vault_summary(v: VaultSim, state: MagicaState) -> str:
    positions = [
        p for (vid, _), p in state.vault_positions.items() if vid == v.vault_id
    ]
    lines = [
        f"Vault #{v.vault_id} — {v.name} ({v.asset_symbol})",
        f"  Enabled: {v.enabled}   Strategy: {v.strategy_hint or '(none)'}",
        f"  MgmtFee: {v.management_fee_bps} bps ({percent(v.management_fee_bps)}) "
        f"WithdrawFee: {v.withdrawal_fee_bps} bps ({percent(v.withdrawal_fee_bps)}) "
        f"ProtocolFee: {v.protocol_fee_bps} bps ({percent(v.protocol_fee_bps)})",
        f"  Total assets: {fmt_eth(v.total_assets_wei)}   Total shares: {v.total_shares:.6f}",
        f"  Deposit cap: {fmt_eth(v.deposit_cap_wei)}",
        f"  Positions: {len(positions)}   Last accrual block: {v.last_accrual_block}",
        f"  Created at: {v.created_at}",
    ]
    return "\n".join(lines)


def position_summary(p: VaultPosition, v: VaultSim) -> str:
    assets = convert_to_assets(v, p.shares)
    return (
        f"Vault #{p.vault_id} — {truncate(p.owner)}: "
        f"{p.shares:.6f} shares (~{fmt_eth(assets)}), lastDepositBlock={p.last_deposit_block}"
    )


def line_summary(l: LineSim) -> str:
    return (
        f"Line #{l.line_id} — {truncate(l.borrower)} in {l.asset_symbol}: "
        f"limit {fmt_eth(l.limit_wei)}, rate {l.rate_bps} bps, "
        f"borrowed {fmt_eth(l.borrowed_wei)}, frozen={l.frozen}, "
        f"createdAt={l.created_at}"
    )


def print_table(rows: List[List[str]]) -> str:
    if not rows:
        return ""
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    lines = []
    for idx, row in enumerate(rows):
        padded = "  ".join(row[i].ljust(widths[i]) for i in range(len(row)))
        lines.append(padded)
        if idx == 0:
            lines.append("  ".join("-" * w for w in widths))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------


def ensure_vault_exists(state: MagicaState, vault_id: int) -> None:
    if vault_id not in state.vaults:
        raise ValueError("vault not found")


def ensure_line_exists(state: MagicaState, line_id: int) -> None:
    if line_id not in state.lines:
        raise ValueError("line not found")


def open_vault(
    state: MagicaState,
    name: str,
    asset_symbol: str,
    deposit_cap_wei: float,
    management_fee_bps: int,
    withdrawal_fee_bps: int,
    protocol_fee_bps: int,
    strategy_hint: str,
    enabled: bool,
) -> Tuple[MagicaState, VaultSim]:
    if len(state.vaults) >= BFIN_MAX_VAULTS:
        raise ValueError("max vaults reached")
    vid = state.next_vault_id
    state.next_vault_id += 1
    management_fee_bps = int(clamp(management_fee_bps, 0, BFIN_MAX_MANAGEMENT_FEE_BPS))
    withdrawal_fee_bps = int(clamp(withdrawal_fee_bps, 0, BFIN_MAX_WITHDRAWAL_FEE_BPS))
    protocol_fee_bps = int(clamp(protocol_fee_bps, 0, BFIN_MAX_PROTOCOL_FEE_BPS))
    v = VaultSim(
        vault_id=vid,
        name=name,
        asset_symbol=asset_symbol,
        deposit_cap_wei=deposit_cap_wei,
        management_fee_bps=management_fee_bps,
        withdrawal_fee_bps=withdrawal_fee_bps,
        protocol_fee_bps=protocol_fee_bps,
        enabled=enabled,
        last_accrual_block=state.current_block,
        strategy_hint=strategy_hint,
    )
    state.vaults[vid] = v
    return state, v


def simulate_deposit(
    state: MagicaState,
    vault_id: int,
    owner: str,
    amount_wei: float,
) -> Tuple[MagicaState, str]:
    if amount_wei <= 0:
        return state, "amount must be > 0"
    ensure_vault_exists(state, vault_id)
    v = state.vaults[vault_id]
    if not v.enabled:
        return state, "vault disabled"
    v, fee = accrue_vault_fees(v, state.current_block)
    if v.deposit_cap_wei > 0 and v.total_assets_wei + amount_wei > v.deposit_cap_wei:
        return state, "deposit cap exceeded"
    shares = convert_to_shares(v, amount_wei)
    if shares <= 0:
        shares = amount_wei
    v.total_assets_wei += amount_wei
    v.total_shares += shares
    state.protocol_fee_wei += fee * (v.protocol_fee_bps / BFIN_BPS_BASE)
    pos_key = (vault_id, owner)
    pos = state.vault_positions.get(pos_key)
    if pos is None:
        pos = VaultPosition(vault_id=vault_id, owner=owner, shares=0.0, last_deposit_block=state.current_block)
    pos.shares += shares
    pos.last_deposit_block = state.current_block
    state.vault_positions[pos_key] = pos
    state.vaults[vault_id] = v
    msg = (
        f"deposit: vault {vault_id}, owner {truncate(owner)}, "
        f"assets={fmt_eth(amount_wei)}, shares={shares:.6f}, mgmtFee={fmt_eth(fee)}"
    )
    return state, msg


def simulate_withdraw(
    state: MagicaState,
    vault_id: int,
    owner: str,
    shares: float,
) -> Tuple[MagicaState, str]:
    if shares <= 0:
        return state, "shares must be > 0"
    ensure_vault_exists(state, vault_id)
    v = state.vaults[vault_id]
    pos_key = (vault_id, owner)
    pos = state.vault_positions.get(pos_key)
    if pos is None or pos.shares < shares:
        return state, "insufficient shares"
    v, fee = accrue_vault_fees(v, state.current_block)
    gross = convert_to_assets(v, shares)
    if gross <= 0 or gross > v.total_assets_wei:
        return state, "invalid withdrawal amount"
    w_fee = gross * (v.withdrawal_fee_bps / BFIN_BPS_BASE)
    p_fee = gross * (v.protocol_fee_bps / BFIN_BPS_BASE)
    total_fee = w_fee + p_fee + fee
    payout = gross - (w_fee + p_fee)
    v.total_assets_wei -= gross
    v.total_shares -= shares
    pos.shares -= shares
    if pos.shares <= 0:
        state.vault_positions.pop(pos_key, None)
    else:
        state.vault_positions[pos_key] = pos
    state.vaults[vault_id] = v
    state.protocol_fee_wei += total_fee
    msg = (
        f"withdraw: vault {vault_id}, owner {truncate(owner)}, "
        f"shares={shares:.6f}, payout={fmt_eth(payout)}, totalFee={fmt_eth(total_fee)}"
    )
    return state, msg


def simulate_harvest(state: MagicaState, vault_id: int, gain_wei: float) -> Tuple[MagicaState, str]:
    if gain_wei <= 0:
        return state, "gain must be > 0"
    ensure_vault_exists(state, vault_id)
    v = state.vaults[vault_id]
    protocol_cut = gain_wei * (v.protocol_fee_bps / BFIN_BPS_BASE)
    net_gain = gain_wei - protocol_cut
    v.total_assets_wei += net_gain
    state.protocol_fee_wei += protocol_cut
    state.vaults[vault_id] = v
    msg = f"harvest: vault {vault_id}, gain={fmt_eth(gain_wei)}, protocolCut={fmt_eth(protocol_cut)}"
    return state, msg


def open_line(
    state: MagicaState,
    borrower: str,
    asset_symbol: str,
    limit_wei: float,
    rate_bps: int,
) -> Tuple[MagicaState, LineSim]:
    if len(state.lines) >= BFIN_MAX_LINES:
        raise ValueError("max lines reached")
    if rate_bps <= 0 or rate_bps > BFIN_MAX_RATE_BPS:
        raise ValueError("invalid rate")
    lid = state.next_line_id
    state.next_line_id += 1
    l = LineSim(
        line_id=lid,
        borrower=borrower,
        asset_symbol=asset_symbol,
        limit_wei=limit_wei,
        rate_bps=rate_bps,
        last_accrual_block=state.current_block,
    )
    state.lines[lid] = l
    return state, l


def simulate_draw(state: MagicaState, line_id: int, amount_wei: float) -> Tuple[MagicaState, str]:
    if amount_wei <= 0:
        return state, "amount must be > 0"
    ensure_line_exists(state, line_id)
    l = state.lines[line_id]
    if l.frozen:
        return state, "line frozen"
    l, _interest = accrue_line_interest(l, state.current_block)
    if l.borrowed_wei + amount_wei > l.limit_wei:
        return state, "limit exceeded"
    l.borrowed_wei += amount_wei
    state.lines[line_id] = l
    msg = (
        f"draw: line {line_id}, borrower {truncate(l.borrower)}, "
        f"amount={fmt_eth(amount_wei)}, borrowed={fmt_eth(l.borrowed_wei)}"
    )
    return state, msg


def simulate_repay(state: MagicaState, line_id: int, amount_wei: float) -> Tuple[MagicaState, str]:
    if amount_wei <= 0:
        return state, "amount must be > 0"
    ensure_line_exists(state, line_id)
    l = state.lines[line_id]
    l, interest = accrue_line_interest(l, state.current_block)
    if l.borrowed_wei <= 0:
        return state, "nothing owed"
    pay = min(amount_wei, l.borrowed_wei)
    l.borrowed_wei -= pay
    state.lines[line_id] = l
    state.protocol_fee_wei += interest
    msg = (
        f"repay: line {line_id}, borrower {truncate(l.borrower)}, "
        f"paid={fmt_eth(pay)}, interestAccrued={fmt_eth(interest)}, remaining={fmt_eth(l.borrowed_wei)}"
    )
    return state, msg


def set_tag(state: MagicaState, address: str, tags_hash: str, note: str) -> MagicaState:
    state.tags[address] = TagRecord(address=address, tags_hash=tags_hash, note=note)
    return state


# ---------------------------------------------------------------------------
# CLI command implementations
# ---------------------------------------------------------------------------


def cmd_info(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    cfg = load_config(args.config)
    print(f"{APP_NAME} v{APP_VERSION}")
    print(f"State file : {state_path(args.state)}")
    print(f"Config file: {config_path(args.config)}")
    print(f"Vaults     : {len(state.vaults)}   Lines: {len(state.lines)}   Positions: {len(state.vault_positions)}")
    print(f"Current block: {state.current_block}")
    print(f"Fee collector: {truncate(state.fee_collector)}")
    print(f"Guardian     : {truncate(state.guardian)}")
    print(f"Risk council : {truncate(state.risk_council)}")
    print(f"Protocol fee: {fmt_eth(state.protocol_fee_wei)}")
    rpc_url = cfg.get("rpc_url", "(none)")
    print(f"RPC URL (optional): {rpc_url}")
    return 0


def cmd_step(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    steps = max(1, args.blocks)
    state.current_block += steps
    save_state(state, args.state)
    print(f"Advanced block by {steps} → {state.current_block}")
    return 0


def cmd_open_vault(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    cap_wei = float(args.deposit_cap_wei)
    mgmt = args.management_fee_bps
    wdr = args.withdrawal_fee_bps
    proto = args.protocol_fee_bps
    state, v = open_vault(
        state=state,
        name=args.name,
        asset_symbol=args.asset_symbol,
        deposit_cap_wei=cap_wei,
        management_fee_bps=mgmt,
        withdrawal_fee_bps=wdr,
        protocol_fee_bps=proto,
        strategy_hint=args.strategy_hint,
        enabled=not args.disabled,
    )
    save_state(state, args.state)
    print("Created vault:")
    print(vault_summary(v, state))
    return 0


def cmd_vaults(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    if not state.vaults:
        print("No vaults.")
        return 0
    for vid in sorted(state.vaults.keys()):
        print(vault_summary(state.vaults[vid], state))
        print()
    return 0


def cmd_vault(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    vid = args.vault_id
    if vid not in state.vaults:
        print("Vault not found.")
        return 1
    print(vault_summary(state.vaults[vid], state))
    return 0


def cmd_deposit(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    amt = float(args.amount_wei)
    owner = args.owner
    state, msg = simulate_deposit(state, args.vault_id, owner, amt)
    save_state(state, args.state)
    print(msg)
    return 0


def cmd_withdraw(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    shares = float(args.shares)
    owner = args.owner
    state, msg = simulate_withdraw(state, args.vault_id, owner, shares)
    save_state(state, args.state)
    print(msg)
    return 0


def cmd_positions(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    owner = args.owner
    rows: List[List[str]] = [["Vault", "Owner", "Shares", "ApproxAssets"]]
    for (vid, addr), pos in state.vault_positions.items():
        if owner and addr.lower() != owner.lower():
            continue
        v = state.vaults.get(vid)
        if not v:
            continue
        assets = convert_to_assets(v, pos.shares)
        rows.append(
            [
                str(vid),
                truncate(addr),
                f"{pos.shares:.6f}",
                fmt_eth(assets),
            ]
        )
    if len(rows) == 1:
        print("No positions.")
        return 0
    print(print_table(rows))
    return 0


def cmd_harvest(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    gain = float(args.gain_wei)
    state, msg = simulate_harvest(state, args.vault_id, gain)
    save_state(state, args.state)
    print(msg)
    return 0


def cmd_open_line(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    limit = float(args.limit_wei)
    rate = args.rate_bps
    state, line = open_line(
        state=state,
        borrower=args.borrower,
        asset_symbol=args.asset_symbol,
        limit_wei=limit,
        rate_bps=rate,
    )
    save_state(state, args.state)
    print("Created line:")
    print(line_summary(line))
    return 0


def cmd_lines(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    if not state.lines:
        print("No credit lines.")
        return 0
    for lid in sorted(state.lines.keys()):
        print(line_summary(state.lines[lid]))
    return 0


def cmd_line(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    lid = args.line_id
    if lid not in state.lines:
        print("Line not found.")
        return 1
    print(line_summary(state.lines[lid]))
    return 0


def cmd_draw(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    amt = float(args.amount_wei)
    state, msg = simulate_draw(state, args.line_id, amt)
    save_state(state, args.state)
    print(msg)
    return 0


def cmd_repay(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    amt = float(args.amount_wei)
    state, msg = simulate_repay(state, args.line_id, amt)
    save_state(state, args.state)
    print(msg)
    return 0


def cmd_tags(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    if not state.tags:
        print("No tags.")
        return 0
    for addr, rec in state.tags.items():
        print(f"{truncate(addr)} → {rec.tags_hash} ({rec.note})")
    return 0


def cmd_set_tag(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    state = set_tag(state, args.address, args.tags_hash, args.note or "")
    save_state(state, args.state)
    print(f"Set tags for {truncate(args.address)}")
    return 0


def cmd_config(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
