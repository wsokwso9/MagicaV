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
