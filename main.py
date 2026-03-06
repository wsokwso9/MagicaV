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
