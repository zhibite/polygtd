"""Test-run environment defaults.

``.env.docker`` is loaded by the settings classes in tests too. Production
may set ``PAUSE_NEW_ENTRIES=true`` while the backtest collects history;
unit tests exercise the trading logic, so force the pause off here. Tests
that need the pause set ``PAUSE_NEW_ENTRIES`` on their own config stub.
"""
from __future__ import annotations

import os

os.environ["PAUSE_NEW_ENTRIES"] = "false"
