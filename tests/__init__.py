"""Test fixtures."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

# Make src importable
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Stub the heavy py-clob-client-v2 imports that aren't installed in test env
sys.modules.setdefault("py_clob_client", MagicMock())
sys.modules.setdefault("py_clob_client.client", MagicMock())
sys.modules.setdefault("py_clob_client.clob_types", MagicMock())
sys.modules.setdefault("py_clob_client.order_builder", MagicMock())
