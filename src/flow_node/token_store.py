"""Token loading for flow.run — one source of truth (T-088 envelope).

Since T-088 the runtime token file is a JSON envelope
``{"token": "...", "expires_at": "..."|null}``. Legacy installs have a
plaintext file (one line = token). This function accepts both
and returns the raw bearer token — node_utils.load_token() in
iowap-node implements the same semantics (T-088), deliberately
duplicated here so flow.run runs without the nodes package.
"""
from __future__ import annotations

import json
from pathlib import Path


def load_bearer_token(token_file: str | Path) -> str:
    """Reads the token file and returns the bearer token (env or JSON format)."""
    raw = Path(token_file).read_text().strip()
    if not raw:
        return ""
    if raw.lstrip().startswith("{"):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return raw
        if isinstance(data, dict) and data.get("token"):
            return str(data["token"])
        return ""
    return raw