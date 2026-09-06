"""Token-Laden für flow.run — eine Quelle der Wahrheit (T-088-Envelope).

Seit T-088 ist die Runtime-Token-Datei ein JSON-Envelope
``{"token": "...", "expires_at": "..."|null}``. Legacy-Installs haben eine
 Plaintext-Datei (eine Zeile = Token). Diese Funktion akzeptiert beides
und gibt den roten Bearer-Token zurück — node_utils.load_token() im
iowap-node implementiert dieselbe Semantik (T-088), hier bewusst
dupliziert, damit flow.run ohne nodes-Package läuft.
"""
from __future__ import annotations

import json
from pathlib import Path


def load_bearer_token(token_file: str | Path) -> str:
    """Liest die Token-Datei und liefert den Bearer-Token (Env- oder JSON-Format)."""
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