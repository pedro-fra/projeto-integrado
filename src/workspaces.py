# src/workspaces.py

import httpx
from typing import List, Dict, Any

from .auth import acquire_token
from .config import API_BASE

async def list_workspaces() -> List[Dict[str, Any]]:
    """
    Busca e retorna todos os workspaces (grupos) do Power BI.
    Cada item no retorno terá, pelo menos, as chaves 'id' e 'name'.
    """
    token = acquire_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    url = f"{API_BASE}/groups"

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=30.0)) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        return data.get("value", [])
