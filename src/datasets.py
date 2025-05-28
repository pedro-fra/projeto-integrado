# src/datasets.py

import httpx
from typing import List, Dict, Any

from .auth import acquire_token
from .config import API_BASE

async def list_datasets(group_id: str) -> List[Dict[str, Any]]:
    """
    Retorna os datasets de um workspace (grupo) que podem ser atualizados,
    excluindo aqueles cujo nome contenha 'Usage Metrics'.
    Cada item tem pelo menos 'id' e 'name'.
    """
    token = acquire_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    url = f"{API_BASE}/groups/{group_id}/datasets"
    params = {"$filter": "isRefreshable eq true"}

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=30.0)) as client:
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        all_ds = data.get("value", [])
        # Filtra out datasets com 'Usage Metrics' no nome
        return [
            ds for ds in all_ds
            if "usage metrics" not in ds.get("name", "").lower()
        ]
