# src/refresh.py

import httpx
from typing import List, Dict, Any, Optional

from .auth import acquire_token
from .config import API_BASE

async def fetch_all_pages(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Faz chamadas paginadas à API até exaurir @odata.nextLink.
    Retorna lista de todos os itens em 'value'.
    """
    items: List[Dict[str, Any]] = []
    while url:
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
        params = None  # só usa params na primeira requisição
    return items

async def list_dataset_refresh_history(
    group_id: str,
    dataset_id: str
) -> List[Dict[str, Any]]:
    """
    Retorna todo o histórico de refreshes de um dataset em um workspace.
    Cada item é o JSON cru do endpoint /refreshes.
    """
    token = acquire_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    url = f"{API_BASE}/groups/{group_id}/datasets/{dataset_id}/refreshes"

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=30.0)) as client:
        return await fetch_all_pages(client, url, headers)
