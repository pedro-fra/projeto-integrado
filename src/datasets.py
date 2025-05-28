import httpx
from typing import List, Dict, Any

from .auth import acquire_token
from .config import API_BASE

async def list_datasets(group_id: str) -> List[Dict[str, Any]]:
    """
    Retorna os datasets de um workspace (grupo) que podem ser atualizados.
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
        return data.get("value", [])
