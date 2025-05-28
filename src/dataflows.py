# src/dataflows.py

import httpx
from typing import List, Dict, Any

from .auth import acquire_token
from .config import API_BASE
from .refresh import fetch_all_pages

async def list_dataflows(group_id: str) -> List[Dict[str, Any]]:
    """
    Retorna todos os dataflows de um workspace.
    Cada item tem pelo menos 'objectId' e 'name'.
    """
    token = acquire_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    url = f"{API_BASE}/groups/{group_id}/dataflows"
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=30.0)) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json().get("value", [])

async def list_dataflow_transactions(
    group_id: str,
    dataflow_id: str
) -> List[Dict[str, Any]]:
    """
    Retorna todas as transações de um dataflow, fazendo paginação.
    """
    token = acquire_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    url = f"{API_BASE}/groups/{group_id}/dataflows/{dataflow_id}/transactions"
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=30.0)) as client:
        return await fetch_all_pages(client, url, headers)
