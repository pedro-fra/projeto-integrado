# src/parsers.py

import json
from datetime import datetime
import zoneinfo
from typing import Dict, Any, Optional

def parse_iso(s: str) -> Optional[datetime]:
    """
    Converte string ISO 8601 (com ou sem 'Z') para datetime com offset.
    """
    if not s:
        return None
    if s.endswith('Z'):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

def fmt_local(iso: str, tz: zoneinfo.ZoneInfo) -> str:
    """
    Formata string ISO para horário local no formato 'dd-mm-YYYY HH:MM:SS'.
    """
    dt = parse_iso(iso)
    return dt.astimezone(tz).strftime("%d-%m-%Y %H:%M:%S") if dt else ""

def extract_error_message(raw: str) -> str:
    """
    Extrai descrição de erro de um JSON bruto ou retorna a string original limpa.
    """
    if not raw:
        return ""
    try:
        obj = json.loads(raw)
        desc = obj.get("errorDescription", raw)
    except json.JSONDecodeError:
        desc = raw
    # Se tiver ';', assume-se que a mensagem útil vem depois
    return desc.split(";", 1)[1].strip() if ";" in desc else desc.strip()

def parse_refresh_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza um registro de refresh de dataset, extraindo apenas campos relevantes.
    """
    attempts = entry.get("refreshAttempts") or []
    first = next((a for a in attempts if a.get("attemptId") == 1), {})
    return {
        "requestId":    entry.get("requestId"),
        "id":           entry.get("id"),
        "refreshType":  entry.get("refreshType"),
        "startTime":    entry.get("startTime"),
        "endTime":      entry.get("endTime"),
        "status":       entry.get("status"),
        "errorMessage": first.get("serviceExceptionJson", ""),
        "attemptCount": len(attempts),
    }

def parse_transaction_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza um registro de transação de dataflow.
    """
    return {
        "transactionId": entry.get("transactionId"),
        "requestId":     entry.get("requestId"),
        "startTime":     entry.get("startTime"),
        "endTime":       entry.get("endTime"),
        "status":        entry.get("status"),
    }
