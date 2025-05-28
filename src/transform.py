# src/transform.py

import json
from datetime import datetime
import zoneinfo
from typing import List, Dict, Any, Optional

def parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    # transforma 'Z' em '+00:00' para fromisoformat
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    return datetime.fromisoformat(s)

def fmt_local(iso: str, tz: zoneinfo.ZoneInfo) -> str:
    dt = parse_iso(iso)
    return dt.astimezone(tz).strftime('%d-%m-%Y %H:%M:%S') if dt else ''

def extract_error_message(raw: str) -> str:
    if not raw:
        return ''
    try:
        obj = json.loads(raw)
        desc = obj.get('errorDescription', raw)
    except json.JSONDecodeError:
        desc = raw
    # se há ';', pega só depois
    return desc.split(';', 1)[1].strip() if ';' in desc else desc.strip()

def transform_entries(
    entries: List[Dict[str, Any]],
    timezone: str
) -> List[Dict[str, Any]]:
    tz = zoneinfo.ZoneInfo(timezone)
    out: List[Dict[str, Any]] = []
    for e in entries:
        # comum
        ds = parse_iso(e.get('startTime', ''))
        de = parse_iso(e.get('endTime', ''))
        raw_status = e.get('status', '')
        status = (
            'Cancelado' if raw_status == 'Cancelled'
            else 'Sucesso' if raw_status in ('Success', 'Completed')
            else 'Falha'
        )

        if e.get('type') == 'Dataset':
            out.append({
                'workspace':      e['workspace'],
                'origem':         'Dataset',
                'dataset_name':   e['dataset_name'],
                'status':         status,
                'tipo_refresh':   {'ViaApi':'API','Scheduled':'Agendamento'}.get(e.get('refreshType',''), 'Sob Demanda'),
                'data_inicio':    fmt_local(e.get('startTime',''), tz),
                'data_fim':       fmt_local(e.get('endTime',''), tz),
                'duration_seconds': int((de - ds).total_seconds()) if ds and de else None,
                'erro':           extract_error_message(e.get('errorMessage','')),
                'attemptCount':   e.get('attemptCount', 0)
            })
        else:
            # Dataflow
            out.append({
                'workspace':       e['workspace'],
                'origem':          'Dataflow',
                'dataflow_name':   e['dataflow_name'],
                'status':          status,
                'data_inicio':     fmt_local(e.get('startTime',''), tz),
                'data_fim':        fmt_local(e.get('endTime',''), tz),
                'duration_seconds': int((de - ds).total_seconds()) if ds and de else None
            })
    return out
