#!/usr/bin/env python3
# src/main.py

import asyncio
import argparse
import json
from typing import List, Dict, Any

from .workspaces import list_workspaces
from .datasets import list_datasets
from .refresh import list_dataset_refresh_history
from .dataflows import list_dataflows, list_dataflow_transactions
from .parser import parse_refresh_entry, parse_transaction_entry
from .transform import transform_entries

async def gather_all_refreshes() -> List[Dict[str, Any]]:
    """
    Para cada workspace, coleta:
      - histórico de refresh de datasets
      - histórico de transações de dataflows
    Retorna uma lista de dicionários brutos.
    """
    result: List[Dict[str, Any]] = []
    workspaces = await list_workspaces()

    for ws in workspaces:
        ws_name = ws.get('name')
        ws_id   = ws.get('id')

        # 1) Datasets
        datasets = await list_datasets(ws_id)
        for ds in datasets:
            history = await list_dataset_refresh_history(ws_id, ds['id'])
            for entry in history:
                parsed = parse_refresh_entry(entry)
                parsed.update({
                    'workspace':     ws_name,
                    'dataset_name':  ds['name'],
                    'type':          'Dataset'
                })
                result.append(parsed)

        # 2) Dataflows
        dataflows = await list_dataflows(ws_id)
        for df in dataflows:
            transactions = await list_dataflow_transactions(ws_id, df['objectId'])
            for entry in transactions:
                parsed = parse_transaction_entry(entry)
                parsed.update({
                    'workspace':      ws_name,
                    'dataflow_name':  df['name'],
                    'type':           'Dataflow'
                })
                result.append(parsed)

    return result

async def main():
    parser = argparse.ArgumentParser(
        description="Coleta histórico de refresh de datasets e transações de dataflows do Power BI."
    )
    parser.add_argument(
        "--output", "-o",
        default="refresh_history.json",
        help="Caminho do arquivo JSON de saída"
    )
    parser.add_argument(
        "--timezone", "-t",
        default="America/Sao_Paulo",
        help="Fuso horário para formatação (ex: America/Sao_Paulo)"
    )
    args = parser.parse_args()

    # coleta bruta
    raw_entries = await gather_all_refreshes()

    # transforma e enriquece
    transformed = transform_entries(raw_entries, args.timezone)

    # grava arquivo de saída
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(transformed, f, ensure_ascii=False, indent=2)

    print(f"{len(transformed)} registros transformados e salvos em {args.output}")

if __name__ == "__main__":
    asyncio.run(main())
