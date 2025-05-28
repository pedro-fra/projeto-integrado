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

async def fetch_dataset_entries(
    ws: Dict[str, Any],
    ds: Dict[str, Any],
    sem: asyncio.Semaphore
) -> List[Dict[str, Any]]:
    async with sem:
        history = await list_dataset_refresh_history(ws['id'], ds['id'])
    entries = []
    for entry in history:
        parsed = parse_refresh_entry(entry)
        parsed.update({
            'workspace':     ws['name'],
            'dataset_name':  ds['name'],
            'type':          'Dataset'
        })
        entries.append(parsed)
    return entries

async def fetch_dataflow_entries(
    ws: Dict[str, Any],
    df: Dict[str, Any],
    sem: asyncio.Semaphore
) -> List[Dict[str, Any]]:
    async with sem:
        txs = await list_dataflow_transactions(ws['id'], df['objectId'])
    entries = []
    for entry in txs:
        parsed = parse_transaction_entry(entry)
        parsed.update({
            'workspace':      ws['name'],
            'dataflow_name':  df['name'],
            'type':           'Dataflow'
        })
        entries.append(parsed)
    return entries

async def gather_all_refreshes(
    max_concurrency: int
) -> List[Dict[str, Any]]:
    """
    Dispara todas as coletas de datasets e dataflows como tarefas,
    usando um Semaphore para limitar a concorrência.
    """
    sem = asyncio.Semaphore(max_concurrency)
    result_tasks: List[asyncio.Task] = []

    workspaces = await list_workspaces()
    for ws in workspaces:
        # datasets
        ds_list = await list_datasets(ws['id'])
        for ds in ds_list:
            result_tasks.append(
                asyncio.create_task(fetch_dataset_entries(ws, ds, sem))
            )
        # dataflows
        df_list = await list_dataflows(ws['id'])
        for df in df_list:
            result_tasks.append(
                asyncio.create_task(fetch_dataflow_entries(ws, df, sem))
            )

    # aguarda todas as tarefas e achata a lista de listas em lista única
    results = await asyncio.gather(*result_tasks, return_exceptions=False)
    return [item for sublist in results for item in sublist]

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
    parser.add_argument(
        "--max-concurrency", "-c",
        type=int,
        default=5,
        help="Máximo de requisições HTTP simultâneas"
    )
    args = parser.parse_args()

    # coletas concorrentes
    raw_entries = await gather_all_refreshes(args.max_concurrency)

    # transformação final
    transformed = transform_entries(raw_entries, args.timezone)

    # grava saída
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(transformed, f, ensure_ascii=False, indent=2)

    print(f"{len(transformed)} registros transformados e salvos em {args.output}")

if __name__ == "__main__":
    asyncio.run(main())
