#!/usr/bin/env python3
import asyncio
import argparse
import json
import sys
from typing import List, Dict, Any

from .workspaces import list_workspaces
from .datasets import list_datasets
from .refresh import list_dataset_refresh_history
from .dataflows import list_dataflows, list_dataflow_transactions
from .parser import parse_refresh_entry, parse_transaction_entry
from .transform import transform_entries
from .logger import setup_logger

async def fetch_dataset_entries(
    ws: Dict[str, Any],
    ds: Dict[str, Any],
    sem: asyncio.Semaphore
) -> List[Dict[str, Any]]:
    async with sem:
        history = await list_dataset_refresh_history(ws['id'], ds['id'])
    entries: List[Dict[str, Any]] = []
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
    entries: List[Dict[str, Any]] = []
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
    Dispara as coletas de datasets e dataflows de todos os workspaces,
    limitando o número de requisições simultâneas.
    """
    sem = asyncio.Semaphore(max_concurrency)
    tasks: List[asyncio.Task] = []
    workspaces = await list_workspaces()

    for ws in workspaces:
        # datasets
        ds_list = await list_datasets(ws['id'])
        for ds in ds_list:
            tasks.append(asyncio.create_task(fetch_dataset_entries(ws, ds, sem)))
        # dataflows
        df_list = await list_dataflows(ws['id'])
        for df in df_list:
            tasks.append(asyncio.create_task(fetch_dataflow_entries(ws, df, sem)))

    results = await asyncio.gather(*tasks)
    # achata a lista de listas para uma única lista de registros
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

    logger = setup_logger()

    try:
        # 1) coleta bruta
        raw_entries = await gather_all_refreshes(args.max_concurrency)
        # 2) transforma
        transformed = transform_entries(raw_entries, args.timezone)
        # 3) grava JSON
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(transformed, f, ensure_ascii=False, indent=2)

        msg = f"{len(transformed)} registros transformados e salvos em {args.output}"
        print(msg)
        logger.info(msg)
        sys.exit(0)

    except Exception as e:
        err_msg = f"Falha ProcessingError: {e}"
        print(err_msg, file=sys.stderr)
        logger.error(err_msg)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
