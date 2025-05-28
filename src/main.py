#!/usr/bin/env python3
# src/main.py

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
    print(f"[Dataset] Coletando histórico de refresh do dataset '{ds['name']}' em workspace '{ws['name']}'...")
    async with sem:
        history = await list_dataset_refresh_history(ws['id'], ds['id'])
    print(f"[Dataset] {len(history)} registros brutos coletados para '{ds['name']}'.")
    entries: List[Dict[str, Any]] = []
    for entry in history:
        parsed = parse_refresh_entry(entry)
        parsed.update({
            'workspace':     ws['name'],
            'dataset_name':  ds['name'],
            'type':          'Dataset'
        })
        entries.append(parsed)
    print(f"[Dataset] {len(entries)} registros parseados para '{ds['name']}'.")
    return entries

async def fetch_dataflow_entries(
    ws: Dict[str, Any],
    df: Dict[str, Any],
    sem: asyncio.Semaphore
) -> List[Dict[str, Any]]:
    print(f"[Dataflow] Coletando transações do dataflow '{df['name']}' em workspace '{ws['name']}'...")
    async with sem:
        txs = await list_dataflow_transactions(ws['id'], df['objectId'])
    print(f"[Dataflow] {len(txs)} transações brutas coletadas para '{df['name']}'.")
    entries: List[Dict[str, Any]] = []
    for entry in txs:
        parsed = parse_transaction_entry(entry)
        parsed.update({
            'workspace':      ws['name'],
            'dataflow_name':  df['name'],
            'type':           'Dataflow'
        })
        entries.append(parsed)
    print(f"[Dataflow] {len(entries)} registros parseados para '{df['name']}'.")
    return entries

async def gather_all_refreshes(
    max_concurrency: int
) -> List[Dict[str, Any]]:
    print("Iniciando coleta de workspaces...")
    workspaces = await list_workspaces()
    print(f"{len(workspaces)} workspaces encontrados.")

    sem = asyncio.Semaphore(max_concurrency)
    tasks: List[asyncio.Task] = []

    for ws in workspaces:
        # datasets
        ds_list = await list_datasets(ws['id'])
        print(f"Workspace '{ws['name']}' tem {len(ds_list)} datasets refreshable.")
        for ds in ds_list:
            tasks.append(asyncio.create_task(fetch_dataset_entries(ws, ds, sem)))
        # dataflows
        df_list = await list_dataflows(ws['id'])
        print(f"Workspace '{ws['name']}' tem {len(df_list)} dataflows.")
        for df in df_list:
            tasks.append(asyncio.create_task(fetch_dataflow_entries(ws, df, sem)))

    print(f"Total de tarefas a executar: {len(tasks)}")
    results = await asyncio.gather(*tasks)
    # achata lista de listas
    flattened = [item for sublist in results for item in sublist]
    print(f"Coleta bruta finalizada com {len(flattened)} registros.")
    return flattened

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
        print("=== Iniciando pipeline de coleta ===")
        raw_entries = await gather_all_refreshes(args.max_concurrency)

        print("=== Iniciando transformação de registros ===")
        transformed = transform_entries(raw_entries, args.timezone)
        print(f"{len(transformed)} registros transformados.")

        print(f"Gravando resultados em '{args.output}'...")
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(transformed, f, ensure_ascii=False, indent=2)
        print("Arquivo salvo com sucesso.")

        msg = f"{len(transformed)} registros transformados e salvos em {args.output}"
        logger.info(msg)
        print("=== Pipeline concluído com sucesso ===")
        sys.exit(0)

    except Exception as e:
        err_msg = f"Falha ProcessingError: {e}"
        print(err_msg, file=sys.stderr)
        logger.error(err_msg)
        print("=== Pipeline finalizado com erros ===", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
