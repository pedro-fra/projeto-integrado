# src/main.py

import asyncio
from src.workspaces import list_workspaces
from src.datasets import list_datasets
from src.refresh import list_dataset_refresh_history

async def main():
    grps = await list_workspaces()
    for g in grps:
        ds_list = await list_datasets(g['id'])
        for d in ds_list:
            history = await list_dataset_refresh_history(g['id'], d['id'])
            print(f"{g['name']} / {d['name']} → {len(history)} registros de refresh")

if __name__ == "__main__":
    asyncio.run(main())
