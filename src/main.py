# src/main.py

import asyncio
from .workspaces import list_workspaces

async def main():
    grps = await list_workspaces()
    if not grps:
        print("Nenhum workspace encontrado.")
    else:
        for g in grps:
            print(f"{g['id']} — {g['name']}")

if __name__ == "__main__":
    asyncio.run(main())