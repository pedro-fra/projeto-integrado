import asyncio
from src.workspaces import list_workspaces
from src.datasets import list_datasets

async def main():
    grps = await list_workspaces()
    if not grps:
        print("Nenhum workspace encontrado.")
        return

    for g in grps:
        print(f"\nWorkspace: {g['name']} ({g['id']})")
        ds_list = await list_datasets(g['id'])
        if not ds_list:
            print("Nenhum dataset atualizavel.")
        else:
            for d in ds_list:
                print(f"  - {d['id']}  —  {d['name']}")

if __name__ == "__main__":
    asyncio.run(main())
