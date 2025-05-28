# src/auth.py

import os
import msal
from dotenv import load_dotenv

# Carrega variáveis de ambiente de um arquivo .env
load_dotenv()

TENANT_ID     = os.getenv('TENANT_ID')
CLIENT_ID     = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    raise EnvironmentError(
        "Variáveis de ambiente ausentes. "
        "Copie .env.example → .env e preencha TENANT_ID, CLIENT_ID e CLIENT_SECRET."
    )

def acquire_token() -> str:
    """
    Obtém um token de acesso usando MSAL ConfidentialClientApplication.
    Tenta primeiro no cache; se não houver, faz client_credentials flow.
    Retorna a string do access_token.
    """
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )

    # Tenta recuperar do cache
    result = app.acquire_token_silent(
        scopes=["https://analysis.windows.net/powerbi/api/.default"],
        account=None
    )

    # Se não achou no cache, faz client credentials
    if not result:
        result = app.acquire_token_for_client(
            scopes=["https://analysis.windows.net/powerbi/api/.default"]
        )

    access_token = result.get("access_token")
    if not access_token:
        err = result.get("error_description", result.get("error"))
        raise RuntimeError(f"Falha ao obter token: {err}")

    return access_token
