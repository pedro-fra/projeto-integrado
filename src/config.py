# src/config.py

import os
from dotenv import load_dotenv

# Carrega variáveis de .env
load_dotenv()

# Variáveis obrigatórias
TENANT_ID     = os.getenv('TENANT_ID')
CLIENT_ID     = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# URL base da API Power BI (padrão se não existir no .env)
API_BASE      = os.getenv('PBI_API_URL', 'https://api.powerbi.com/v1.0/myorg')

# Verifica se todas as credenciais estão presentes
if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    raise EnvironmentError(
        "Faltam variáveis de ambiente: copie .env.example → .env e preencha TENANT_ID, CLIENT_ID e CLIENT_SECRET."
    )
