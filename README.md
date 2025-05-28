## Sobre

Este projeto coleta e transforma automaticamente o histórico de refresh de **datasets** e **dataflows** do Power BI, gerando um arquivo JSON padronizado e um log de execução. Ele:

- Autentica via MSAL usando credenciais do Azure (client-credentials flow)  
- Lista workspaces, datasets “refreshable” (excluindo Usage Metrics) e dataflows  
- Busca, pagina e normaliza entradas de refresh/transação  
- Formata timestamps no fuso local e calcula durações  
- Registra status de execução em `pbiRefreshHistory_logs.txt`  