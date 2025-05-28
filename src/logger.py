#!/usr/bin/env python3
import logging
from logging import Logger

def setup_logger(
    name: str = "pbi_refresh",
    log_file: str = "pbiRefreshHistory_logs.txt"
) -> Logger:
    """
    Configura um logger que grava em arquivo no formato padrão,
    incluindo data, hora (com milissegundos) e nível.
    Exemplo de linha de log:
    2025-05-28 14:23:45,123 INFO <mensagem>
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
        # Não passamos datefmt, para usar o formato padrão que adiciona os milissegundos corretamente
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
