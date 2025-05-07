import csv
from datetime import datetime
from pathlib import Path
from typing import List
import logging


def salvar_erros_csv(notas_com_erro: List[str], pasta_log: str = "logs") -> None:
    if not notas_com_erro:
        return

    data_formatada = datetime.now().strftime('%Y%m%d')
    Path(pasta_log).mkdir(parents=True, exist_ok=True)
    caminho_csv = Path(pasta_log) / f'erros_{data_formatada}.csv'

    with caminho_csv.open('w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows([[nota] for nota in notas_com_erro])

    logging.warning(f"{len(notas_com_erro)} NUNOTA(s) com erro salvas em {caminho_csv}")
