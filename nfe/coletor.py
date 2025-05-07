import csv
import os
import logging
from datetime import datetime

from sankhya_api import get_data

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)


def load_existing_nunota():
    """Carrega os NUNOTAs existentes no arquivo CSV com base na data de hoje."""
    data_hoje = datetime.now().strftime("%Y%m%d")
    csv_path = f"logs/{data_hoje}.csv"

    nunotas_existentes = set()
    if os.path.exists(csv_path):
        with open(csv_path, mode="r", newline='', encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Ignora o cabeçalho, se existir
            for linha in reader:
                if linha:
                    nunotas_existentes.add(int(linha[0]))
    return nunotas_existentes


def save_nunota_list_to_csv(query):
    data_str = datetime.now().strftime('%Y%m%d')
    path_csv = f'logs/{data_str}.csv'
    os.makedirs("logs", exist_ok=True)

    # 1. Carregar as NUNOTAs já existentes no CSV
    nunota_existentes = load_existing_nunota()

    try:
        # 2. Obter dados da consulta
        result = get_data(query)
        rows = result.get("responseBody", {}).get("rows", []) if result else []
    except Exception as e:
        logging.error(f"Erro ao obter dados: {e}")
        return []

    # 3. Filtrar novas NUNOTAs
    novos_nunota = [row[0] for row in rows if row and row[0] not in nunota_existentes]

    # 4. Verificar se há novas NUNOTAs
    if novos_nunota:
        # Adicionar novos NUNOTAs ao CSV
        try:
            with open(path_csv, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for nunota in novos_nunota:
                    writer.writerow([nunota])
            logging.info(f"Novas NUNOTAs encontradas e adicionadas ao CSV: {novos_nunota}")
        except Exception as e:
            logging.warning(f"Erro ao escrever no arquivo: {e}")
    else:
        # Caso não haja novas NUNOTAs
        logging.info("Nenhuma nova NUNOTA para carregar e salvar.")

    return novos_nunota
