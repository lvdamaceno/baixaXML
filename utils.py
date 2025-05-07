import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_credentials(config_file='config.json'):
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.critical(f"Erro ao carregar config: {e}")
        return None


def load_existing_nunota(csv_path):
    if not os.path.exists(csv_path):
        return set()
    with open(csv_path, 'r', encoding='utf-8') as f:
        return {linha.strip() for linha in f}


def load_query(nome, **params):
    caminho = os.path.join("queries", f"{nome}.sql")
    with open(caminho, "r", encoding="utf-8") as file:
        query = file.read()
        return query.format(**params)
