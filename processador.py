import logging
import traceback
from nfe.executor import save_all_nunota_to_xmls
from utils import load_query


def processar_query(nome_query: str, max_workers: int = 15) -> None:
    try:
        query = load_query(nome_query)
        save_all_nunota_to_xmls(max_workers, query)
    except Exception as e:
        logging.error(f"Erro ao processar consulta '{nome_query}': {e}")
        logging.debug(traceback.format_exc())
