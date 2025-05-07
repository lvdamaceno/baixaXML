import logging
from xml.dom import minidom
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from sankhya_api import get_data
from utils import load_existing_nunota, load_query
import os
import csv
from datetime import datetime


def save_nunota_list_to_csv(query):
    data_str = datetime.now().strftime('%Y%m%d')
    path_csv = f'logs/{data_str}.csv'
    os.makedirs("logs", exist_ok=True)

    # Carrega NUNOTA existentes no CSV como set (melhor performance)
    nunota_existentes = set(load_existing_nunota(path_csv))

    try:
        result = get_data(query)
        rows = result.get("responseBody", {}).get("rows", []) if result else []
    except Exception as e:
        print(f"Erro ao obter dados: {e}")
        return []

    novos_nunota = []

    for row in rows:
        if not row or len(row) < 1:
            continue
        nunota = row[0]
        if nunota and nunota not in nunota_existentes:
            novos_nunota.append(nunota)

    if novos_nunota:
        try:
            with open(path_csv, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for nunota in novos_nunota:
                    writer.writerow([nunota])
        except Exception as e:
            print(f"Erro ao escrever no arquivo: {e}")

    return novos_nunota


def fetch_xml_string(nunota):
    try:
        query_xml = f"SELECT XMLENVCLI FROM TGFNFE WHERE NUNOTA = {int(nunota)}"
    except ValueError:
        logging.error(f"Nunota inválido: {nunota}")
        return False

    try:
        result = get_data(query_xml)
        rows = result.get("responseBody", {}).get("rows", [])
        if not rows or not rows[0]:
            logging.warning(f"XML não encontrado - Chave: {nunota}")
            return False

        xml_string = rows[0][0]
        if not isinstance(xml_string, str) or not xml_string.strip():
            logging.warning(f"XML vazio ou inválido - Chave: {nunota}")
            return False

        return xml_string

    except Exception as e:
        logging.error(f"Erro ao obter XML da base - Chave: {nunota} | Erro: {e}")
        return False


def create_xml_file_from_nunota(nunota):
    xml_string = fetch_xml_string(nunota)
    try:
        dom = minidom.parseString(xml_string)
        pretty_xml = dom.toprettyxml(indent="  ")
    except Exception as e:
        logging.error(f"Erro ao processar o XML da nota {nunota}: {e}")
        return False

    try:
        os.makedirs('xmls', exist_ok=True)
        file_path = os.path.join('xmls', f'{nunota}.xml')
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(pretty_xml)
        logging.info(f"XML salvo com sucesso em: {file_path}")
        return True
    except Exception as e:
        logging.error(f"Erro ao salvar arquivo XML da nota {nunota}: {e}")
        return False


def save_all_xmls(workers, query):
    listadenotas = save_nunota_list_to_csv(query)
    if not listadenotas:
        logging.info("Nenhuma nova chave para processar.")
        return

    total = len(listadenotas)
    sucesso = 0
    falha = 0
    nunota_com_erro = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(create_xml_file_from_nunota, nunota): nunota for nunota in listadenotas}
        for future in tqdm(as_completed(futures), total=total, desc=f"Baixando XMLs"):
            chave = futures[future]
            try:
                if future.result():
                    sucesso += 1
                else:
                    falha += 1
                    nunota_com_erro.append(chave)
            except Exception as e:
                logging.error(f"Erro inesperado ao processar nunota {chave}: {e}")
                falha += 1
                nunota_com_erro.append(chave)

    # Salvar CSV com chaves com erro
    if nunota_com_erro:
        data_formatada = datetime.now().strftime('%Y%m%d')
        os.makedirs('logs', exist_ok=True)
        caminho_erro_csv = f'logs/erros_{data_formatada}.csv'
        with open(caminho_erro_csv, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["NUNOTA_COM_ERRO"])
            for chave in nunota_com_erro:
                writer.writerow([chave])
        logging.warning(f"{len(nunota_com_erro)} nunotas com erro salvas em {caminho_erro_csv}")

    # Log final
    logging.info("==== RELATÓRIO FINAL ====")
    logging.info(f"Total de notas processadas: {total}")
    logging.info(f"Sucessos: {sucesso}")
    logging.info(f"Falhas: {falha}")


def processar_periodos_xml(nome_query, max_workers=15):
    try:
        query = load_query(nome_query)
        save_all_xmls(max_workers, query)
    except Exception as e:
        logging.error(f"Erro ao processar")


if __name__ == "__main__":
    processar_periodos_xml('xmls')
