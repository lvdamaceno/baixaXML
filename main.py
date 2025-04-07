import csv
import os
import time
import json
import logging
from datetime import datetime
from xml.dom import minidom
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import itertools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ------------------- Configurações e utilitários -------------------

def load_credentials(config_file='config.json'):
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.critical(f"Erro ao carregar config: {e}")
        return None


credentials = load_credentials()
if not credentials:
    raise SystemExit("Credenciais inválidas ou inexistentes")

url_auth = "https://api.sankhya.com.br/login"
url_query = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DbExplorerSP.executeQuery&outputType=json"

token_cache = {"token": None}


def auth(max_retries=3, delay=3):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url_auth, headers=credentials)
            if response.status_code == 200:
                token = response.json().get("bearerToken")
                if token:
                    token_cache["token"] = token
                    return token
            logger.warning(f"[{attempt}] Erro de autenticação: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            logger.error(f"[{attempt}] Exceção: {e}")
        time.sleep(delay)
    raise SystemExit("Falha ao autenticar após várias tentativas")


# ------------------- Requisições à API -------------------

def get_data(query, max_attempts=5):
    if not token_cache["token"]:
        auth()

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token_cache['token']}"
    }
    payload = {
        "serviceName": "DbExplorerSP.executeQuery",
        "requestBody": {"sql": query}
    }

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url_query, headers=headers, json=payload, timeout=10)

            if response.status_code == 200:
                return response.json()

            elif response.status_code in [401, 403]:
                logger.warning(f"[{attempt}] Token inválido/expirado, renovando...")
                auth()  # força renovação do token
                headers["Authorization"] = f"Bearer {token_cache['token']}"

            else:
                logger.warning(f"[{attempt}] Erro HTTP {response.status_code} - {response.text}")
        except requests.exceptions.Timeout:
            logger.warning(f"[{attempt}] Timeout. Tentando novamente...")
        except requests.RequestException as e:
            logger.error(f"[{attempt}] Erro de requisição: {e}")
        time.sleep(7)
    return None


# ------------------- Processamento de XML -------------------

def create_xml_file(chave):
    query_xml = f"SELECT [XML] FROM TGFNFE WHERE CHAVENFE = '{chave}'"
    result = get_data(query_xml)

    if not result or 'responseBody' not in result or 'rows' not in result['responseBody']:
        logging.warning(f"Sem dados para a chave: {chave}")
        return False

    rows = result['responseBody']['rows']
    if not rows or not rows[0]:
        logging.warning(f"XML não encontrado - Chave: {chave}")
        return False

    xml_string = rows[0][0]
    os.makedirs('xmls', exist_ok=True)

    try:
        dom = minidom.parseString(xml_string)
        pretty_xml = dom.toprettyxml(indent="  ")
        file_path = os.path.join('xmls', f'{chave}.xml')
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(pretty_xml)
        # logging.info(f"XML salvo em: {file_path}")
        return True
    except Exception as e:
        logging.error(f"Erro ao processar XML para {chave}: {e}")
        return False


# ------------------- Controle de chaves e logs -------------------

def load_existing_chaves(csv_path):
    if not os.path.exists(csv_path):
        return set()
    with open(csv_path, 'r', encoding='utf-8') as f:
        return {linha.strip() for linha in f}


def save_chaves_to_csv(query):
    data_str = datetime.now().strftime('%Y%m%d')
    path_csv = f'logs/{data_str}.csv'
    os.makedirs("logs", exist_ok=True)

    chaves_existentes = load_existing_chaves(path_csv)
    result = get_data(query)

    rows = result.get("responseBody", {}).get("rows", []) if result else []
    novas_chaves = []

    with open(path_csv, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in rows:
            chave = ''.join(filter(str.isdigit, row))
            if chave and chave not in chaves_existentes:
                novas_chaves.append(chave)
                writer.writerow([chave])
                # logger.info(f"Chave nova adicionada: {chave}")

    return novas_chaves


# ------------------- Execução principal -------------------

def save_xmls(workers, query, data):
    chaves = save_chaves_to_csv(query)
    if not chaves:
        logging.info("Nenhuma nova chave para processar.")
        return

    total = len(chaves)
    sucesso = 0
    falha = 0
    chaves_com_erro = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(create_xml_file, chave): chave for chave in chaves}
        for future in tqdm(as_completed(futures), total=total, desc=f"Baixando XMLs {data}"):
            chave = futures[future]
            try:
                if future.result():
                    sucesso += 1
                else:
                    falha += 1
                    chaves_com_erro.append(chave)
            except Exception as e:
                logging.error(f"Erro inesperado ao processar chave {chave}: {e}")
                falha += 1
                chaves_com_erro.append(chave)

    # Salvar CSV com chaves com erro
    if chaves_com_erro:
        data_formatada = datetime.now().strftime('%Y%m%d')
        os.makedirs('logs', exist_ok=True)
        caminho_erro_csv = f'logs/erros_{data_formatada}.csv'
        with open(caminho_erro_csv, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["CHAVE_COM_ERRO"])
            for chave in chaves_com_erro:
                writer.writerow([chave])
        logging.warning(f"{len(chaves_com_erro)} chaves com erro salvas em {caminho_erro_csv}")

    # Log final
    logging.info("==== RELATÓRIO FINAL ====")
    logging.info(f"Total de chaves processadas: {total}")
    logging.info(f"Sucessos: {sucesso}")
    logging.info(f"Falhas: {falha}")


def load_query(nome, **params):
    caminho = os.path.join("queries", f"{nome}.sql")
    with open(caminho, "r", encoding="utf-8") as file:
        query = file.read()
        return query.format(**params)


def processar_periodos_xml(nome_query, ano_inicio=2013, ano_fim=None, empresas=range(1, 8), max_workers=15):
    if ano_fim is None:
        ano_fim = datetime.now().year

    for emp, ano, mes in itertools.product(empresas, range(ano_inicio, ano_fim + 1), range(1, 13)):
        # Evita meses futuros do ano atual
        if ano == datetime.now().year and mes > datetime.now().month:
            continue

        try:
            query = load_query(nome_query, ano=ano, mes=mes, codemp=emp)
            descricao = f"{mes:02d}/{ano} Emp {emp}"
            save_xmls(max_workers, query, descricao)
        except Exception as e:
            logging.error(f"Erro ao processar {mes:02d}/{ano} Empresa {emp}: {e}")


if __name__ == "__main__":
    processar_periodos_xml("SAIDAS_ANO_MES_EMP", ano_inicio=2013, empresas=range(1, 8))
