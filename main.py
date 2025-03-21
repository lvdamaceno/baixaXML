import csv
import os
import time
from xml.dom import minidom
from datetime import datetime
import requests
import logging
import json

# Configurações de logging
logging.basicConfig(level=logging.INFO)


# Função para carregar credenciais a partir de um arquivo JSON
def load_credentials():
    try:
        with open('config.json', 'r') as file:
            credentials = json.load(file)
            return credentials
    except FileNotFoundError:
        print("Arquivo de configuração não encontrado!")
        return None
    except json.JSONDecodeError:
        print("Erro ao decodificar o arquivo de configuração!")
        return None


credentials = load_credentials()
if credentials:
    token = credentials['token']
    appkey = credentials['appkey']
    password = credentials['password']
    username = credentials['username']

# URL e Cabeçalhos de autenticação
url_auth = "https://api.sankhya.com.br/login"
url = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DbExplorerSP.executeQuery&outputType=json"
headers_auth = {
    "token": token,
    "appkey": appkey,
    "password": password,
    "username": username
}

# Query SQL
query_keys = (f"SELECT top 5 CHAVENFE FROM TGFCAB CAB WHERE CODTIPOPER IN (1110,1101,1105,1112,1181,1111,1243,1509) "
              f"AND CAB.DTNEG = '{datetime.now().strftime('%d/%m/%Y')}' AND CHAVENFE IS NOT NULL")


def auth():
    response_auth = requests.post(url_auth, headers=headers_auth)
    if response_auth.status_code == 200:
        return response_auth.json().get('bearerToken')
    else:
        logging.error(f"Erro na autenticação: {response_auth.status_code} - {response_auth.text}")
        return None


def get_data(query):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth()}"
    }
    data = {
        "serviceName": "DbExplorerSP.executeQuery",
        "requestBody": {"sql": query}
    }
    attempt = 0
    while attempt < 5:
        try:
            response = requests.get(url, headers=headers, json=data, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                logging.warning(f"Erro na requisição: {response.status_code}")
                return None
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout na requisição, tentando novamente... ({attempt + 1}/5)")
            attempt += 1
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro na requisição: {e}")
            return None


def create_xml_file(chave):
    query_xml = f"SELECT [XML] FROM TGFNFE WHERE CHAVENFE = '{chave}'"
    xml_string = get_data(query_xml)['responseBody']['rows'][0][0]
    os.makedirs('xmls', exist_ok=True)

    # Gerar o XML formatado com indentação
    dom = minidom.parseString(xml_string)
    pretty_xml = dom.toprettyxml(indent="  ")

    file_path = os.path.join('xmls', f'{chave}.xml')
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(pretty_xml)

    logging.info(f"Arquivo XML salvo em: {file_path}")


def check_value_in_csv(file_path, value_to_check):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        return any(value_to_check in row for row in reader)


def chaves_csv():
    data_formatada = datetime.now().strftime('%Y%m%d')
    arquivo_csv = f'logs/{data_formatada}.csv'

    if not os.path.exists(arquivo_csv):
        with open(arquivo_csv, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Chaves'])  # Cabeçalho do CSV

    rows = get_data(query_keys)['responseBody']['rows']
    with open(arquivo_csv, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for row in rows:
            chave = ''.join(filter(str.isdigit, row))
            if not check_value_in_csv(arquivo_csv, chave):
                logging.info(f"Adicionando chave: {chave}")
                writer.writerow([chave])


def save_xmls():
    chaves_csv()

    data_formatada = datetime.now().strftime('%Y%m%d')
    arquivo_csv = f'logs/{data_formatada}.csv'

    with open(arquivo_csv, mode='r', newline='', encoding='utf-8') as file:
        leitor_csv = csv.reader(file)
        for linha in leitor_csv:
            chave = linha[0]
            nome_arquivo_xml = f"{chave}.xml"
            caminho_arquivo_xml = os.path.join('xmls', nome_arquivo_xml)

            if os.path.exists(caminho_arquivo_xml):
                logging.info(f"Arquivo XML já existe: {caminho_arquivo_xml}")
            else:
                create_xml_file(chave)


if __name__ == "__main__":
    save_xmls()
