import logging
import time
import os

import requests
from dotenv import load_dotenv

from utils import load_query

# Carregar variáveis do arquivo .env
load_dotenv()

urlauth = "https://api.sankhya.com.br/login"
urlquery = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DbExplorerSP.executeQuery&outputType=json"

token_cache = {"token": None}

headers = {
            "token": os.getenv("SANKHYA_TOKEN"),
            "appkey": os.getenv("SANKHYA_APPKEY"),
            "username": os.getenv("SANKHYA_USERNAME"),
            "password": os.getenv("SANKHYA_PASSWORD")
        }


def auth(max_retries=5, delay=3):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(urlauth, headers=headers, timeout=60)
            if response.status_code == 200:
                token = response.json().get("bearerToken")
                if token:
                    token_cache["token"] = token
                    return token
            logging.warning(f"[{attempt}] Erro de autenticação: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            logging.error(f"[{attempt}] Exceção: {e}")
        time.sleep(delay)
    raise SystemExit("Falha ao autenticar após várias tentativas")


def get_data(query, max_attempts=5):
    if not token_cache["token"]:
        auth()

    if query == 'xmls':
        sql = load_query(query)
    else:
        sql = query

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token_cache['token']}"
    }
    payload = {
        "serviceName": "DbExplorerSP.executeQuery",
        "requestBody": {"sql": sql}
    }

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(urlquery, headers=headers, json=payload, timeout=60)

            if response.status_code == 200:
                return response.json()

            elif response.status_code in [401, 403]:
                logging.warning(f"[{attempt}] Token inválido/expirado, renovando...")
                auth()  # força renovação do token
                headers["Authorization"] = f"Bearer {token_cache['token']}"

            else:
                logging.warning(f"[{attempt}] Erro HTTP {response.status_code} - {response.text}")
        except requests.exceptions.Timeout:
            logging.warning(f"[{attempt}] Timeout. Tentando novamente...")
        except requests.RequestException as e:
            logging.error(f"[{attempt}] Erro de requisição: {e}")
        time.sleep(7)
    return None
