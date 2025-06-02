import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()


# Função para enviar notificação para o Telegram
def enviar_notificacao_telegram(mensagem):
    token = os.getenv('BOTTOKEN')  # Seu Token do Bot
    chat_id = os.getenv('CHATID')  # O chat_id do destinatário
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # Dados a serem enviados
    payload = {
        'chat_id': chat_id,
        'text': mensagem,
        'parse_mode': 'Markdown'
    }

    # Enviar a requisição para o Telegram
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print("Notificação enviada com sucesso!")
            return True
        else:
            print(f"Falha ao enviar notificação: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Ocorreu um erro: {e}")
        return None