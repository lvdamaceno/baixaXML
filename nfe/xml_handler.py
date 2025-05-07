import logging
import os
from xml.dom import minidom
from sankhya_api import get_data


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
        logging.debug(f"XML salvo com sucesso em: {file_path}")
        return True
    except Exception as e:
        logging.error(f"Erro ao salvar arquivo XML da nota {nunota}: {e}")
        return False
