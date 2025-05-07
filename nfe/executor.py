from notification import enviar_notificacao_telegram
from .coletor import save_nunota_list_to_csv
from .xml_handler import create_xml_file_from_nunota
from .erros import salvar_erros_csv

import os
import csv
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


def ler_arquivos_de_erro():
    """Lê os arquivos de erro existentes no diretório de logs."""
    arquivos_erro = [f for f in os.listdir("logs") if f.startswith("erros") and f.endswith(".csv")]
    notas_reprocessadas = set()

    for arquivo in arquivos_erro:
        caminho_erro = os.path.join("logs", arquivo)
        logging.debug(f"🔁 Reprocessando erros do arquivo {caminho_erro}")

        try:
            with open(caminho_erro, mode="r", newline='', encoding="utf-8") as f:
                reader = csv.reader(f)
                notas_reprocessadas.update({
                    int(row[0]) for row in reader
                    if row and row[0].isdigit() and not row[0].startswith("ok")
                })
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo de erro {caminho_erro}: {e}")
            logging.debug(traceback.format_exc())

    return arquivos_erro, notas_reprocessadas


def processar_xmls(todas_notas, workers, notas_reprocessadas, notas_removidas_do_csv_erro, notas_com_erro):
    """Processa as notas fiscais para baixar os XMLs usando paralelismo."""
    sucesso, falha = 0, 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        tarefas = {executor.submit(create_xml_file_from_nunota, nota): nota for nota in todas_notas}

        for future in tqdm(as_completed(tarefas), total=len(todas_notas), desc="Baixando XMLs"):
            nota = tarefas[future]
            try:
                if future.result():
                    sucesso += 1
                    if nota in notas_reprocessadas:
                        notas_removidas_do_csv_erro.add(nota)
                else:
                    falha += 1
                    notas_com_erro.append(nota)
            except Exception as e:
                logging.error(f"Erro inesperado ao processar NUNOTA {nota}: {e}")
                enviar_notificacao_telegram(f"📄 *Erro inesperado ao processar baixaXML {nota}: {e}*")
                logging.debug(traceback.format_exc())
                falha += 1
                notas_com_erro.append(nota)

    return sucesso, falha


def reescrever_arquivo_de_erro(arquivos_erro, notas_removidas_do_csv_erro):
    """Reescreve os arquivos de erro removendo as notas reprocessadas com sucesso."""
    for arquivo in arquivos_erro:
        caminho_erro = os.path.join("logs", arquivo)
        if notas_removidas_do_csv_erro:
            logging.debug(
                f"✅ Removendo {len(notas_removidas_do_csv_erro)} NUNOTAs reprocessados com sucesso do arquivo de erro {caminho_erro}")

            try:
                with open(caminho_erro, mode="r", newline='', encoding="utf-8") as f:
                    reader = csv.reader(f)
                    linhas_restantes = [
                        row for row in reader
                        if row and row[0].isdigit() and int(row[0]) not in notas_removidas_do_csv_erro
                    ]

                with open(caminho_erro, mode="w", newline='', encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(linhas_restantes)

                logging.debug(f"NUNOTAs removidos do arquivo de erro {arquivo}: {sorted(notas_removidas_do_csv_erro)}")

            except Exception as e:
                logging.error(f"Erro ao reescrever o arquivo de erro {caminho_erro}: {e}")
                logging.debug(traceback.format_exc())


def gerar_relatorio(todas_notas, notas_query, notas_reprocessadas, notas_removidas_do_csv_erro, sucesso, falha):
    """Gera e envia o relatório final de processamento."""
    total = len(todas_notas)
    reprocessadas = len(notas_reprocessadas)

    # Relatório final
    logging.info("==== RELATÓRIO FINAL ====")
    logging.info(f"Total de notas processadas: {total}")
    logging.info(f"Novas notas da query: {len(notas_query)}")
    logging.info(f"Notas reprocessadas: {reprocessadas}")
    logging.info(f"Reprocessadas com sucesso: {len(notas_removidas_do_csv_erro)}")
    logging.info(f"Sucessos: {sucesso}")
    logging.info(f"Falhas: {falha}")

    mensagem = f"""
    *📄 Relatório de Processamento de XMLs:*
    • *Total processado:* `{total}`
    • *Novas notas da consulta:* `{len(notas_query)}`
    • *Notas reprocessadas:* `{reprocessadas}`
    • *Reprocessadas com sucesso:* `{len(notas_removidas_do_csv_erro)}`
    • *Sucesso:* `{sucesso}`
    • *Falhas:* `{falha}`
    """
    enviar_notificacao_telegram(mensagem)


def save_all_nunota_to_xmls(workers: int, query: str) -> None:
    """Função principal que coordena o fluxo de processamento de NUNOTAs."""
    notas_query = save_nunota_list_to_csv(query)
    notas_reprocessadas = set()
    notas_removidas_do_csv_erro = set()

    arquivos_erro, notas_reprocessadas = ler_arquivos_de_erro()
    todas_notas = list(set(notas_query) | notas_reprocessadas)

    if not todas_notas:
        logging.debug("Nenhuma nova nota fiscal eletrônica (NFe) para processar.")
        enviar_notificacao_telegram('🧾 *Nenhuma nova nota fiscal eletrônica (NFe) para processar.*')
        return

    sucesso, falha = 0, 0
    notas_com_erro = []

    sucesso, falha = processar_xmls(todas_notas, workers, notas_reprocessadas, notas_removidas_do_csv_erro,
                                    notas_com_erro)

    salvar_erros_csv(notas_com_erro)

    reescrever_arquivo_de_erro(arquivos_erro, notas_removidas_do_csv_erro)

    gerar_relatorio(todas_notas, notas_query, notas_reprocessadas, notas_removidas_do_csv_erro, sucesso, falha)
