
# Projeto de Integração com a API Sankhya

Este projeto tem como objetivo integrar a API do Sankhya para realizar a consulta de notas fiscais e fazer o download dos arquivos XML. Além disso, após o processamento, uma notificação é enviada via Telegram.

## Funcionalidades

- **Conexão com a API do Sankhya**: Autenticação e execução de consultas para listar notas fiscais.
- **Download de XMLs**: Realiza o download dos arquivos XML das notas fiscais listadas.
- **Notificação via Telegram**: Ao final do processamento, uma notificação é enviada informando sobre a conclusão ou erros no processo.

## Estrutura de Arquivos

- `main.py`: Arquivo principal que inicia o processamento.
- `processador.py`: Contém a lógica de execução da consulta e do download dos XMLs.
- `notifications.py`: Responsável pelo envio de notificações via Telegram.
- `sankhya_api.py`: Contém as funções para autenticação e consulta na API do Sankhya.
- `utils.py`: Utilitário para carregamento de queries e verificação de arquivos existentes.
- `nfe/coletor.py`: Coleta e salva os dados das NUNOTAs.
- `nfe/erros.py`: Lida com erros e salva registros de falhas em arquivos CSV.
- `nfe/executor.py`: Executor que processa as notas e realiza o download dos XMLs.
- `nfe/xml_handler.py`: Busca as notas e salva os arquivos xml.

## Requisitos

Este projeto depende de algumas bibliotecas Python. Instale as dependências utilizando o comando:

```
pip install -r requirements.txt
```

As dependências são:
- `requests`: Para fazer requisições HTTP.
- `python-dotenv`: Para carregar variáveis de ambiente de um arquivo `.env`.
- `tqdm`: Para mostrar uma barra de progresso durante o download dos XMLs.

## Como Rodar

1. Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis de ambiente:
    ```
    SANKHYA_TOKEN=<seu_token_aqui>
    SANKHYA_APPKEY=<sua_appkey_aqui>
    SANKHYA_USERNAME=<seu_username_aqui>
    SANKHYA_PASSWORD=<sua_password_aqui>
    BOTTOKEN=<seu_token_do_bot_aqui>
    CHATID=<seu_chat_id_aqui>
    ```

2. Execute o script principal para começar o processo:
    ```
    python main.py
    ```

3. O script irá conectar à API Sankhya, buscar as notas fiscais, fazer o download dos XMLs e, ao final, enviar uma notificação no Telegram.

## Logs

Os arquivos de log são salvos na pasta `logs/`. Os logs de erro são salvos em arquivos CSV, e os arquivos de notas processadas também são registrados.

## Licença

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE](LICENSE) para mais detalhes.
