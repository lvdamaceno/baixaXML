import os


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
