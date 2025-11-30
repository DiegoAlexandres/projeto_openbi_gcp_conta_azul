import pandas as pd
from pandas_gbq import to_gbq, read_gbq
from google.cloud import bigquery
from datetime import datetime
from src.config import DATASET_BRONZE, PROJECT_ID, LOCATION

TABELA_CONTROLE = f"{DATASET_BRONZE}.dControleAtualizacao"

def salvar_no_bigquery(df, dataset, tabela, modo='replace'):
    if df is None or df.empty:
        return

    destino = f"{dataset}.{tabela}"
    print(f"[BigQuery] Salvando {len(df)} linhas em {destino} ({modo})...")

    try:
        to_gbq(
            df,
            destino,
            project_id=PROJECT_ID,
            if_exists=modo,
            location=LOCATION
        )
        print("[BigQuery] Sucesso!")
    except Exception as e:
        print(f"[BigQuery] Erro: {e}")

def get_ultima_execucao(nome_tabela, client_id):
    query = f"""
        SELECT MAX(ultima_execucao) as data_ref
        FROM `{PROJECT_ID}.{TABELA_CONTROLE}`
        WHERE tabela = '{nome_tabela}' 
        AND client_id = {client_id}
        AND status = 'SUCESSO'
    """
    try:
        df = read_gbq(query, project_id=PROJECT_ID, location=LOCATION)
        if not df.empty and pd.notna(df['data_ref'][0]):
            return df['data_ref'][0]
    except Exception:
        return None
    return None

def registrar_execucao(nome_tabela, client_id, data_corte_usada):
    agora = datetime.now()
    
    log_data = {
        'tabela': [nome_tabela],
        'client_id': [client_id],
        'ultima_execucao': [agora],
        'data_corte': [pd.to_datetime(data_corte_usada) if data_corte_usada else None],
        'status': ['SUCESSO']
    }
    
    df_log = pd.DataFrame(log_data)
    
    try:
        to_gbq(
            df_log,
            TABELA_CONTROLE,
            project_id=PROJECT_ID,
            if_exists='append',
            location=LOCATION
        )
        print(f"[Controle] Log registrado: {agora}")
    except Exception as e:
        print(f"Erro ao gravar log de controle: {e}")

def listar_ids_faltantes(tabela_origem, tabela_destino, coluna_id_origem, coluna_id_destino):
    client = bigquery.Client(project=PROJECT_ID)
    
    try:
        client.get_table(f"{PROJECT_ID}.{DATASET_BRONZE}.{tabela_destino}")
    except Exception:
        print(f"Tabela {tabela_destino} é nova. Baixando tudo")

        query = f"""
            SELECT DISTINCT {coluna_id_origem} as id 
            FROM `{PROJECT_ID}.{DATASET_BRONZE}.{tabela_origem}`
            WHERE {coluna_id_origem} IS NOT NULL
        """
        return client.query(query).to_dataframe()['id'].astype(str).tolist()

    print(f"Verificando Delta (O que falta baixar)")
    query = f"""
        SELECT DISTINCT A.{coluna_id_origem} as id
        FROM `{PROJECT_ID}.{DATASET_BRONZE}.{tabela_origem}` A
        LEFT JOIN `{PROJECT_ID}.{DATASET_BRONZE}.{tabela_destino}` B
            ON CAST(A.{coluna_id_origem} AS STRING) = CAST(B.{coluna_id_destino} AS STRING)
        WHERE B.{coluna_id_destino} IS NULL
        AND A.{coluna_id_origem} IS NOT NULL
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df['id'].astype(str).tolist()
    except Exception as e:
        print(f"Erro ao calcular delta: {e}")
        return []