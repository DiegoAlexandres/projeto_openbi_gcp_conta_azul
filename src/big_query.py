# src/big_query.py
from pandas_gbq import to_gbq
from src.config import PROJECT_ID, LOCATION

def salvar_no_bigquery(df, dataset, tabela):

    if df is None or df.empty:
        print(f"   ⚠️ [BigQuery] Abortado: DataFrame vazio para a tabela '{tabela}'.")
        return

    destino = f"{dataset}.{tabela}"

    print(f"   🚀 [BigQuery] Enviando {len(df)} linhas para: {destino}...")

    try:
        to_gbq(
            df,
            destino,
            project_id=PROJECT_ID,
            if_exists='replace',  
            location=LOCATION
        )
        print("   ✅ [BigQuery] Carga realizada com sucesso!")

    except Exception as e:
        print(f"   ❌ [BigQuery] Erro Crítico: {e}")