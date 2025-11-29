import pandas as pd
from src.config import CLIENTES, PROJECT_ID, DATASET_BRONZE
from src.big_query import salvar_no_bigquery 
from src.extractors import dCategorias 

def rodar_etl():
    print("🚀 Iniciando Carga Multi-Empresas...")

    # 1. Loop por cada Tabela que queremos
    tabelas_para_baixar = [
        
        {"nome": "dCategorias", "funcao": dCategorias.extrair_categorias},
        
    ]

    for tabela_info in tabelas_para_baixar:
        lista_dfs = []
        nome_tabela = tabela_info["nome"]
        funcao_extracao = tabela_info["funcao"]
        
        print(f"\n📂 Processando tabela: {nome_tabela.upper()}")

        for cliente in CLIENTES:
            print(f"   -> Cliente: {cliente['nome']} (ID: {cliente['id']})")
            
            df_cliente = funcao_extracao(cliente['token'])
            
            if df_cliente is not None and not df_cliente.empty:
                df_cliente['client_id'] = cliente['id'] 
                df_cliente['cliente_nome'] = cliente['nome']
                
                lista_dfs.append(df_cliente)
        
        if lista_dfs:
            df_final = pd.concat(lista_dfs, ignore_index=True)
            
            df_final = df_final.astype(str)
            
            salvar_no_bigquery(df_final, DATASET_BRONZE, nome_tabela)
            
            print(f"✅ Tabela {nome_tabela} atualizada com {len(df_final)} registros totais.")
        else:
            print(f"⚠️ Nenhum dado encontrado para {nome_tabela}.")

if __name__ == "__main__":
    rodar_etl() 