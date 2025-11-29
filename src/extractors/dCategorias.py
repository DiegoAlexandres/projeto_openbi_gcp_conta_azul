import pandas as pd
from src.api import get_dados_api

def extrair_categorias(token):
    print("   -> 🔎 Buscando Categorias (Receitas e Despesas)...")
    
    tipos_conta = ["expenses", "revenues"]
    dfs = []
    
    for tipo in tipos_conta:

        endpoint = f"app/finance/v1/category/activation/{tipo}" 
        
        dados = get_dados_api(endpoint, token)
        
        if dados:
            if isinstance(dados, list):
                lista_limpa = dados
            else:
                lista_limpa = dados.get('items', [])

            if lista_limpa:
                df_temp = pd.json_normalize(lista_limpa)
                df_temp['tipo_conta_extracao'] = tipo
                
                df_temp = df_temp.astype(str)
                
                dfs.append(df_temp)
    
    if not dfs:
        return None
        
    df_final = pd.concat(dfs, ignore_index=True)
    return df_final