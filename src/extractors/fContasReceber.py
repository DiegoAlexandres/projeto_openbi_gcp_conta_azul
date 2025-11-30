import pandas as pd
import time
from src.api import get_dados_api

def extrair_contas_receber(token):
    print("Buscando Contas a Receber (Installment View)...")
    
    endpoint = "finance-pro-reader/v1/installment-view"
    
    body_filtro = {
        "dueDateFrom": None,
        "dueDateTo": None,
        "quickFilter": "ALL",
        "search": None,
        "type": "REVENUE"
    }

    lista_completa = []
    pagina_atual = 1
    
    while True:
        params = {
            "page": pagina_atual,
            "page_size": 100
        }
        
        print(f"Baixando Página {pagina_atual}")
        
        dados = get_dados_api(endpoint, token, params=params, method="POST", body=body_filtro)
        
        if not dados:
            break
            
        items = dados.get('items', [])
        
        if not items:
            break
            
        lista_completa.extend(items)
        
        if len(items) < 100:
            break
            
        pagina_atual += 1
        time.sleep(0.5)

    if not lista_completa:
        return None

    df = pd.json_normalize(lista_completa)
    
    df.columns = df.columns.str.replace('.', '_')
    
    df = df.astype(str)
    
    return df