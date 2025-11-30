import pandas as pd
import time
from src.api import get_dados_api

def extrair_clientes(token):
    print("Buscando Cadastros (Clientes/Fornecedores)...")
    
    endpoint = "contaazul-bff/person-registration/v1/persons"
    
    lista_completa = []
    pagina_atual = 1
    
    while True:
        params = {
            "page": pagina_atual,
            "page_size": 100
        }

        dados = get_dados_api(endpoint, token, params=params)
        
        if not dados:
            break
            
        items = dados.get('items', [])
        
        if not items:
            break
            
        lista_completa.extend(items)
        
        if len(items) < 100:
            break
            
        pagina_atual += 1
        time.sleep(0.2)

    if not lista_completa:
        return None

    df = pd.json_normalize(lista_completa)
    
    df.columns = df.columns.str.replace('.', '_')
    
    df = df.astype(str)
    
    return df