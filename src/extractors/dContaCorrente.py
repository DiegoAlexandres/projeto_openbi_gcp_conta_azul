import pandas as pd
from src.api import get_dados_api

def extrair_contas_corrente(token):
    print("Buscando Contas Bancárias (Dashboard)")
    
    endpoint = "contaazul-bff/dashboard/v1/financial-accounts"
    
    dados = get_dados_api(endpoint, token)
    
    if not dados:
        return None

    lista_contas = dados.get('dashboardBankAccounts', [])
    
    if not lista_contas:
        return None

    df = pd.json_normalize(lista_contas)
    
    df.columns = df.columns.str.replace('.', '_')
    
    df = df.astype(str)
    
    return df