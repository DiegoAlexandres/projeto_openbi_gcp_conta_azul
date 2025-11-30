import pandas as pd
from datetime import timedelta
from src.config import CLIENTES, DATASET_BRONZE
from src.big_query import salvar_no_bigquery, get_ultima_execucao, registrar_execucao, listar_ids_faltantes

from src.extractors import (
    dCategorias, 
    dCentroCusto, 
    dClientes, 
    dContaCorrente, 
    fContasReceber, 
    fContasPagar, 
    fCentroCusto
)

def rodar_etl():
    print("Iniciando Pipeline Controlado")

    tabelas = [
        {"nome": "dCategorias", "funcao": dCategorias.extrair_categorias, "tipo": "full"},
        {"nome": "dCentroCusto", "funcao": dCentroCusto.extrair_centros_custo, "tipo": "full"},
        {"nome": "dClientes", "funcao": dClientes.extrair_clientes, "tipo": "full"},
        {"nome": "dContaCorrente", "funcao": dContaCorrente.extrair_contas_corrente, "tipo": "full"},
        
        {"nome": "fContasReceber", "funcao": fContasReceber.extrair_contas_receber, "tipo": "incremental"},
        {"nome": "fContasPagar", "funcao": fContasPagar.extrair_contas_pagar, "tipo": "incremental"},
    ]

    for info in tabelas:
        nome_tabela = info["nome"]
        print(f"Processando: {nome_tabela.upper()}")
        
        for cliente in CLIENTES:
            print(f"Cliente: {cliente['nome']}")
            
            data_filtro = None
            modo_escrita = 'replace'
            
            if info["tipo"] == "incremental":
                ultima_execucao = get_ultima_execucao(nome_tabela, cliente['id'])
                
                if ultima_execucao:
                    data_segura = ultima_execucao - timedelta(days=2)
                    data_filtro = data_segura.strftime('%Y-%m-%d')
                    
                    modo_escrita = 'append'
                    print(f"Incremental: Janela de 2 dias (Desde {data_filtro})")
                else:
                    print("Primeira execução: Carga Full")
                    modo_escrita = 'replace'

            try:
                if data_filtro:
                    df = info["funcao"](cliente['token'], data_inicio=data_filtro)
                else:
                    df = info["funcao"](cliente['token'])
            except TypeError:
                df = info["funcao"](cliente['token'])
            
            if df is not None and not df.empty:
                df['client_id'] = cliente['id'] 
                df['cliente_nome'] = cliente['nome']
                
                salvar_no_bigquery(df, DATASET_BRONZE, nome_tabela, modo=modo_escrita)
                
                if info["tipo"] == "incremental":
                    registrar_execucao(nome_tabela, cliente['id'], data_filtro)
            
            else:
                print(f"Nenhum dado retornado.")
                if info["tipo"] == "incremental" and data_filtro:
                     registrar_execucao(nome_tabela, cliente['id'], data_filtro)

    print("Processando: dCentroCustoDetalhes")
    
    for cliente in CLIENTES:
        print(f"   -> Cliente: {cliente['nome']}")
        
        ids_receber = listar_ids_faltantes(
            tabela_origem="fContasReceber",
            tabela_destino="dCentroCustoDetalhes",
            coluna_id_origem="financialEvent_id", 
            coluna_id_destino="id_evento_financeiro"
        )
        
        ids_pagar = listar_ids_faltantes(
            tabela_origem="fContasPagar",
            tabela_destino="dCentroCustoDetalhes",
            coluna_id_origem="financialEvent_id",
            coluna_id_destino="id_evento_financeiro"
        )
        
        todos_ids = list(set(ids_receber + ids_pagar))
        
        if todos_ids:
            df_detalhes = fCentroCusto.extrair_detalhes_centro_custo(cliente['token'], todos_ids)
            
            if df_detalhes is not None:
                df_detalhes['client_id'] = cliente['id']
                df_detalhes['cliente_nome'] = cliente['nome']
                
                salvar_no_bigquery(df_detalhes, DATASET_BRONZE, "dCentroCustoDetalhes", modo="append")
        else:
            print("Todos os detalhes já estão atualizados.")

if __name__ == "__main__":
    rodar_etl()