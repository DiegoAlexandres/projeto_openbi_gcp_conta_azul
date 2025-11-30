import pandas as pd
import asyncio
import aiohttp
import time

SEMAFORO_LIMITE = 5 

async def fetch_detalhe(session, id_evento, token, sem):
    url = f"https://services.contaazul.com/contaazul-bff/finance/v1/financial-events/{id_evento}/summary"
    
    headers = {
        "X-Authorization": token,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    async with sem:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    dados = await response.json()
                    
                    df = pd.json_normalize(dados)
                    df['id_evento_financeiro'] = id_evento
                    
                    colunas_uteis = ['id_evento_financeiro', 'categoriesRatio']
                    
                    for col in colunas_uteis:
                        if col not in df.columns:
                            df[col] = None
                            
                    return df[colunas_uteis]
                
                elif response.status == 429:
                    print(f"Rate Limit no ID {id_evento}. Pulando")
                    await asyncio.sleep(2)
                    return None
                else:
                    return None
        except Exception as e:
            print(f"Erro async ID {id_evento}: {e}")
            return None

async def processar_lote_async(token, lista_ids):
    sem = asyncio.Semaphore(SEMAFORO_LIMITE)
    tarefas = []
    
    async with aiohttp.ClientSession() as session:
        for id_evento in lista_ids:
            tarefa = asyncio.create_task(fetch_detalhe(session, id_evento, token, sem))
            tarefas.append(tarefa)
        
        resultados = await asyncio.gather(*tarefas)
        
    return [r for r in resultados if r is not None]

def extrair_detalhes_centro_custo(token, lista_ids):
    if not lista_ids:
        return None
        
    print(f"[TURBO] Baixando {len(lista_ids)} detalhes (5 simultâneos)")
    
    start_time = time.time()
    
    lista_dfs = asyncio.run(processar_lote_async(token, lista_ids))
    
    tempo = time.time() - start_time
    print(f"Concluído em {tempo:.2f} segundos. Sucesso: {len(lista_dfs)}/{len(lista_ids)}")

    if not lista_dfs:
        return None
        
    df_final = pd.concat(lista_dfs, ignore_index=True)
    df_final = df_final.astype(str)
    
    return df_final