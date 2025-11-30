import requests
import time

def get_dados_api(endpoint, token, params=None, method="GET", body=None):

    base_url = "https://services.contaazul.com"
    
    endpoint_limpo = endpoint.lstrip('/')
    url = f"{base_url}/{endpoint_limpo}"
    
    headers = {
        "X-Authorization": token,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    for tentativa in range(3):
        try:
            if tentativa > 0:
                print(f"Tentativa {tentativa + 1}...")

            if method == "POST":
                response = requests.post(url, headers=headers, params=params, json=body)
            else:
                response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            
            elif response.status_code == 401:
                print(f"Erro 401 (Recusado).")

                print(f"Resposta: {response.text}")
                return None 
            
            elif response.status_code == 404:
                print(f"Erro 404 (Endereço incorreto). URL: {url}")
                return None

            elif response.status_code == 429: 
                print("Rate Limit. Esperando 5s")
                time.sleep(5)
                continue
                
            elif response.status_code >= 500:

                print(f"Erro servidor ({response.status_code})")
                time.sleep(2)
                continue
            
            else:
                print(f"Erro {response.status_code}: {response.text}")
                return None

        except Exception as e:
            print(f"Erro de conexão: {e}")
            time.sleep(2)
            
    return None