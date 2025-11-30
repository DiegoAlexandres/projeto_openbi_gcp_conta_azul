import os
import json
from dotenv import load_dotenv
from google.cloud import secretmanager

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
DATASET_BRONZE = os.getenv("DATASET_BRONZE")

def carregar_clientes():
    secret_id = "config_clientes_conta_azul" 
    
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
        
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        
        print("[Config] Clientes carregados do Secret Manager (GCP).")
        return json.loads(payload)
        
    except Exception:
        print("[Config] Secret Manager não acessível. Usando configuração Local (.env).")
        
        return [
            {
                "id": 1, 
                "nome": "Empresa A", 
                "token": os.getenv("TOKEN_EMPRESA_A")
            }
        ]

CLIENTES = carregar_clientes()