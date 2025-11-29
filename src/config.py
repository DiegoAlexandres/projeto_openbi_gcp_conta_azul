import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION", "US")

DATASET_BRONZE = os.getenv("DATASET_BRONZE")

CLIENTES = [
    {
        "id": 1, 
        "nome": "Empresa A", 
        "token": os.getenv("TOKEN_EMPRESA_A")
    },
    # {
    #     "id": 2, 
    #     "nome": "Empresa B", 
    #     "token": os.getenv("TOKEN_EMPRESA_B")
    # },
]