# 1. Imagem Base (Python 3.12 leve)
FROM python:3.14-slim

# 2. Define variáveis de ambiente para o Python não gerar cache (.pyc) e imprimir logs na hora
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Cria a pasta de trabalho dentro do container
WORKDIR /app

# 4. Copia o arquivo de dependências primeiro (Estratégia de Cache)
COPY requirements.txt .

# 5. Instala as bibliotecas
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copia todo o resto do seu código para dentro do container
COPY . .

# 7. Comando que roda quando o container liga
# Usamos o mesmo comando que funcionou no seu PC (-m src.main)
CMD ["python", "-m", "src.main"]