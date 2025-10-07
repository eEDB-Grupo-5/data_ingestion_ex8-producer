import os
from dotenv import load_dotenv

CHUNK_SIZE = 10
PROCESS_INTERVAL = 60  # seconds
RETRY_DELAY = 20  # seconds
DOTENV_PATH = "/home/bait/dev/ex8-producer/.env"

load_dotenv(dotenv_path=DOTENV_PATH)

AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PATH_PREFIX = os.getenv("S3_PATH_PREFIX", "")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "my-topic")

SCHEMA_PATH = "src/ex8_producer/schemas/reclamacoes.avsc"

SCHEMA = {
    "ano": "int",
    "trimestre": "string",
    "categoria": "string",
    "tipo": "string",
    "cnpj_if": "string",
    "instituicao_financeira": "string",
    "indice": "string",
    "quantidade_de_reclamacoes_reguladas_procedentes": "int",
    "quantidade_de_reclamacoes_reguladas_outras": "int",
    "quantidade_de_reclamacoes_nao_reguladas": "int",
    "quantidade_total_de_reclamacoes": "int",
    "quantidade_total_de_clientes_ccs_e_scr": "int",
    "quantidade_de_clientes_ccs": "int",
    "quantidade_de_clientes_scr": "int"
}