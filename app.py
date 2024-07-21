import json
import os

from dotenv import load_dotenv

load_dotenv()

from utils.s3_mock import __create_connection, __create_bucket, __to_s3

if __name__ == "__main__":
    #============================================
    # Criando variaveis
    bucket_name = 'dlake-prd-da'
    bucket_path = 'api/gitlab/projects'

    url = os.getenv('URL')
    access_key = os.getenv('ACCESS_KEY')
    secret_key = os.getenv('SECRET_KEY')

    partition_fields = ['namespaces', 'dt']

    #============================================
    # Lendo arquivo que será materializado no s3.
    with open('./dados/gitlab.json', 'r') as file:
        data = json.load(file)

    #============================================
    # Chamando funções e passando variaveis.

    s3_client = __create_connection(url, access_key, secret_key)

    __create_bucket(s3_client, bucket_name)
    __to_s3(s3_client, bucket_path, bucket_name, partition_fields, data)
