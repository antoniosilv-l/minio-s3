import boto3
import json

from botocore.client import Config
from botocore.exceptions import ClientError
from collections import defaultdict

def __create_connection(url: str, access_key: str, secret_key: str):
    '''
    objectives:
        criar uma conexão com o ambiente da AWS.
    
    args:
        url (string): URL de conexão com o ambiente.
        access_key (string): Access key do ambiente. 
        secret_key (string): Secret key do ambiente.

    return:
        modulo de conexão com o ambiente do s3.
    '''
    s3_client = boto3.client(
        's3',
        endpoint_url=url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    return s3_client

def __create_bucket(s3_client, bucket_name: str):
    '''
    objectives:
        criar um bucket no ambiente.
    
    args:
        s3_client (module): Modulo para conexão com o ambiente.
        bucket_name (string): Nome do bucket que será criado.

    return:
        criação do bucket no ambiente.
    '''
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' já existe.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404': 
            s3_client.create_bucket(Bucket=bucket_name)
            print(f'Bucket "{bucket_name}" criado com sucesso!')    
        else:
            print(f'Criação do bucket falhou: {e}')

def __to_s3_partitioned(s3_client, bucket_path: str, bucket_name: str, partition_fields: list, data: list):
    '''
    objectives:
        Carregar dados no bucket designado, particionando o chave por uma chave ou mais.
    
    args:
        s3_client (module): Modulo para conexão com o ambiente.
        bucket_path (string): Caminho que serão materializado os dados.
        bucket_name (string): Nome do bucket que será criado.
        partition_fields (list): Lista de campos para realizar o particionamento.
        data (list): Dados que serão particionados e carregados no S3.

    return:
        arquivos particionados e salvos no s3.
    '''
    #==============================================================
    # Criação de um dicionario para agrupar valores por cada chave 
    # de partição.
    partitioned_data = defaultdict(list)
    
    for item in data:
        partitions = [f"{field}={str(item[field])}" for field in partition_fields]
        partition_key = "/".join(partitions)
        
        partitioned_data[partition_key].append(item)
    
    #==============================================================
    # Iteramos em cada chave e lista armazenado no partitioned_data
    # para conseguir salvar os dados necessarios.
    for partition_key, items in partitioned_data.items():
        key = f"{bucket_path}/{partition_key}/data.json"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(items),
            ContentType='application/json'
        )
        print(f"Salvando dados '{partition_key}' em {key}")