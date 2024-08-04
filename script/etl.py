#Bibliotecas
import os
import requests
import pyarrow.parquet as pq
from google.cloud import bigquery


#Criar diretório bronze
if os.path.exists('src'):
    pass
else:
    os.mkdir('src')


#Importar dados do site do TLC NYC
url1 = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet'
url2 = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

response = requests.get(url1)
with open('src/viagens.parquet', 'wb') as f:
    f.write(response.content)
    print('viagens.parquet extraído')

response = requests.get(url2)
with open('src/locais.csv', 'wb') as f:
    f.write(response.content)
    print('locais.csv extraído')


#Transformação
data = pq.read_table('src/viagens.parquet')
df = data.to_pandas()

df = df.loc[df['VendorID'] != 6,:]
df = df.loc[df['fare_amount'] > 0, :]        #retirar linhas com dados incongruentes
df = df.loc[df['extra'] > 0, :]
df = df.drop(columns='total_amount')

df['passenger_count'] = df['passenger_count'].astype('Int64')
df['RatecodeID'] = df['RatecodeID'].astype('Int64')
df['payment_type'] = df['payment_type'].astype('Int64')

df.loc[~df['RatecodeID'].isin([1, 2, 3, 4, 5, 6]),'RatecodeID'] = None                  #converter ids não existentes em Nan
df.loc[~df['payment_type'].isin([1, 2, 3, 4, 5, 6]),'payment_type'] = None

df = df.reset_index(drop=True)
df['ID'] = df.index         #criar id indice das viagens
print('Tranformação efetuada')


#Carregamento
client = bigquery.Client()

dataset = client.create_dataset('silver', exists_ok=True)       #Cria dataset silver

# Tabela Viagens
#    Definir o esquema da tabela
schema = client.schema_from_json('schemas/schema_viagens.json')

#   Criação da tabela silver_viagens
tabela = dataset.table('viagens')

#   Configuração do Job
job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, schema=schema)

load_job = client.load_table_from_dataframe(df, tabela, job_config=job_config)
load_job.result()
print(f'Tabela {tabela} carregada para o BigQuery')

# Tabela Locais
with open('src/locais.csv', 'rb') as locais:
    #   Definir o esquema da tabela
    schema = client.schema_from_json('schemas/schema_locais.json')

    #   Criação da tabela silver_locais
    tabela = dataset.table('locais')

    #   Configuração do Job
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, schema=schema, skip_leading_rows=1)

    load_job = client.load_table_from_file(locais, tabela, job_config=job_config)
    load_job.result()
    print(f'Tabela {tabela} carregada para o BigQuery')
