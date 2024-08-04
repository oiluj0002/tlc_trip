#Bibliotecas
import os
import requests
import pyarrow.parquet as pq
from google.cloud import bigquery


def criar_diretorio(diretorio):
    if not os.path.exists(diretorio):   # Criar diretório dos dados brutos
        os.mkdir(diretorio)


def extract(url, destino):
    '''Função que extrai os dados brutos da fonte'''
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(destino, 'wb') as f:
            f.write(response.content)
        print(f'{destino} extraído')
    except requests.exceptions.RequestException as exception:
        print(f'{exception}')


def transform(df):
    '''Função que limpa os dados da tabela viagens para de acordo com os requisitos de negócio'''
    df = df.loc[df['VendorID'] != 6,:]
    df = df.loc[df['fare_amount'] > 0, :] #retirar linhas com dados incongruentes
    df = df.loc[df['extra'] > 0, :]
    df = df.drop(columns='total_amount')

    df['passenger_count'] = df['passenger_count'].astype('Int64')
    df['RatecodeID'] = df['RatecodeID'].astype('Int64')
    df['payment_type'] = df['payment_type'].astype('Int64')

    df.loc[~df['RatecodeID'].isin([1, 2, 3, 4, 5, 6]),'RatecodeID'] = None  #converter ids não existentes em Nan
    df.loc[~df['payment_type'].isin([1, 2, 3, 4, 5, 6]),'payment_type'] = None

    df = df.reset_index(drop=True)
    df['ID'] = df.index   #criar id indice das viagens
    print('Tranformação efetuada')
    
    return df


def load(fonte, tabela, schema, formato, **kwargs):
    '''Função que carrega os dados limpos para o BigQuery'''
    client = bigquery.Client()
    dataset = client.create_dataset('silver', exists_ok=True) #Cria dataset caso não exista

    schema = client.schema_from_json(schema)  #Associa o json do schema

    job_config = bigquery.LoadJobConfig( #Configuração do Job
        source_format=formato, 
        schema=schema, 
        **kwargs
        )
    
    table = dataset.table(tabela) #Cria tabela

    if formato == bigquery.SourceFormat.PARQUET: #Diferencia entre a fonte Parquet e a fonte CSV
        load_job = client.load_table_from_dataframe(fonte, table, job_config=job_config)
    else:
        load_job = client.load_table_from_file(fonte, table, job_config=job_config)
    
    load_job.result() #Executa o Job
    print(f'Tabela {tabela} carregada para o BigQuery')


def main():
    criar_diretorio('data')

    #Extract
    extract(
        'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet',
        'data/viagens.parquet'
        ) #Viagens

    
    extract(
        'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
        'data/locais.csv'
        ) #Locais
    
    #Transform
    data = pq.read_table('data/viagens.parquet')
    df_viagens = transform(data.to_pandas())

    #Load
    load(
        fonte=df_viagens,
        tabela='viagens',
        schema='schemas/schema_viagens.json',
        formato=bigquery.SourceFormat.PARQUET
        ) #Viagens
    
    with open('data/locais.csv', 'rb') as df_locais:
        load(
            fonte=df_locais,
            tabela='locais',
            schema='schemas/schema_locais.json',
            formato=bigquery.SourceFormat.CSV,
            skip_leading_rows=1
        ) #Locais
    

if __name__ == '__main__':
    main()