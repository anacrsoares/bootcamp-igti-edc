import boto3
import pandas as pd

# Criar um cliente para interagir com o AWS S3
s3_client = boto3.client('s3')

# Criar pasta raw-data no Bucket datalake-anacrsoares-688125619919
'''bucket_name = 'datalake-anacrsoares-688125619919'
directory_name = 'raw-data'
s3_client.put_object(Bucket=bucket_name, Key=(directory_name+'/'))'''

'''# Criar pasta raw-data no Bucket datalake-anacrsoares-688125619919
bucket_name = 'datalake-anacrsoares-688125619919'
directory_name = 'raw-data'
s3_client.put_object(Bucket=bucket_name, Key=(directory_name+'/'))'''


'''s3_client.upload_file('docentes_co.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/docentes_co.csv')
s3_client.upload_file('docentes_nordeste.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/docentes_nordeste.csv')
s3_client.upload_file('docentes_norte.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/docentes_norte.csv')
s3_client.upload_file('docentes_sudeste.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/docentes_sudeste.csv')'''
s3_client.upload_file('docentes_sul.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/docentes_sul.csv')
'''s3_client.upload_file('escolas.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/escolas.csv')
s3_client.upload_file('gestor.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/gestor.csv')
s3_client.upload_file('matricul_co.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/matricula.csv')
s3_client.upload_file('matricula_nordeste.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/matricula_nordeste.csv')
s3_client.upload_file('matricula_norte.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/matricula_norte.csv')
s3_client.upload_file('matricula_sudeste.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/matricula_sudeste.csv')
s3_client.upload_file('matricula_sul.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/matricula_sul.csv')
s3_client.upload_file('MD5_microdados_ed_basica_2020.txt',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/MD5_microdados_ed_basica_2020.txt')
s3_client.upload_file('matricula_turmas.csv',
                      'datalake-anacrsoares-688125619919',
                      'raw-data/dados-censo-2020/matricula_turmas.csv')'''