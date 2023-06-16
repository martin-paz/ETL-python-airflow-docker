from datetime import timedelta,datetime
from pathlib import Path
import requests
import json
import pandas as pd
import os
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator

dag_path = os.getcwd()     #path original.. home en Docker

url='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
with open('./keys/db.txt','r') as f:
    data_base=f.read()
with open('./keys/user.txt','r') as f:
    user=f.read()
with open('./keys/pwd.txt','r') as f:
    pwd=f.read()

redshift_conn= {
    'host':url,
    'username':user,
    'database':data_base,
    'pwd':pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'MartinPaz',
    'start_date': datetime(2023,6,16),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

nasa_asteroid = DAG(
    dag_id='Asteroid_NeoWs',
    default_args=default_args,
    description='Agrega datos de la nasa sobre asteroides ',
    schedule_interval="@daily",
    catchup=False
)

# funcion de extraccion de datos
#EXTRACT
def extract(start_date,end_date):
    try:
        START_DATE = start_date #'2023-01-01'
        END_DATE = end_date #'2023-01-01'
        API_KEY='Mj4hNXe0IY3dwl2ypj4Cc1jEsTgGHu5O1P2KdvGj'
        url=f'https://api.nasa.gov/neo/rest/v1/feed?start_date={START_DATE}&end_date={END_DATE}&api_key={API_KEY}'
        response=requests.get(url)
        if response:
            print('Success!')
            data = response.json()
            with open(f'{dag_path}/raw_data/Asteroids_NeoWs.json','w') as json_file:
                json.dump(data, json_file)
        else:
            print('An error has occurred')
    except ValueError as e:
        print('Formato datetime  deberia ser %Y-%m-%d', e)
        raise e

# Funcion de transformacion en tabla
#TRANSFORM   
def transform():
    with open(f'{dag_path}/raw_data/Asteroids_NeoWs.json','r') as json_file:
        raw_data=json.load(json_file)
    datas=[]
    #loop para extraer los topicos de interes del json en un diccionario de python
    for r in raw_data['near_earth_objects']['2023-01-01']:
        datas.append({
            'id':r['id'],
            'neo_reference_id':r['neo_reference_id'],
            'name':r['name'],
            'absolute_magnitude_h':r['absolute_magnitude_h'],
            'estimated_diameter_min':r['estimated_diameter']['meters']['estimated_diameter_min'],
            'estimated_diameter_max':r['estimated_diameter']['meters']['estimated_diameter_max'],
            'potentially_hazardous_asteroid':r['is_potentially_hazardous_asteroid']
        })
    #Cargar el diccionario en un dataframe
    data=pd.DataFrame(datas)
    data['id']=data['id'].astype('int64')
    data['neo_reference_id']=data['neo_reference_id'].astype('int64')
    data.to_csv(f'{dag_path}/processed_data/Asteroids_NeoWs.csv', index=False, mode='w')

from psycopg2.extras import execute_values
###Insertar Datos##
def load():
    try:
        table_name='asteroides_neows'
        data=pd.read_csv(f'{dag_path}/processed_data/Asteroids_NeoWs.csv')
        print(data.shape)
        print(data.head())
        dtypes= data.dtypes
        cols= list(dtypes.index )
        tipos= list(dtypes.values)
        type_map = {'int64': 'INT','int64': 'INT','object': 'VARCHAR(50)','float64':'DECIMAL(4,2)','float64':'DECIMAL(10,6)','float64':'DECIMAL(10,6)','bool':'boolean'}
        sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
        
        # Combine column definitions into the CREATE TABLE statement
        column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
        #Crear la tabla
        table_schema = f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(column_defs)}
            );
            """
        conn=psycopg2.connect(
            host=url,
            dbname=redshift_conn['database'],
            user=redshift_conn['username'],
            password=redshift_conn['pwd'],
            port='5439'
        )
        cur = conn.cursor()
        cur.execute(table_schema)
    # Generar los valores a insertar
        values = [tuple(x) for x in data.to_numpy()]
        # Definir el INSERT
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
        

    # Execute the transaction to insert the data
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        print('Finished process')
    except Exception as ex:
        print("Unable to connect to Redshift.")
        print(ex)
    #finally:
        #conn.close()
        #cur.close()
        #print('Successfully Completed')

# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extract,
    op_args=['2023-01-01','2023-01-01'],
    dag=nasa_asteroid,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transform,
    dag=nasa_asteroid,
)

#3. Cargar datos
task_3 = PythonOperator(
    task_id='Cargar_datos_a_redshift',
    python_callable=load,
    dag=nasa_asteroid,
)

task_1 >> task_2 >> task_3