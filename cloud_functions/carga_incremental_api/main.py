import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import ast
import datetime
from io import StringIO

def add_data(file):
    # Step 1: Obtener el archivo desde el bucket de almacenamiento
    client = storage.Client()
    project_id = 'hoteles-396000'
    bucket_name = 'carga_inicial_test'
    archivo_nombre = 'reviews-google-place.csv'
    
    # Convertir los datos a un DataFrame
    api_data= pd.read_csv('gs://' + bucket_name + '/' + archivo_nombre, on_bad_lines='skip')

    # Big Query
    cliente = bigquery.Client()
    
    sql_guest = """SELECT COUNT(*) FROM `hoteles-396000.hoteles.guests`"""
    count_guest = cliente.query(sql_guest).to_dataframe().iloc[0, 0]

    sql_lodging = """SELECT COUNT(*) FROM `hoteles-396000.hoteles.lodgings`"""
    count_lodging = cliente.query(sql_lodging).to_dataframe().iloc[0, 0]

    sql_review = """SELECT COUNT(*) FROM `hoteles-396000.hoteles.reviews`"""
    count_review = cliente.query(sql_review).to_dataframe().iloc[0, 0]
    
    # Step 1
    reviews_data = api_data.copy()
    reviews_data['reviews'] = reviews_data['reviews'].apply(ast.literal_eval)
    reviews_data = reviews_data.explode('reviews')
    reviews_data.reset_index(drop=True, inplace=True)

    reviews_data.dropna(subset=['reviews'], inplace=True)
    reviews_data.reset_index(drop=True, inplace=True)

    # Extraer el valor del estado y guardar en la columna 'state'
    reviews_data['state'] = reviews_data['address'].str.split(', ').str[-2].str.split(' ').str[0]
    reviews_data.drop(columns=['type','rating'],inplace=True)

    # Desanidar columna 'reviews' en columnas separadas
    reviews_nested = pd.DataFrame(reviews_data['reviews'].tolist())

    # Extraer el conjunto de números entre "contrib/" y "/reviews" y guardarlos en la columna 'author_id'
    reviews_nested['author_id'] = reviews_nested['author_url'].str.extract(r'contrib/(\d+)/reviews')

    reviews_df = pd.concat([reviews_data, reviews_nested], axis=1)
    reviews_df.dropna(subset=['language'], inplace=True)
    reviews_df.reset_index(drop=True, inplace=True)
    reviews_df['time'] = reviews_df['time'].apply(lambda x: datetime.datetime.fromtimestamp(x).date())

    # Step 2
    # TABLA DIMENSIONAL GUEST
    guest_df = reviews_df.copy()
    guest_df = guest_df[['author_id','author_name']]
    guest_df = guest_df.rename(columns={"author_id":"guest_code","author_name":"guest_name"})

    guest_df= guest_df.drop_duplicates(subset=['guest_code'])
    #guest_df.insert(count_guest, 'guest_id', range(1, 1 + len(guest_df)))

    guest_df['guest_id'] = range(count_guest+1, count_guest+1 + len(guest_df))

    # Obtiene la fecha y hora actual
    current_datetime = datetime.datetime.now()

    # Agrega la columna 'date' con la fecha y hora actual
    guest_df['created_date'] = current_datetime
    guest_df['updated_date'] = current_datetime

    # Step 3
    # TABLA DIMENSIONAL LODGINGS
    lodgings_df = reviews_df.copy()
    lodgings_df = lodgings_df.rename(columns={"place_id":"lodging_code","name":"lodging_name"})
    lodgings_df['latitude'] = lodgings_df['latitude'].astype(str)
    lodgings_df['longitude'] = lodgings_df['longitude'].astype(str)
    lodgings_df = lodgings_df[['lodging_code','lodging_name','latitude','longitude','state']]
    lodgings_df= lodgings_df.drop_duplicates(subset=['lodging_code'])
    lodgings_df.reset_index(drop=True,inplace=True)
    #lodgings_df.insert(0, 'lodging_id', range(1, 1 + len(lodgings_df)))
    lodgings_df['lodging_id'] = range(count_lodging+1, count_lodging+1 + len(lodgings_df))

    # Agrega la columna 'date' con la fecha y hora actual
    lodgings_df['created_date'] = current_datetime
    lodgings_df['updated_date'] = current_datetime

    # Step 4
    # TABLA DE HECHOS REVIEWS
    reviews_df = reviews_df.rename(columns={"place_id":"lodging_code","author_id":"guest_code",
                        "text":"review","time":"date"})
                
    reviews_df = reviews_df[['lodging_code','guest_code','date','review','rating']]
    reviews_df.reset_index(drop=True,inplace=True)

    df1 = pd.merge(reviews_df,lodgings_df,on="lodging_code",how="right")

    df2 = pd.merge(df1,guest_df,on="guest_code",how="right")
    #df2.insert(0, 'review_id', range(1, 1 + len(df2)))
    df2['review_id'] = range(count_review+1, count_review+1 + len(df2))

    reviews_final = df2[['review_id','lodging_id','guest_id','date','review','rating']]
    # Convertir la columna "fecha" a tipo de dato datetime
    reviews_final['date'] = pd.to_datetime(reviews_final['date'])
    # Agrega la columna 'date' con la fecha y hora actual
    reviews_final['created_date'] = current_datetime
    reviews_final['updated_date'] = current_datetime

    # Envía el dataframe 'guest_df' a la tabla
    guest_df.to_gbq(destination_table = 'hoteles.' + 'guests_temp',
                        project_id = project_id, 
                        if_exists = 'append', progress_bar=False)
        
    # Envía el dataframe 'lodgings_df' a la tabla
    lodgings_df.to_gbq(destination_table = 'hoteles.' + 'lodgings_temp',
                        project_id = project_id, 
                        if_exists = 'append', progress_bar=False)
        
    # Envía el dataframe 'reviews_df' a la tabla
    reviews_final.to_gbq(destination_table = 'hoteles.' + 'reviews_temp',
                        project_id = project_id, 
                        if_exists = 'append', progress_bar=False)
    
    return 'Load Data Success'