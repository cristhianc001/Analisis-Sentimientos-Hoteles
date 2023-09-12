import pandas as pd
from datetime import datetime
from google.cloud import storage


def carga_inicial(file):
    """
    Trabaja sobre los dataset 'reviews.csv', 'users.csv', 'lodgings.csv', 'reviews.csv' y 'calendario.csv' para la carga inicial
    """

    # Configura el cliente de Google Cloud Storage
    client = storage.Client()
     
    # Obtiene el nombre de tu bucket y archivo en Google Cloud Storage
    bucket_name = 'carga_inicial_test'
    archivo_users = 'users.csv'
    archivo_lodgings = 'lodgings.csv'
    archivo_reviews = 'reviews.csv'
    archivo_calendar = 'calendario.csv'

    # Cargamos los datos del CSV a los dataframes
    df_guests = pd.read_csv('gs://' + bucket_name + '/' + archivo_users, sep='|', on_bad_lines='skip')
    df_lodgings = pd.read_csv('gs://' + bucket_name + '/' + archivo_lodgings, sep='|', on_bad_lines='skip')
    df_reviews = pd.read_csv('gs://' + bucket_name + '/' + archivo_reviews, sep='|', on_bad_lines='skip')
    calendar = pd.read_csv('gs://' + bucket_name + '/' + archivo_calendar, sep='|', on_bad_lines='skip')


    # Renombra las columnas
    df_guests.rename(columns={'user_id': 'guest_code', 'user_name': 'guest_name'}, inplace=True)

    # Obtiene la fecha y hora actual
    current_datetime = datetime.now()

    # Agrega la columna 'date' con la fecha y hora actual
    df_guests['created_date'] = current_datetime
    df_guests['updated_date'] = current_datetime

    # Elimina duplicados en base a la columna 'review'
    df_guests= df_guests.drop_duplicates(subset=['guest_code'])
    df_guests.reset_index(drop=True,inplace=True)

    # Asigna 'guest_id' desde el 1 incrementalmente
    df_guests.insert(0, 'guest_id', range(1, 1 + len(df_guests)))




    # Renombra las columnas
    df_lodgings.rename(columns={'place_id': 'lodging_code', 'hotel_name': 'lodging_name'}, inplace=True)

    # Agrega la columna 'date' con la fecha y hora actual
    df_lodgings['created_date'] = current_datetime
    df_lodgings['updated_date'] = current_datetime

    # Elimina columnas que no se utilizarán
    df_lodgings.drop(columns=['category_name','address'], inplace=True)

    # Asigna 'lodging_id' desde el 1 incrementalmente
    df_lodgings.insert(0, 'lodging_id', range(1, 1 + len(df_lodgings)))

    # Cambia el tipo de datos de las columnas para no generar conflicto
    df_lodgings['latitude'] = df_lodgings['latitude'].astype(str)
    df_lodgings['longitude'] = df_lodgings['longitude'].astype(str)




    # Renombra las columnas
    df_reviews.rename(columns={'place_id': 'lodging_code','user_id': 'guest_code'}, inplace=True)
    
    # Realiza merge entre 'df_reviews' y 'df_lodgings' sobre la columna 'lodging_code'
    df1 = pd.merge(df_reviews,df_lodgings,on="lodging_code",how="right")

    # Realiza merge entre 'df1' y 'df_guests' sobre la columna 'guest_code'
    df2 = pd.merge(df1,df_guests,on="guest_code",how="right")
    
    # Sobreescribe 'df_reviews' con las columnas que necesitamos
    df_reviews = df2.copy()
    df_reviews = df_reviews[['date','rating','review','guest_id','lodging_id']]

    # Elimina duplicados en base a la columna 'review'
    df_reviews= df_reviews.drop_duplicates(subset=['review'])
    df_reviews.reset_index(drop=True,inplace=True)

    # Agrega la columna 'date' con la fecha y hora actual
    df_reviews['created_date'] = current_datetime
    df_reviews['updated_date'] = current_datetime

    # Asigna 'review_id' desde el 1 incrementalmente
    df_reviews.insert(0, 'review_id', range(1, 1 + len(df_reviews)))

    # Cambia el tipo de dato de la columna 'rating' de float a int
    df_reviews['rating'] = df_reviews['rating'].astype(int)


    # Envía el dataframe 'df_guests' a la tabla 'guests'
    df_guests.to_gbq(destination_table = 'hoteles.' + 'guests', 
    project_id = 'hoteles-396000', 
    if_exists = 'append', progress_bar=False)

    # Envía el dataframe 'df_lodgings' a la tabla 'lodgings'
    df_lodgings.to_gbq(destination_table = 'hoteles.' + 'lodgings', 
    project_id = 'hoteles-396000', 
    if_exists = 'append', progress_bar=False)

    # Envía el dataframe 'df_reviews' a la tabla 'reviews'
    df_reviews.to_gbq(destination_table = 'hoteles.' + 'reviews', 
    project_id = 'hoteles-396000', 
    if_exists = 'append', progress_bar=False)

    # Envía el dataframe 'calendar' a la tabla 'calendar'
    calendar.to_gbq(destination_table = 'hoteles.' + 'calendar', 
    project_id = 'hoteles-396000', 
    if_exists = 'append', progress_bar=False)
    
    return 'Success'