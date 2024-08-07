import requests
import pandas as pd
import ast
import psycopg2

# Definir las credenciales de Redshift en variables
redshift_dbname = 'data-engineer-database'
redshift_user = 'jjuanaherrera_coderhouse'
redshift_password = '7g2q21gCL3'
redshift_host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
redshift_port = '5439'

# Parámetros
lat = "40.7128"  
lon = "-74.0060"  
api_key = "f7aaaedfbc71575b10ca84c59c435edc"  

# URL de la API con parámetros incluidos
url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&&appid={api_key}"

# Solicitud GET
response = requests.get(url)

# Verifica si solicitud fue exitosa
if response.status_code == 200:
    data = response.json()
    print("Datos del clima para la ubicación especificada:")

    # Extraer la lista de predicciones
    forecast_list = data['list']
    
    # DataFrame a partir de la lista de predicciones
    df = pd.DataFrame.from_dict(forecast_list).rename(columns={'dt_txt':'Time of data forecasted','wind':'Wind','weather':'Weather condition','main':'Temperature','pop': 'Probability of precipitation', 'sys': 'Part of the day with probability of precipitation','rain':'Rain volume for last 3 hours','visibility':'Average visibility','dt':'Time of data forecasted Unix Timestamp'})
    df['Time of data forecasted Unix Timestamp'] = pd.to_datetime(df['Time of data forecasted Unix Timestamp'],unit='s')
    df.drop(columns=['clouds'], inplace=True)

    # Expandir 'Weather condition'
    df['Weather condition'] = df['Weather condition'].apply(lambda x: ', '.join([item['description'] for item in x]))
    
    # Expandir 'Wind' si contiene datos en formato de diccionario
    if 'speed' in df['Wind'].iloc[0]:
        df[['Wind Speed', 'Wind Direction']] = df['Wind'].apply(lambda x: pd.Series([x.get('speed', None), x.get('deg', None)]))
        df.drop(columns=['Wind'], inplace=True)

    # Expandir 'Temperature'
    if isinstance(df['Temperature'].iloc[0], dict):
        temp_columns = ['Temp', 'Feels Like', 'Temp Min', 'Temp Max']
        temp_data = df['Temperature'].apply(lambda x: pd.Series({
            'Temp': x.get('temp', None),
            'Feels Like': x.get('feels_like', None),
            'Temp Min': x.get('temp_min', None),
            'Temp Max': x.get('temp_max', None)
        }))
        df = pd.concat([df, temp_data], axis=1)
        df.drop(columns=['Temperature'], inplace=True)

    # Convertir {'pod': 'd'} y {'pod': 'n'} en 'day' y 'night'
    if isinstance(df['Part of the day with probability of precipitation'].iloc[0], dict):
        df['Part of the day with probability of precipitation'] = df['Part of the day with probability of precipitation'].apply(
            lambda x: 'day' if x.get('pod') == 'd' else 'night' if x.get('pod') == 'n' else None
        )

    # Extraer el valor numérico de la columna 'Rain volume for last 3 hours'
    df['Rain volume for last 3 hours'] = df['Rain volume for last 3 hours'].apply(
        lambda x: x.get('3h', 0) if isinstance(x, dict) else 0
    )

    # Mostrar el DataFrame
    print(df.head)
else:
    print(f"Error en la solicitud: {response.status_code}")

# Conectar a Redshift
conn = psycopg2.connect(
        dbname=redshift_dbname,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port
    )

cur = conn.cursor()

# Consulta SQL
create_table_query = """
CREATE TABLE weather_forecast (
    time_of_data_forecasted TIMESTAMP,
    weather_condition VARCHAR(255),
    wind_speed FLOAT,
    wind_direction FLOAT,
    temp FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    probability_of_precipitation FLOAT,
    rain_volume_for_last_3_hours FLOAT DEFAULT 0,
    average_visibility FLOAT,
    part_of_the_day_with_probability_of_precipitation VARCHAR(10)
);
"""

# Ejecutar la consulta
cur.execute(create_table_query)

# Confirmar los cambios y cerrar la conexión
conn.commit()
cur.close()
conn.close()