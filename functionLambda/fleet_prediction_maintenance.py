import boto3
import awswrangler as wr
import pandas as pd
import json
import os
import uuid
from datetime import datetime

# Inicializar clientes
sagemaker_runtime = boto3.client('sagemaker-runtime')
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

# Variables de entorno
ENDPOINT_NAME = os.environ.get('ENDPOINT_NAME')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')
TABLE_NAME = os.environ.get('DYNAMODB_TABLE')
GLUE_DB = os.environ.get('GLUE_DATABASE')
GLUE_TABLE = os.environ.get('GLUE_TABLE')
ATHENA_OUTPUT = os.environ.get('ATHENA_OUTPUT')

def get_telemetry_data():
    """
    Consulta Athena para obtener datos de los últimos 4 minutos.
    Asume que el campo timestamp es compatible con SQL standard.
    """
    #query = f"""
    #SELECT device_id, speed, fuel_consumption, engine_temperature
    #FROM "{GLUE_DB}"."{GLUE_TABLE}"
    #WHERE timestamp >= current_timestamp - interval '4' minute
    #"""

    query = f"""
    SELECT device_id, speed, fuel_consumption, engine_temperature
    FROM "{GLUE_DB}"."{GLUE_TABLE}"
    WHERE timestamp >= (
        SELECT MAX(timestamp) 
        FROM "{GLUE_DB}"."{GLUE_TABLE}"
    ) - interval '4' minute
    """
    
    # awswrangler maneja la espera de la query y devuelve un DataFrame de Pandas directamente
    df = wr.athena.read_sql_query(
        sql=query,
        database=GLUE_DB,
        s3_output=ATHENA_OUTPUT
    )
    return df

def calculate_statistics(df):
    """
    Agrupa por device_id y calcula las estadísticas requeridas.
    """
    if df.empty:
        return pd.DataFrame()

    # Agrupación y cálculo
    stats = df.groupby('device_id').agg(
        speed_mean=('speed', 'mean'),
        speed_std=('speed', 'std'),
        fuel_mean=('fuel_consumption', 'mean'),
        fuel_std=('fuel_consumption', 'std'),
        temp_mean=('engine_temperature', 'mean'),
        temp_max=('engine_temperature', 'max')
    ).reset_index()
    
    # Rellenar NaNs con 0 (en caso de que solo haya un registro y std sea NaN)
    stats.fillna(0, inplace=True)
    
    return stats

def lambda_handler(event, context):
    print("Iniciando proceso de inferencia de flota...")
    
    # 1. Obtener datos
    try:
        raw_df = get_telemetry_data()
        print(f"Registros obtenidos: {len(raw_df)}")
    except Exception as e:
        print(f"Error consultando Athena: {str(e)}")
        return {"status": "error", "message": str(e)}

    if raw_df.empty:
        return {"status": "success", "message": "No hay datos nuevos en los últimos 4 minutos."}

    # 2. Calcular estadísticas
    features_df = calculate_statistics(raw_df)
    
    table = dynamodb.Table(TABLE_NAME)
    alerts_sent = 0
    THRESHOLD = 0.7  # ajusta según tu modelo

    # 3. Iterar por cada vehículo
    for index, row in features_df.iterrows():
        device_id = row['device_id']
        # 1. Preparar el payload como una CADENA CSV (6 valores separados por comas)
        csv_payload = f"{row['speed_mean']},{row['speed_std']},{row['fuel_mean']},{row['fuel_std']},{row['temp_mean']},{row['temp_max']}"
        try:
            # 2. Invocar Endpoint de SageMaker
            response = sagemaker_runtime.invoke_endpoint(
                EndpointName=ENDPOINT_NAME,
                ContentType='text/csv',       
                Body=csv_payload               # <--- Enviamos la cadena CSV
            )
            # 3. Procesar la respuesta (que debería ser una cadena simple como "0.98" o "1")
            result_str = response['Body'].read().decode('utf-8')
            # Limpieza de la respuesta y conversión a float
            prediction_clean = result_str.strip().replace('[', '').replace(']', '')
            prediction_value = float(prediction_clean)
            if prediction_value>THRESHOLD:
                print(f"ALERTA: Vehículo {device_id} requiere mantenimiento.")
                
                # 5. Guardar en DynamoDB
                alert_id = str(uuid.uuid4())
                timestamp_now = datetime.now().isoformat()
                
                item = {
                    'alert_id': alert_id,
                    'device_id': device_id,
                    'alert_timestamp': timestamp_now,
                    'prediction_score': str(prediction_value),
                    'telemetry_stats': json.dumps({
                    'speed_mean': row['speed_mean'],
                    'speed_std': row['speed_std'],
                    'fuel_mean': row['fuel_mean'],
                    'fuel_std': row['fuel_std'],
                    'temp_mean': row['temp_mean'],
                    'temp_max': row['temp_max'],
                })
                }
                table.put_item(Item=item)
                
                # 6. Enviar notificación SNS
                sns_message = (
                    f"⚠️ Mantenimiento Requerido\n"
                    f"Vehículo ID: {device_id}\n"
                    f"Temp Max: {row['temp_max']}°C\n"
                    f"Velocidad Promedio: {row['speed_mean']}\n"
                    f"Hora: {timestamp_now}"
                )
                
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=sns_message,
                    Subject=f"Alerta Flota: {device_id}"
                )
                alerts_sent += 1

        except Exception as e:
            print(f"Error procesando dispositivo {device_id}: {str(e)}")
            continue

    return {
        "statusCode": 200,
        "body": json.dumps(f"Proceso completado. Alertas enviadas: {alerts_sent}")
    }