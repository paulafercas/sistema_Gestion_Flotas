import json
import boto3
from decimal import Decimal # 👈 ¡CRÍTICO!

# Inicialización de DynamoDB (fuera de la función)
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('SUMO_Vehicle_Data') # <-- ¡Aquí se define 'table'!

def lambda_handler(event, context):
    try:
        # 1. TIMESTAMP (Asumimos que viene como string y lo convertimos a INT)
        # Esto se necesita para la Clave de Ordenación
        event['timestamp'] = int(event['timestamp']) 
        
        # 2. CONVERSIÓN DE DECIMALES (lat, lon, ang)
        # Es NECESARIO usar Decimal(str(...)) para evitar errores de precisión.
        event['lat'] = Decimal(str(event['lat']))
        event['lon'] = Decimal(str(event['lon']))
        event['ang'] = Decimal(str(event['ang'])) 
        
        # 3. ESCRITURA
        response = table.put_item(
           Item=event
        )
        
        print(f"Escritura exitosa. ID de vehículo: {event['device_id']}")
        
        # ... (retorno exitoso) ...
        
    except Exception as e:
        # El Traceback te está diciendo que el error ocurre justo antes de esta línea.
        print(f"Error al escribir en DynamoDB: {e}") 
        raise e