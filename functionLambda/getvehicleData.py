import json
import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = 'SUMO_Vehicle_Data'
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    try:
        # La solicitud POST de API Gateway pone el JSON del cuerpo en 'event['body']'
        body = json.loads(event['body'])
        device_id = body.get('vehicleId') 
        
        if not device_id:
            return {
                'statusCode': 400, 
                'body': json.dumps({'error': 'Vehicle ID is missing'})
            }

        # 1. Consultar DynamoDB para obtener el último registro por Partition Key
        response = table.query(
            KeyConditionExpression='device_id = :d_id',
            ExpressionAttributeValues={':d_id': device_id},
            Limit=1,
            # ScanIndexForward=False ordena por timestamp (Sort Key) en orden descendente
            ScanIndexForward=False 
        )
        
        # 2. Formatear la Respuesta
        if response['Items']:
            item = response['Items'][0]
            
            # CRÍTICO: Convertir Decimal de DynamoDB a float para que JSON/el navegador lo entienda
            for key, value in item.items():
                if isinstance(value, Decimal):
                    item[key] = float(value)
            
            return {
                'statusCode': 200, 
                'body': json.dumps(item) # Devuelve el último registro
            }
        else:
            return {
                'statusCode': 404, 
                'body': json.dumps({'error': 'Vehicle not found'})
            }
            
    except Exception as e:
        print(f"Error en la consulta: {e}")
        return {
            'statusCode': 500, 
            'body': json.dumps({'error': str(e)})
        }