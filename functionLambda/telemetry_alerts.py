import boto3
import json
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("AlertConfig")

sns = boto3.client("sns")

# SNS topics ARNs (cambiar por los tuyos)
SNS_SPEED = "arn:aws:sns:us-east-2:491934530980:alert-speed"
SNS_FUEL = "arn:aws:sns:us-east-2:491934530980:alert-fuel"
SNS_TEMP = "arn:aws:sns:us-east-2:491934530980:alert-temperature"
SNS_HOURS = "arn:aws:sns:us-east-2:491934530980:alert-hours"

def lambda_handler(event, context):

    # El IoT rule envía el mensaje en "event"
    data = event

    vehicle_id = data.get("device_id")
    speed = data.get("speed")
    fuel = data.get("fuel")
    temp = data.get("temperature")
    now = datetime.utcnow().hour

    # Obtener configuración GLOBAL
    cfg = table.get_item(Key={"config_id":"GLOBAL"})["Item"]

    # Leer umbrales
    speed_limit = cfg["speed_limit"]
    low_fuel_pct = cfg["low_fuel_pct"]
    temp_high = cfg["temp_high"]
    start_h = cfg["allowed_hours_start"]
    end_h = cfg["allowed_hours_end"]

    # Lista de alertas generadas
    alerts = []

    # 1. Exceso de velocidad
    if speed > speed_limit:
        msg = f"[{vehicle_id}] Exceso de velocidad: {speed} km/h (límite {speed_limit})"
        sns.publish(TopicArn=SNS_SPEED, Message=msg)
        alerts.append(msg)

    # 2. Nivel bajo de combustible
    if fuel < low_fuel_pct:
        msg = f"[{vehicle_id}] Nivel bajo de combustible: {fuel}% (mín {low_fuel_pct}%)"
        sns.publish(TopicArn=SNS_FUEL, Message=msg)
        alerts.append(msg)

    # 3. Temperatura motor alta
    if temp > temp_high:
        msg = f"[{vehicle_id}] Temperatura alta: {temp}°C (máx {temp_high}°C)"
        sns.publish(TopicArn=SNS_TEMP, Message=msg)
        alerts.append(msg)

    # 4. Fuera de horario permitido
    if not (start_h <= now <= end_h):
        msg = f"[{vehicle_id}] Conducción fuera de horario permitido ({now}h)"
        sns.publish(TopicArn=SNS_HOURS, Message=msg)
        alerts.append(msg)

    return {
        "alerts_triggered": len(alerts),
        "details": alerts
    }
