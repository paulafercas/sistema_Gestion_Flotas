import json
import boto3
import uuid

s3 = boto3.client('s3')
BUCKET = "fleet-telemetry"

def lambda_handler(event, context):

    data = event
    if isinstance(event, str):
        data = json.loads(event)

    device_id = data.get("device_id", "unknown")
    file_id = uuid.uuid4().hex

    key = f"telemetry/device_id={device_id}/{file_id}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    return {"status": "ok", "saved_to": key}