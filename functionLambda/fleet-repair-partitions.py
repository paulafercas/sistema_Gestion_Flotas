import boto3
import time

athena = boto3.client('athena')

DATABASE = "fleet_db"
OUTPUT = "s3://fleet-partitions-results/"  # <-- Cambiar a tu bucket

def lambda_handler(event, context):
    query = "MSCK REPAIR TABLE fleet_db.telemetry_data;"

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT}
    )

    query_execution_id = response["QueryExecutionId"]

    # Opcional: esperar el estado (útil para logs)
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result["QueryExecution"]["Status"]["State"]

        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            print("Query finished with state:", state)
            break

        time.sleep(1)

    return {"status": "done", "query_id": query_execution_id}