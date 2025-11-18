"""
En este código simulamos 20 vehículos con las siguientes 
características:
1. id
2. Tiempo en el cual se realizó el muestreo
3. latitud del vehículo
4. Longitud del vehículo
5. Velocidad del vehículo
6. rpm
7. cantidad del combustible (fuel)
8. Temperatura del motor
9. Dirección a la cual se encuentra el dispositivo (heading)
10. Kilometraje (odometer)

La idea es que cada uno de estos datos sean enviados a los servicios 
en la nube de AWS. 
"""

"""
Importamos las librerías necesarias. json es específica para 
establecer conexión entre el vehículo y AWS mediante MQTT
"""
import time, json, ssl, random 
import paho.mqtt.client as mqtt
from datetime import datetime, timezone

"""
Estos nombres son los necesarios para establecer conexión por MQTT 
"""
#Hace las veces de subscriptor, es el endpoint seguro de la instancia
#AWS IOT Core. AWS asigna a cada cuenta un endpoint seguro para MQTT
IOT_ENDPOINT = "your-iot-endpoint.amazonaws.com" 

#Descargar certificado a través Security → Certificates → Create certificate
#Este certificado deberá quedar en la misma carpeta del código
ROOT_CA = "AmazonRootCA1.pem" 

#Este es el certificado del dispositivo. En EC2 deben quedar guardados
#los certificados Device certificate (este), Private key y Public key
CERT = "V001-certificate.pem.crt"

#Esta llave está también en el certificado descargado con ROOT_CA
KEY = "V001-private.pem.key"

#Puerto seguro para MQTT
PORT = 8883

"""
Total de vehículos: 20 y se debe enviar la información cada 10 s
"""
NUM_VEHICLES = 20
PUBLISH_INTERVAL = 10  # seconds

# predefine simple routes (for lab generate lat/lon near a city)
BASE_LAT = 4.7110
BASE_LON = -74.0721

"""
Función para crear los datos necesarios en cada vehículo
Recibe el número de vehículos que quiere generar y recibe una lista
con el id del vehículo, latitud, longitud, velocidad, dirección,
Kilometraje
"""
def make_vehicle_list(n):
    vehicles = []
    for i in range(1, n+1):
        vid = f"V{i:03d}"
        # small random offset per vehicle
        vehicles.append({
            "vehicle_id": vid,
            "lat": BASE_LAT + random.uniform(-0.02, 0.02),
            "lon": BASE_LON + random.uniform(-0.02, 0.02),
            "speed": random.uniform(0, 80),
            "heading": random.uniform(0, 360),
            "odometer": random.uniform(10000, 200000)
        })
    return vehicles

#Llamamos la función para generar los vehículos
vehicles = make_vehicle_list(NUM_VEHICLES)

"""
Funcion que utiliza MQTT para confirmar que se realizó conexión 
exitosa. 
1. Client es el subscriptor
2. Userdata es el espacio utilizado para almacenar los datos
3. Flags es un diccionario que puede dar información adicional
4. RC: Return Code. Es el más importante, porque es el que confirma 
que sí se estableció conexión

| rc | Significado                       |
| -- | --------------------------------- |
| 0  | **Conexión exitosa**              |
| 1  | Protocolo incorrecto              |
| 2  | Identificador de cliente inválido |
| 3  | Servidor inaccesible              |
| 4  | Nombre/clave incorrectos          |
| 5  | No autorizado                     |

"""
def on_connect(client, userdata, flags, rc):
    print("Connected with rc:", rc)

#Le damos al cliente las variables definidads anteriormente
client = mqtt.Client()
client.tls_set(ca_certs=ROOT_CA, certfile=CERT, keyfile=KEY, tls_version=ssl.PROTOCOL_TLSv1_2)
client.on_connect = on_connect
client.connect(IOT_ENDPOINT, PORT, keepalive=60)
client.loop_start()

"""
Esta funcion crea un diccionario con los datos determinados del vehiculo
a inspeccionar.
Nótese que en esta parte estamos además tomando el tiempo en el cual
se reciben estos datos.
Luego, crea un topic asociado a cada vehículo
Por último transforma el diccionario en un mensaje json para enviarlo al
cliente a través de MQTT.
"""
def publish_gps(v):
    payload = {
        "vehicle_id": v["vehicle_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lat": v["lat"],
        "lon": v["lon"],
        "heading": v["heading"],
        "speed_kmh": round(v["speed"], 2)
    }
    topic = f"fleet/{v['vehicle_id']}/gps"
    client.publish(topic, json.dumps(payload), qos=1)

"""
Esta funcion publica la telemetria del vehículo seleccionado.
Con telemetría nos referimos a las condiciones mecánicas del 
vehículo. rpm del motor, temperatura del motor, brake_event:
si ocurrió alguna frenada brusca. Accel_event: si ocurrió 
alguna aceleración brusca y el kilometraje del vehículo.

Nuevamente transforma este diccionario en json para enviarlo 
mediante MQTT al cliente

Esta telemetría será usada después para hacer un mantenimiento
predictivo y alertas.
"""
def publish_telemetry(v):
    # random events
    rpm = 800 + v["speed"] * 30 + random.uniform(-100,100)
    fuel = max(5, random.uniform(10, 80))
    engine_temp = 70 + random.uniform(-5, 30) # occasionally high
    payload = {
        "vehicle_id": v["vehicle_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rpm": int(rpm),
        "fuel_pct": round(fuel,1),
        "engine_temp_c": round(engine_temp,1),
        "brake_event": random.random() < 0.01,
        "accel_event": random.random() < 0.02,
        "odometer_km": round(v["odometer"],1)
    }
    topic = f"fleet/{v['vehicle_id']}/telemetry"
    client.publish(topic, json.dumps(payload), qos=1)


"""
Esta funcion permite que los valores del vehiculo cambien continuamente
Especialmente el heading, la latitud, la longitud, la velocidad y el 
kilometraje (odometer)
"""
def step_vehicle(v):
    # simple movement: move a bit in random direction; change speed randomly
    v["heading"] = (v["heading"] + random.uniform(-10, 10)) % 360
    delta_km = v["speed"] * (PUBLISH_INTERVAL/3600.0)  # km
    # approx latitude/longitude change ~ (delta_km / 111km)
    v["lat"] += (delta_km / 111.0) * random.uniform(-0.5, 0.5)
    v["lon"] += (delta_km / (111.0 * abs(math.cos(math.radians(v["lat"])))) ) * random.uniform(-0.5, 0.5)
    v["speed"] = max(0, v["speed"] + random.uniform(-5, 5))
    v["odometer"] += delta_km



if __name__ == "__main__":
    import math
    try:
        #Ejecuta el simulador de forma continua
        while True: 
            for v in vehicles:
                # random behaviours
                if random.random() < 0.01:
                    # speed spike
                    v["speed"] = min(160, v["speed"] + random.uniform(30, 60))
                if random.random() < 0.005:
                    # sudden brake
                    v["speed"] = max(0, v["speed"] - random.uniform(20, 80))
                #Publicamos las variables de posición y telemetría
                publish_gps(v)
                publish_telemetry(v)
                #Variamos algunos parámetros del vehículo
                step_vehicle(v)
                #Espera cada 10 segundos para ejecutarse nuevamente el ciclo
            time.sleep(PUBLISH_INTERVAL)
    #El ciclo se interrumpe mediante ctrl + c de lo contrario queda 
    #corriendo para siempre
    except KeyboardInterrupt:
        client.loop_stop()
        client.disconnect()
