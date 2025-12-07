import os
import sys
import traci
import xml.etree.ElementTree as ET
import json
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import ssl
import time

#--------Lectura de egde--------------
def read_route_vehicle(dir_file, veh_id_start, veh_id_end):
    # Inicializar diccionarios
    dic_start = {}
    dic_via = {}
    dic_end = {}
    # 1. Lectrua y Parsear el XML directamente desde el archivo
    tree = ET.parse(os.path.join(os.getcwd(), dir_file) )
    root = tree.getroot()
    # Iterar sobre todos los elementos 'trip'
    for trip in root.findall('trip'):
        veh_id = trip.get('id')
        if veh_id in [f'veh_{i}' for i in range(veh_id_start, veh_id_end+1)]:
            # Extracción de elementos
            dic_start[veh_id] = trip.get('from')
            dic_end[veh_id] = trip.get('to')
            via_data = trip.get('via')
            if via_data:
                dic_via[veh_id] = via_data.split()
            else:
                dic_via[veh_id] = [] # Si 'via' está vacío o no existe

    return  dic_start, dic_via, dic_end

# --- Configuracion Path ---
if 'SUMO_HOME' in os.environ:
    tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
    sys.path.append(tools)
else:
    sys.exit("Declara la variable SUMO_HOME")

#------Calculo ruta------
def calculate_route(origen, via, destino, type_ID, routeID):
    """
    Calcula la ruta segmento a segmento para llenar los huecos entre edges
    que no están conectados directamente.
    """
    # Lista de hitos: Origen -> Via 1 -> Via 2 -> Destino
    hitos = [origen] + via + [destino]
    ruta_final_edges = []
    
    for i in range(len(hitos) - 1):
        inicio = hitos[i]
        fin = hitos[i+1]
        # Pedimos a SUMO que encuentre el camino entre estos dos puntos
        try:
            ruta_segmento = traci.simulation.findRoute(inicio, fin, vType=type_ID)
        except traci.exceptions.TraCIException as e:
            print(f"Error calculando tramo {inicio} -> {fin}: {e}")
            return None
        edges_segmento = list(ruta_segmento.edges)
        if not edges_segmento:
            print(f"No hay camino posible entre {inicio} y {fin}")
            return None
        # Evitamos duplicar el edge de conexión (el fin del segmento A es el inicio del segmento B)
        if len(ruta_final_edges) > 0:
            # Si el último edge acumulado es igual al primero de este nuevo segmento, lo saltamos
            if ruta_final_edges[-1] == edges_segmento[0]:
                ruta_final_edges.extend(edges_segmento[1:])
            else:
                ruta_final_edges.extend(edges_segmento)
        else:
            ruta_final_edges.extend(edges_segmento)

    if ruta_final_edges:
        try:
            traci.route.add(routeID, ruta_final_edges)
            print(f"Ruta válida guardada como '{routeID}' ({len(ruta_final_edges)} edges).")
        except traci.exceptions.TraCIException as e:
            print(f"Advertencia ruta: {e}")
    else:
        print("No se pudo calcular la ruta. El script fallará al intentar reinsertar.")   

#---------Funcion que instala Deamon---------
def installVehicleDeamon(veh_id, route_id, type_id, color, origen, via, destino):
    # Se crea la clase y se agrega la funcion DEAMON a Traci
    try:
        daemon = VehicleDeamon(veh_id, route_id, type_id, color, origen, via, destino)
        traci.addStepListener(daemon)
        print(f"Daemon activado del vehiculo {veh_id}.")
    except traci.exceptions.TraCIException as e:
        print(f"Error de TraCI al inicializar daemon: {e}")

#---------Clase DAEMON---------
class VehicleDeamon(traci.StepListener):
    def __init__(self, veh_id, route_id, type_id, color, origen, via, destino):
        # Definicion de atributos
        self.veh_id = veh_id
        self.route_id = route_id
        self.type_id = type_id
        self.color = color
        #Inserción de ruta
        calculate_route(origen, via, destino, type_id, route_id)
    
    def step(self,t):
        #solo corre en el background de la simulacion
        arrived = traci.simulation.getArrivedIDList()
        if self.veh_id in arrived:
            # Disparamos la funcion externa
            self.reset_vehicle()
        return True
    
    def reset_vehicle(self):
        try:
            traci.vehicle.add(
                vehID=self.veh_id,
                routeID=self.route_id,
                typeID=self.type_id,
                depart="now",
                departLane="best",
                departPos="base",
                departSpeed="max"
            )
            traci.vehicle.setColor(self.veh_id, self.color)
            print(f"Vehículo {self.veh_id} reinsertado con éxito.")
        except traci.exceptions.TraCIException as e:
            print(f"Error al reinsertar: {e}")


#---------Definicion de constantes---------------
SUMO_CONFIG = r"Medellin traffic\osm.sumocfg.xml"
VEH_ID_START = 1
VEH_ID_END = 20 
VEH_IDS = [f"veh_{i}" for i in range(VEH_ID_START, VEH_ID_END+1)]
TYPE_ID = "pt_bus" 
COLOR = (0, 0, 255, 255)
dic_origen, dic_via, dic_destino= read_route_vehicle(r'Medellin traffic\osm_pt.rou.xml', VEH_ID_START, VEH_ID_END)


#Definimos la conexión 
# --- Configuración MQTT de AWS IoT Core ---
# ⚠️ REEMPLAZA ESTOS VALORES CON TUS CREDENCIALES DE AWS IOT CORE ⚠️
AWS_ENDPOINT = "ag6t3wyqi6umd-ats.iot.us-east-2.amazonaws.com" # Ej: a1b2c3d4e5f6g7.iot.us-east-1.amazonaws.com
MQTT_PORT = 8883 # Puerto seguro TLS
MQTT_TOPIC = "fleet/sumo/trafficdata"
ROOT_CA = r"Medellin traffic\keys\AmazonRootCA1.pem" # Certificado raíz de Amazon (AmazonRootCA1.pem)
CERT_FILE = r"Medellin traffic\keys\1b4263579a7b988b3c204262f1c36d7f84f59d5d9f4da47364c70cf045107192-certificate.pem.crt" # Certificado de tu "Thing"
PRIVATE_KEY = r"Medellin traffic\keys\1b4263579a7b988b3c204262f1c36d7f84f59d5d9f4da47364c70cf045107192-private.pem.key" # Clave privada de tu "Thing"
PUBLISH_INTERVAL = 10  # segundos

# ------------------------------------------------------------------
# ... (El resto de tus funciones: read_route_vehicle, calculate_route, installVehicleDeamon, VehicleDeamon) ...
# ------------------------------------------------------------------

# --- Funciones de Conexión MQTT ---
def connect_mqtt():
    """Configura y conecta el cliente MQTT a AWS IoT Core."""
    client = mqtt.Client(client_id="simulator_master")
    
    # Configuración de TLS con certificados de AWS
    try:
        client.tls_set(
            ca_certs=ROOT_CA,
            certfile=CERT_FILE,
            keyfile=PRIVATE_KEY,
            tls_version=mqtt.ssl.PROTOCOL_TLSv1_2 # Protocolo recomendado por AWS
        )
        client.connect(AWS_ENDPOINT, MQTT_PORT, keepalive=60)
        print("Conexión MQTT a AWS establecida con éxito.")
        return client
    except FileNotFoundError:
        sys.exit(f"Error: No se encontraron los archivos de certificado. Revisa las rutas: {ROOT_CA}, {CERT_FILE}, {PRIVATE_KEY}")
    except Exception as e:
        print(f"Error al conectar con MQTT: {e}")
        return None

#---------------Funcion de Extraccion---------------
def get_vehicle_data(vehicle_id):
    # --- Datos reales desde SUMO ---
    x, y = traci.vehicle.getPosition(vehicle_id)
    lat, lon = traci.simulation.convertGeo(x, y, fromGeo=False)
    ang = traci.vehicle.getAngle(vehicle_id)
    speed_kmh = traci.vehicle.getSpeed(vehicle_id) * 3.6
    # --- Simulaciones de telemetría ---
    rpm = int(speed_kmh * 60)  # Simulación
    consumo_fuel = round(speed_kmh * 0.02, 2)  # L/h ficticio
    engine_temp = round(70 + speed_kmh * 0.5, 2)
    timestamp = datetime.now(timezone.utc).isoformat()
    distance_m = traci.vehicle.getDistance(vehicle_id)
    # Construimos un paquete centralizado
    data = {
        "device_id": vehicle_id,
        "timestamp": timestamp,
        "lat": lat,
        "lon": lon,
        "ang": ang,
        "distance_m": distance_m,
        "speed": round(speed_kmh, 2),
        "rpm": rpm,
        "fuel_consumption": consumo_fuel,
        "engine_temperature": engine_temp,
    }

    return data

#-------------Envio MQTT------------------------
def publish_vehicle_data(data, client):
    vehicle_id = data["device_id"]
    # ----- Mensaje GPS -----
    gps_msg = {
        "device_id": vehicle_id,
        "timestamp": data["timestamp"],
        "lat": data["lat"],
        "lon": data["lon"],
        "ang": data["ang"],
    }
    # ----- Mensaje Telemetría -----
    telemetry_msg = {
        "device_id": vehicle_id,
        "timestamp": data["timestamp"],
        "speed": data["speed"],
        "distance_m" : data["distance_m"],
        "rpm": data["rpm"],
        "fuel_consumption": data["fuel_consumption"],
        "engine_temperature": data["engine_temperature"],
    }
    topic_gps = f"fleet/{vehicle_id}/gps"
    topic_tel = f"fleet/{vehicle_id}/telemetry"
    client.publish(topic_gps, json.dumps(gps_msg))
    client.publish(topic_tel, json.dumps(telemetry_msg))
    print(f"[{vehicle_id}] Publicado GPS + Telemetría")

#-----------main---------------
def main():
    mqtt_client = None # Variable para almacenar el cliente MQTT

    try:
        # --- Parte de AWS MQTT ---
        print("Intentando conectar a AWS IoT Core...")
        # 1. CONECTAR: Llama a tu función para conectar y obtener el cliente
        mqtt_client = connect_mqtt() 
        # 2. INICIAR HILO: Pone el cliente MQTT a correr en segundo plano
        mqtt_client.loop_start()
        sumoCmd = ["sumo-gui", "-c", SUMO_CONFIG, "--start"]
        traci.start(sumoCmd)
        
        #1. Instalacion de funcion DAEMON y calculo de ruta
        for idx, veh_id in enumerate(VEH_IDS, start=1):
            route_id = f"route_{idx}"
            installVehicleDeamon(veh_id, route_id, TYPE_ID, COLOR, dic_origen[veh_id], dic_via[veh_id], dic_destino[veh_id])
  
        #2. Bucle Simulación
        last_send_timestamp = 0
        while traci.simulation.getMinExpectedNumber() > 0:
            traci.simulationStep()
            # Publicar cada 10 segundos
            now = time.time()
            if now - last_send_timestamp >= PUBLISH_INTERVAL:
                for vehicle_id in VEH_IDS:
                    if vehicle_id not in traci.vehicle.getIDList():
                        continue
                    data = get_vehicle_data(vehicle_id)
                    publish_vehicle_data(data, mqtt_client)
                last_send_timestamp = now
            # -------------------------------------------------------------------
        
    except traci.exceptions.TraCIException as e:
        print(f"Error de TraCI en main: {e}")
    except Exception as e:
        print(f"Error inesperado en main: {e}")
    finally:
        # 3. DESCONEXIÓN: Asegurar que el cliente MQTT se cierre limpiamente
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            print("Cliente MQTT desconectado.")
        traci.close()
       
if __name__ == "__main__":
    main()
