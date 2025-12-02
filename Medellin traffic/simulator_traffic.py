import os
import sys
import traci
import xml.etree.ElementTree as ET

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

#-----------main---------------
def main():
    try:
        sumoCmd = ["sumo-gui", "-c", SUMO_CONFIG, "--start"]
        traci.start(sumoCmd)

        #1. Instalacion de funcion DAEMON y calculo de ruta
        for idx, veh_id in enumerate(VEH_IDS, start=1):
            route_id = f"route_{idx}"
            installVehicleDeamon(veh_id, route_id, TYPE_ID, COLOR, dic_origen[veh_id], dic_via[veh_id], dic_destino[veh_id])
  
        #2. Bucle Simulación
        while traci.simulation.getMinExpectedNumber() > 0:
            traci.simulationStep() # El Daemon se ejecuta aquí dentro automáticamente
            #---Aqui va el codigo para recolectar datos e envialos por---
            #-------- a envialos por MQTT--------------------------------
            #------------------------------------------------------------
            #------------------------------------------------------------
            #------------------------------------------------------------
        
    except traci.exceptions.TraCIException as e:
        print(f"Error de TraCI en main: {e}")
    except Exception as e:
        print(f"Error inesperado en main: {e}")
    finally:
        traci.close()
       
if __name__ == "__main__":
    main()
