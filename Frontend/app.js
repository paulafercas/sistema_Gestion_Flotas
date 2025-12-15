// ====================================================
// ⚠️ 1. CONFIGURACIÓN AWS (¡Reemplazar estos valores!)
// ====================================================
const REGION = 'us-east-2'; 
const IDENTITY_POOL_ID = 'us-east-2:c2b05251-ae4e-46e0-92b3-fc545b9b9424'; // Tu Pool de Identidades de Cognito
const MAP_NAME = 'SUMO-Fleet-Map'; 
const PLACE_INDEX_NAME = 'SUMO-Reverse-Geocoding-Index';
const API_GATEWAY_URL = 'https://33hsvfie2g.execute-api.us-east-2.amazonaws.com/prod'; // Tu URL de invocación de API Gateway
const API_KEY = 'rcVrNOSfwOaBIuHQJ6XRe7R8hZDGLJzD2XZYH5IP'; // La clave que envías en el encabezado x-api-key

let map, marker; 

// ====================================================
// 2. INICIALIZACIÓN (Acceso No Autenticado)
// ====================================================

async function initializeMap() {
    AWS.config.region = REGION;
    
    // Obtener Credenciales No Autenticadas (Públicas)
    AWS.config.credentials = new AWS.CognitoIdentityCredentials({
        IdentityPoolId: IDENTITY_POOL_ID
    });

    try {
        await AWS.config.credentials.getPromise();
        const locationService = new AWS.Location({ region: REGION });
        const mapParams = { MapName: MAP_NAME };
        
        // Firmar la solicitud del estilo de mapa
        const styleUrl = locationService.getMapStyleStyleDescriptor(mapParams).response.request.url;

        // Inicializar MapLibre con la URL firmada
        map = new maplibregl.Map({
            container: 'map', 
            style: styleUrl,
            center: [-75.589, 6.208], // Centrado inicial en Medellín
            zoom: 12
        });
        
        map.addControl(new maplibregl.NavigationControl(), 'top-left');

    } catch (error) {
        console.error("Error al inicializar AWS o cargar el mapa (revisar Identity Pool y permisos IAM):", error);
        alert("Error crítico: No se pudo cargar el mapa. Revisa los permisos de Location Service.");
    }
}

// ====================================================
// 3. GEOCODIFICACIÓN INVERSA (Coordenadas a Dirección)
// ====================================================

async function getAddressFromCoordinates(lon, lat) {
    const locationService = new AWS.Location({ region: REGION });
    const params = { IndexName: PLACE_INDEX_NAME, Position: [lon, lat] };

    try {
        const response = await locationService.searchPlaceIndexForPosition(params).promise();
        if (response.Results && response.Results.length > 0) {
            return response.Results[0].Place.Label; 
        }
        return "Dirección no disponible";
    } catch (e) {
        console.error("Error Geocodificación Inversa:", e);
        return "Error de servicio (revisar permisos IAM)";
    }
}

// ====================================================
// 4. LÓGICA DE BÚSQUEDA DEL VEHÍCULO
// ====================================================

async function searchVehicle() {
    const vehicleId = document.getElementById('vehicle-input').value.trim();
    if (!vehicleId) return;

    // Validación simple para el ejemplo
    if (!/^veh_([1-9]|1\d|20)$/.test(vehicleId)) {
        alert("Por favor, ingrese un ID válido (veh_1 a veh_20).");
        return;
    }

    try {
        // Llama a API Gateway (Tu Backend)
        const response = await fetch(`${API_GATEWAY_URL}/data`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': API_KEY // 🔑 Clave API para autorización
            },
            body: JSON.stringify({ vehicleId: vehicleId })
        });

        const data = await response.json();
        
        if (response.ok) {
            updateInterface(data);
        } else {
            // Manejar errores como 404 (Vehículo no encontrado) o 429 (Throttling)
            alert(`Error ${response.status} al buscar ${vehicleId}: ${data.error || response.statusText}`);
        }

    } catch (error) {
        console.error("Error de conexión con la API:", error);
        alert("Error de conexión con el servicio. Verifique la URL de API Gateway y la Clave API.");
    }
}


async function updateInterface(data) {
    const card = document.getElementById('data-card');
    card.classList.remove('hidden');

    const { lon, lat, ang, device_id } = data;
    
    // 1. Geocodificación Inversa
    const address = await getAddressFromCoordinates(lon, lat);
    
    // 2. Actualizar Tarjeta
    document.getElementById('card-device-id').textContent = device_id;
    document.getElementById('card-lat-lon').textContent = `${lat.toFixed(6)}, ${lon.toFixed(6)}`;
    document.getElementById('card-ang').textContent = ang.toFixed(2);
    document.getElementById('card-address').textContent = address; 

    // 3. Actualizar Mapa
    const coordinates = [lon, lat];
    if (marker) {
        marker.setLngLat(coordinates);
    } else {
        // Crear un marcador simple
        marker = new maplibregl.Marker({ color: '#FF4500' }).setLngLat(coordinates).addTo(map);
    }
    map.flyTo({ center: coordinates, zoom: 14, speed: 1.5 });
}

// ====================================================
// 5. LISTENERS DE EVENTOS
// ====================================================

document.getElementById('search-btn').addEventListener('click', searchVehicle);

// Soporte para presionar 'Enter'
document.getElementById('vehicle-input').addEventListener('keypress', function(e) {
    if (e.key === 'Enter') {
        searchVehicle();
    }
});

// Iniciar la aplicación
initializeMap();