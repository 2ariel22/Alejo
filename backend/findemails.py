from apify_client import ApifyClient
import os
from dotenv import load_dotenv
from database import DatabaseManager

# Cargar variables de entorno
load_dotenv()

# Obtener API key desde variables de entorno
APIFY_API_KEY = os.getenv('APIFY_API_KEY')
if not APIFY_API_KEY:
    print("Error: No se encontró la variable de entorno APIFY_API_KEY")
    exit()

# Initialize the ApifyClient with your API token
client = ApifyClient(APIFY_API_KEY)

# Inicializar el gestor de base de datos
db_manager = DatabaseManager()

# Obtener perfiles de la base de datos
try:
    profiles = db_manager.get_all_profiles()
    print(f"\nDatos cargados de la base de datos:")
    print(f"Total de registros: {len(profiles)}")
except Exception as e:
    print(f"Error al leer la base de datos: {e}")
    exit()

# Obtener lista de URLs de perfiles
listprofile = []
for profile in profiles:
    profile_url = profile.get('profileUrl')
    if profile_url and profile_url.strip():
        listprofile.append(profile_url)
print(f"URLs a procesar: {len(listprofile)}")

if not listprofile:
    print("No hay perfiles con URLs para procesar")
    exit()

# Prepare the Actor input
run_input = { "profileUrls": listprofile }

# Run the Actor and wait for it to finish
print("\nIniciando búsqueda de emails y números...")
run = client.actor("2SyF0bVxmgGr8IVCZ").call(run_input=run_input)

# Diccionario para almacenar emails y números
contactos = {}

# Fetch and process Actor results
print("\nProcesando resultados...")
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    profile_url = item.get('linkedinUrl', '')
    email = item.get('email', '')
    mobile = item.get('mobileNumber', '')
    
    print(f"\nPerfil encontrado: {profile_url}")
    print(f"Email: {email}")
    print(f"Número: {mobile}")
    
    if profile_url:
        # Limpiar la URL para que coincida con el formato de la base de datos
        clean_url = profile_url.split('?')[0]  # Remover parámetros de la URL
        contactos[clean_url] = {
            'email': email if email is not None else '',
            'mobileNumber': mobile if mobile is not None else ''
        }

# Actualizar la base de datos con los nuevos datos
print("\nActualizando base de datos...")
updated_count = 0
for profile in profiles:
    profile_url = profile.get('profileUrl')
    if profile_url:
        # Limpiar la URL para que coincida con el formato de la API
        clean_url = profile_url.split('?')[0]  # Remover parámetros de la URL
        if clean_url in contactos:
            # Actualizar el perfil en la base de datos
            success = db_manager.update_profile_contact(
                profile['id'],
                contactos[clean_url]['email'],
                contactos[clean_url]['mobileNumber']
            )
            if success:
                updated_count += 1

# Obtener estadísticas actualizadas
try:
    updated_profiles = db_manager.get_all_profiles()
    emails_encontrados = sum(1 for p in updated_profiles if p.get('email') and p['email'].strip())
    numeros_encontrados = sum(1 for p in updated_profiles if p.get('mobileNumber') and p['mobileNumber'].strip())
    
    print(f"\nBase de datos actualizada exitosamente")
    print(f"Total de perfiles procesados: {len(listprofile)}")
    print(f"Perfiles actualizados: {updated_count}")
    print(f"Emails encontrados: {emails_encontrados}")
    print(f"Números encontrados: {numeros_encontrados}")

    # Mostrar los primeros registros para verificar
    print("\nPrimeros registros actualizados:")
    if updated_profiles:
        for i, profile in enumerate(updated_profiles[:5]):
            print(f"{i+1}. {profile.get('fullName', 'N/A')} - Email: {profile.get('email', 'N/A')} - Tel: {profile.get('mobileNumber', 'N/A')}")
    else:
        print("No se encontraron perfiles para mostrar")

except Exception as e:
    print(f"\nError al obtener estadísticas actualizadas: {e}")