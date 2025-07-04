from apify_client import ApifyClient
import pandas as pd
import json
from datetime import datetime
import os
import sys
import io
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configurar la codificación de la consola
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

def scrape_linkedin_profiles():
    print("\n=== Iniciando búsqueda de perfiles de LinkedIn ===")
    
    # Obtener API key desde variables de entorno
    APIFY_API_KEY = os.getenv('APIFY_API_KEY')
    if not APIFY_API_KEY:
        print("Error: No se encontró la variable de entorno APIFY_API_KEY")
        return None
    
    urlsearch = "https://www.linkedin.com/search/results/people/?geoUrn=%5B%22102476984%22%5D&keywords=liquitech&origin=FACETED_SEARCH&profileLanguage=%5B%22es%22%2C%22en%22%5D&searchId=b0ce83b3-364e-49b7-ad8e-e8e54549a146&sid=B%3B)&titleFreeText=CEO"
    
    # Initialize the ApifyClient with your API token
    client = ApifyClient(APIFY_API_KEY)
    
    # Prepare the Actor input
    run_input = {
        "cookie": [
            {
                "name": "li_sugr",
                "value": "40510076-a6b0-4cd5-965c-b6acac644ce3",
                "domain": ".linkedin.com",
                "path": "/",
                "expires": "2025-08-14T21:56:52.261Z"
            },
            {
                "name": "li_rm",
                "value": "AQE8rvNrfDmzSAAAAZcgA-NaE_kga5o_qWmXs9CEtelSeBGBLK-3oj9qg30VHw_OTgBvfR3Q1nBkVMhgSZ_APhz3y_FutywmDByyX2-2HR7TjjckBI9ST1bA",
                "domain": ".www.linkedin.com",
                "path": "/",
                "expires": "2026-06-05T13:38:55.703Z"
            },
            {
                "name": "li_at",
                "value": "AQEDATwq6g0FCdqOAAABl0BQVFgAAAGXZFzYWE4AIVETkPmGP5nTaxFMwBP1-wEFEiknikFBo2oeguQVv2H7gv_2luRY9-Gcp6q7h2NSJ80zosMFaIU23K2BTAXhChCwEJ41LYfWH0TN70uD-75I1gus",
                "domain": ".www.linkedin.com",
                "path": "/",
                "expires": "2026-06-05T13:38:55.703Z"
            },
            {
                "name": "lang",
                "value": "v=2&lang=es-es",
                "domain": ".linkedin.com",
                "path": "/"
            },
            {
                "name": "g_state",
                "value": "{\"i_l\":0}",
                "domain": "www.linkedin.com",
                "path": "/",
                "expires": "2025-12-02T13:38:17.000Z"
            },
            {
                "name": "fptctx2",
                "value": "taBcrIH61PuCVH7eNCyH0J9Fjk1kZEyRnBbpUW3FKs9EWYO8RHHrIbqKFeT8Mw2Cdl3N1Amqkcc4n3lg%252b15wdBj2tiRSrp1luzLiVnKFEA4FE9uv7LDcL36cAYrIf3zMiKzuzcnyJp7qRvLYXpGE8KNsLeL0vMoi9pl86YPyRdvrZNyPCe9RVVPAnYhqEMYnyMk1UIhwi7OLt08aR4nxsB5%252fez8F1U%252f26kp7LS0%252bE5%252fWgdX82P8oUYNOC5gu1T4zAT3C3w7BuOpi%252febw7Ja0%252fQKtwi%252f1dinSQf%252bu2ZoK7P10YxoAVBGoEcRlJzEyq9I5u49Row4fjSTkyrRq0e4KWRJvXoF7TcR7gp2oGgG4rLk%253d",
                "domain": ".linkedin.com",
                "path": "/"
            },
            {
                "name": "dfpfpt",
                "value": "3371bac7e95e4097808749970b9d2f2c",
                "domain": ".linkedin.com",
                "path": "/",
                "expires": "2026-05-30T05:38:47.363Z"
            },
            {
                "name": "bscookie",
                "value": "\"v=1&202505160118306e22a519-8b06-469f-8a01-bb887df11e0fAQFdqr5BpJpG8_k6YBtDhpb99qKlrwr7\"",
                "domain": ".www.linkedin.com",
                "path": "/",
                "expires": "2026-06-05T13:38:55.703Z"
            },
            {
                "name": "bcookie",
                "value": "\"v=2&b8a5f0e9-560e-41c0-8ff0-2a029f6ea165\"",
                "domain": ".linkedin.com",
                "path": "/",
                "expires": "2026-06-05T15:20:09.598Z"
            },
            {
                "name": "UserMatchHistory",
                "value": "AQL0AuoXO_8d2wAAAZdArQyfl4RPqDCYsDKWjk8LayNFSJGJW58RKhI72Z_QjVOA_FU9KUGTDU4sqrqjNB-IqfdXJSEbuZC1MgsKB1RVPRbT6G5YBd_dzsgqISAq7BhvZghiLMgiTP9zeYKqlf36pKADOT_-ahzahF_ZW03DmK8i7wJqGWvn2tHgQUl0NlEtlD-bza_EUNvuplWUVM5-h9KdyO4Ipn95mDmWgmQAnJqek0meolSX7V-OpiYObACsQwsyK4iy_fxfQ5olewwTF4QCFWKGcZu0fF2Fm7IgD3PTpFxgKbQeMFE3_UjsqM7d8FW7ZuEaoL9lPEj8KLBC5fLyjcJoLW1n60GwIgS5Y-1GNfPjaQ",
                "domain": ".linkedin.com",
                "path": "/",
                "expires": "2025-07-05T15:20:12.000Z"
            },
            {
                "name": "JSESSIONID",
                "value": "\"ajax:2195909279237352274\"",
                "domain": ".www.linkedin.com",
                "path": "/",
                "expires": "2025-09-03T13:38:55.703Z"
            },
            {
                "name": "AnalyticsSyncHistory",
                "value": "AQICwQ_c4dzkIAAAAZbWq-ClghpGQIv78WEqW1GvaKZdUo02KacciXfINXhd9JpyOswD73CZCJ0nqCILv6VNJg",
                "domain": ".linkedin.com",
                "path": "/",
                "expires": "2025-06-15T01:19:24.903Z"
            }
        ],
        "maxDelay": 5,
        "minDelay": 2,
        "searchUrl": urlsearch,
        "startPage": 1
    }
    
    # Run the Actor and wait for it to finish
    run = client.actor("2SyF0bVxmgGr8IVCZ").call(run_input=run_input)
    
    # Verificar que el run se ejecutó correctamente
    if run is None:
        print("Error: No se pudo ejecutar el actor de scraping")
        return None
    
    # Nombre del archivo Excel
    nombre_archivo = "Excels/datasetscrapper.xlsx"
    
    # Cargar datos existentes si el archivo existe
    datos_existentes = []
    if os.path.exists(nombre_archivo):
        df_existente = pd.read_excel(nombre_archivo)
        datos_existentes = df_existente.to_dict('records')
        print(f"Se encontraron {len(datos_existentes)} registros existentes en el archivo.")
    
    # Lista para almacenar los datos procesados
    datos_procesados = []
    # Usar un conjunto para almacenar URLs únicas
    urls_existentes = {item['profileUrl'].split('?')[0] for item in datos_existentes if isinstance(item.get('profileUrl'), str)}
    
    # Fetch and process Actor results from the run's dataset
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        # Extraer solo los campos requeridos
        profile_url = item.get('profileUrl', '')
        if not profile_url:
            continue
            
        # Limpiar la URL para comparación
        clean_url = profile_url.split('?')[0]
        
        # Verificar si la URL ya existe
        if clean_url not in urls_existentes:
            dato_procesado = {
                'fullName': item.get('fullName', ''),
                'headline': item.get('headline', ''),
                'id': item.get('id', ''),
                'lastName': item.get('lastName', ''),
                'location': item.get('location', ''),
                'picture': item.get('picture', ''),
                'profileId': item.get('profileId', ''),
                'profileUrl': profile_url,
                'email': '',  # Inicializar campos vacíos
                'mobileNumber': '',
                'email_checked': False  # Marcar que no se ha buscado email
            }
            datos_procesados.append(dato_procesado)
            urls_existentes.add(clean_url)
            print(f"Nuevo registro encontrado: {dato_procesado['fullName']}")
        else:
            print(f"Registro duplicado encontrado: {item.get('fullName', '')}")
    
    # Combinar datos existentes con nuevos datos
    todos_los_datos = datos_existentes + datos_procesados
    
    # Crear DataFrame con todos los datos
    df = pd.DataFrame(todos_los_datos)
    
    # Asegurarse de que la columna email_checked exista
    if 'email_checked' not in df.columns:
        df['email_checked'] = False
    
    # Guardar en Excel
    df.to_excel(nombre_archivo, index=False)
    print(f"\nArchivo guardado como: {nombre_archivo}")
    print(f"Total de registros en el archivo: {len(todos_los_datos)}")
    print(f"Nuevos registros agregados: {len(datos_procesados)}")
    
    return nombre_archivo

def find_emails(nombre_archivo):
    print("\n=== Iniciando búsqueda de emails y números ===")
    
    # Obtener API key desde variables de entorno
    APIFY_API_KEY = os.getenv('APIFY_API_KEY')
    if not APIFY_API_KEY:
        print("Error: No se encontró la variable de entorno APIFY_API_KEY")
        return
    
    # Initialize the ApifyClient with your API token
    client = ApifyClient(APIFY_API_KEY)
    
    if not os.path.exists(nombre_archivo):
        print("No se encontró el archivo datasetscrapper.xlsx")
        return
    
    try:
        df = pd.read_excel(nombre_archivo)
        print(f"\nDatos cargados del Excel:")
        print(f"Total de registros: {len(df)}")
    except Exception as e:
        print(f"Error al leer el archivo Excel: {e}")
        return
    
    # Asegurarse de que la columna email_checked exista
    if 'email_checked' not in df.columns:
        df['email_checked'] = False
    
    # Filtrar solo los perfiles que no han sido procesados
    perfiles_sin_procesar = df[df['email_checked'] == False]
    
    if len(perfiles_sin_procesar) == 0:
        print("No hay perfiles nuevos para buscar contactos.")
        return
        
    listprofile = perfiles_sin_procesar['profileUrl'].tolist()
    print(f"URLs a procesar: {len(listprofile)}")
    
    # Prepare the Actor input
    run_input = { "profileUrls": listprofile }
    
    # Run the Actor and wait for it to finish
    print("\nIniciando búsqueda de emails y números...")
    run = client.actor("2SyF0bVxmgGr8IVCZ").call(run_input=run_input)
    
    # Verificar que el run se ejecutó correctamente
    if run is None:
        print("Error: No se pudo ejecutar el actor de búsqueda de emails")
        return
    
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
            # Limpiar la URL para que coincida con el formato del Excel
            clean_url = profile_url.split('?')[0]  # Remover parámetros de la URL
            contactos[clean_url] = {
                'email': email if email is not None else '',
                'mobileNumber': mobile if mobile is not None else ''
            }
    
    # Actualizar el DataFrame con los nuevos datos
    print("\nActualizando Excel...")
    for index, row in df.iterrows():
        profile_url = row['profileUrl']
        if isinstance(profile_url, str):  # Verificar que profile_url sea una cadena
            # Limpiar la URL del Excel para que coincida con el formato de la API
            clean_url = profile_url.split('?')[0]  # Remover parámetros de la URL
            if clean_url in contactos:
                df.at[index, 'email'] = contactos[clean_url]['email']
                df.at[index, 'mobileNumber'] = contactos[clean_url]['mobileNumber']
            # Marcar el perfil como procesado
            df.at[index, 'email_checked'] = True
    
    # Convertir las columnas a string para evitar problemas con NaN
    df['email'] = df['email'].astype(str).replace('nan', '')
    df['mobileNumber'] = df['mobileNumber'].astype(str).replace('nan', '')
    
    # Contar emails y números no vacíos
    emails_encontrados = df['email'].str.len() > 0
    numeros_encontrados = df['mobileNumber'].str.len() > 0
    
    # Guardar el DataFrame actualizado
    try:
        df.to_excel(nombre_archivo, index=False)
        print(f"\nArchivo actualizado: {nombre_archivo}")
        print(f"Total de perfiles procesados: {len(listprofile)}")
        print(f"Emails encontrados: {emails_encontrados.sum()}")
        print(f"Números encontrados: {numeros_encontrados.sum()}")
        
        # Mostrar los primeros registros para verificar
        print("\nPrimeros registros actualizados:")
        # Verificar qué columnas existen
        columnas_disponibles = df.columns.tolist()
        columnas_a_mostrar = [col for col in ['name', 'fullName', 'email', 'mobileNumber', 'email_checked'] if col in columnas_disponibles]
        if columnas_a_mostrar:
            print(df[columnas_a_mostrar].head())
        else:
            print("No se encontraron columnas para mostrar")
    except PermissionError:
        print(f"\nError: No se puede guardar el archivo {nombre_archivo} porque está abierto en otro programa.")
        print("Por favor, cierre el archivo Excel y vuelva a intentar.")
    except Exception as e:
        print(f"\nError al guardar el archivo: {e}")

def main():
    print("=== Iniciando proceso de scraping y búsqueda de emails ===")
    
    # Primero ejecutar el scraping de perfiles
    nombre_archivo = scrape_linkedin_profiles()
    
    # Luego buscar emails y números
    if nombre_archivo:
        find_emails(nombre_archivo)
    
    print("\n=== Proceso completado ===")

if __name__ == "__main__":
    main() 