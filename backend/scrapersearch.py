from apify_client import ApifyClient
import pandas as pd
import json
from datetime import datetime
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Obtener API key desde variables de entorno
APIFY_API_KEY = os.getenv('APIFY_API_KEY')
if not APIFY_API_KEY:
    print("Error: No se encontró la variable de entorno APIFY_API_KEY")
    exit()

urlsearch = "https://www.linkedin.com/search/results/people/?geoUrn=%5B%22102476984%22%5D&heroEntityKey=urn%3Ali%3Aorganization%3A40766027&industry=%5B%221810%22%5D&keywords=Liquitech&origin=FACETED_SEARCH&profileLanguage=%5B%22en%22%5D&sid=Nh3"

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
run = client.actor("pdcNMezBkIlhX0LwO").call(run_input=run_input)

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
ids_existentes = {item['id'] for item in datos_existentes}

# Fetch and process Actor results from the run's dataset
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    # Extraer solo los campos requeridos
    dato_procesado = {
        'fullName': item.get('fullName', ''),
        'headline': item.get('headline', ''),
        'id': item.get('id', ''),
        'lastName': item.get('lastName', ''),
        'location': item.get('location', ''),
        'picture': item.get('picture', ''),
        'profileId': item.get('profileId', ''),
        'profileUrl': item.get('profileUrl', '')
    }
    
    # Verificar si el ID ya existe
    if dato_procesado['id'] not in ids_existentes:
        datos_procesados.append(dato_procesado)
        ids_existentes.add(dato_procesado['id'])
        print(f"Nuevo registro encontrado: {dato_procesado['fullName']}")
    else:
        print(f"Registro duplicado encontrado: {dato_procesado['fullName']}")

# Combinar datos existentes con nuevos datos
todos_los_datos = datos_existentes + datos_procesados

# Crear DataFrame con todos los datos
df = pd.DataFrame(todos_los_datos)

# Guardar en Excel
df.to_excel(nombre_archivo, index=False)
print(f"\nArchivo guardado como: {nombre_archivo}")
print(f"Total de registros en el archivo: {len(todos_los_datos)}")
print(f"Nuevos registros agregados: {len(datos_procesados)}")