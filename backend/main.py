from apify_client import ApifyClient
import pandas as pd
import json
from datetime import datetime
import os
import sys
import io
from dotenv import load_dotenv
from database import db_manager

# Cargar variables de entorno
load_dotenv()

# Configurar la codificación de la consola
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

def scrape_linkedin_profiles(search_name: str = None, search_description: str = None, search_url: str = None):
    print("\n=== Iniciando búsqueda de perfiles de LinkedIn ===")
    
    # Verificar que se proporcione una URL de búsqueda
    if not search_url:
        print("Error: Se requiere una URL de búsqueda de LinkedIn")
        return None
    
    # Obtener API key desde variables de entorno
    APIFY_API_KEY = os.getenv('APIFY_API_KEY')
    if not APIFY_API_KEY:
        print("Error: No se encontró la variable de entorno APIFY_API_KEY")
        return None
    
    urlsearch = search_url
    
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
                "expires": "2025-06-15T15:20:09.598Z"
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
    
    if not run:
        print("Error: No se pudo ejecutar el actor de Apify")
        return None
    
    # Obtener perfiles existentes de la base de datos
    existing_profiles = db_manager.get_all_profiles()
    existing_urls = {profile['profileUrl'].split('?')[0] for profile in existing_profiles if isinstance(profile.get('profileUrl'), str)}
    
    print(f"Se encontraron {len(existing_profiles)} registros existentes en la base de datos.")
    
    # Listas para almacenar los datos procesados y todos los perfiles encontrados
    datos_procesados = []
    all_found_urls = []  # URLs de todos los perfiles encontrados en esta búsqueda
    all_found_profiles = []  # Perfiles completos encontrados (nuevos o existentes)
    
    # Fetch and process Actor results from the run's dataset
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        # El actor devuelve 'linkedinUrl' en lugar de 'profileUrl'
        profile_url = item.get('linkedinUrl', '') or item.get('profileUrl', '')
        if not profile_url:
            continue
        
        # Log para debug: ver qué campos trae el scraping masivo
        print(f"\n=== DEBUG: Campos del perfil {item.get('fullName', 'N/A')} ===")
        print(f"URL: {profile_url}")
        print(f"Email del scraping: {item.get('email', 'NO ENCONTRADO')}")
        print(f"Teléfono del scraping: {item.get('mobileNumber', 'NO ENCONTRADO')}")
        print(f"Campos disponibles: {list(item.keys())}")
        
        clean_url = profile_url.split('?')[0]
        all_found_urls.append(clean_url)
        email_scraped = item.get('email', '')
        mobile_scraped = item.get('mobileNumber', '')
        # Si es nuevo, lo agregamos a datos_procesados
        if clean_url not in existing_urls:
            dato_procesado = {
                'fullName': item.get('fullName', ''),
                'headline': item.get('headline', ''),
                'id': item.get('id', ''),
                'lastName': item.get('lastName', ''),
                'location': item.get('location', ''),
                'picture': item.get('picture', ''),
                'profileId': item.get('profileId', ''),
                'profileUrl': profile_url,
                'email': email_scraped,
                'mobileNumber': mobile_scraped,
                'email_checked': bool(email_scraped)
            }
            datos_procesados.append(dato_procesado)
            all_found_profiles.append(dato_procesado)
            existing_urls.add(clean_url)
            print(f"Nuevo registro encontrado: {dato_procesado['fullName']}")
        else:
            # Buscar el perfil existente y agregarlo a la lista de encontrados
            for p in existing_profiles:
                if p['profileUrl'].split('?')[0] == clean_url:
                    all_found_profiles.append(p)
                    # Si el scraping trae un email o teléfono nuevo, actualizar el perfil
                    update_needed = False
                    new_email = email_scraped if email_scraped else p.get('email', '')
                    new_mobile = mobile_scraped if mobile_scraped else p.get('mobileNumber', '')
                    if (email_scraped and email_scraped != p.get('email', '')) or (mobile_scraped and mobile_scraped != p.get('mobileNumber', '')):
                        db_manager.update_profiles_contact_info_batch([
                            {'profileUrl': profile_url, 'email': new_email, 'mobileNumber': new_mobile}
                        ])
                        print(f"Perfil existente actualizado con nuevo email/teléfono: {p.get('fullName', '')}")
                    break
            print(f"Registro existente encontrado: {item.get('fullName', '')}")
    
    # Si no se encontró ningún perfil (ni nuevo ni existente), no crear la búsqueda
    if not all_found_profiles:
        print("No se encontraron perfiles (ni nuevos ni existentes). No se creará la búsqueda.")
        return None
    
    # Crear la búsqueda en la base de datos
    if search_name:
        search_id = db_manager.create_search(search_name, search_description or "", search_url)
        if not search_id:
            print("Error: No se pudo crear la búsqueda en la base de datos")
            return None
        print(f"Búsqueda creada con ID: {search_id}")
    else:
        search_id = None
    
    # Insertar nuevos perfiles en la base de datos
    if datos_procesados:
        inserted_count = db_manager.insert_profiles_batch(datos_procesados)
        print(f"\nNuevos registros insertados en la base de datos: {inserted_count}")
    
    # Asociar TODOS los perfiles encontrados a la búsqueda
    if search_id and all_found_profiles:
        # Obtener los IDs de todos los perfiles encontrados (nuevos y existentes)
        profile_urls_to_find = [p['profileUrl'] for p in all_found_profiles if p.get('profileUrl')]
        profile_ids_by_url = db_manager.get_profile_ids_by_urls(profile_urls_to_find)
        profile_ids_to_add = list(profile_ids_by_url.values())
        # Eliminar duplicados manteniendo el orden
        unique_profile_ids = list(dict.fromkeys(profile_ids_to_add))
        added_to_search = db_manager.add_profiles_to_search_batch(search_id, unique_profile_ids)
        print(f"Perfiles agregados a la búsqueda {search_id}: {added_to_search}")
        print(f"Total de perfiles únicos encontrados en esta búsqueda: {len(unique_profile_ids)}")
    
    total_profiles = db_manager.get_profile_count()
    print(f"Total de registros en la base de datos: {total_profiles}")
    print(f"Nuevos registros agregados: {len(datos_procesados)}")
    print(f"Total de perfiles encontrados en esta búsqueda: {len(all_found_profiles)}")
    
    # Buscar emails automáticamente después del scraping
    print("\n=== Iniciando búsqueda automática de emails ===")
    find_emails()
    
    return search_id if search_id else "profiles.db"

def find_emails():
    print("\n=== Iniciando búsqueda de emails y números ===")
    
    # Obtener API key desde variables de entorno
    APIFY_API_KEY = os.getenv('APIFY_API_KEY')
    if not APIFY_API_KEY:
        print("Error: No se encontró la variable de entorno APIFY_API_KEY")
        return
    
    # Initialize the ApifyClient with your API token
    client = ApifyClient(APIFY_API_KEY)
    
    # DEBUG: Verificar estado de los perfiles
    debug_status = db_manager.debug_profiles_status()
    print(f"\n=== DEBUG: Estado de la base de datos ===")
    print(f"Total de perfiles: {debug_status.get('total_profiles', 0)}")
    print(f"Perfiles con email: {debug_status.get('profiles_with_email', 0)}")
    print(f"Perfiles marcados como verificados: {debug_status.get('profiles_checked', 0)}")
    print(f"Perfiles marcados como NO verificados: {debug_status.get('profiles_unchecked', 0)}")
    print(f"Perfiles con URL válida: {debug_status.get('profiles_with_url', 0)}")
    
    # Obtener perfiles que no han sido verificados para email
    perfiles_sin_procesar = db_manager.get_profiles_without_email()
    
    print(f"DEBUG: Perfiles sin procesar encontrados: {len(perfiles_sin_procesar)}")
    
    if len(perfiles_sin_procesar) == 0:
        print("No hay perfiles nuevos para buscar contactos.")
        return
    
    # Mostrar algunos perfiles para debug
    print("DEBUG: Primeros 3 perfiles sin procesar:")
    for i, profile in enumerate(perfiles_sin_procesar[:3]):
        print(f"  {i+1}. {profile.get('fullName', 'N/A')} - {profile.get('profileUrl', 'N/A')} - email_checked: {profile.get('email_checked', 'N/A')}")
    
    # Filtrar solo perfiles que tienen URL válida
    perfiles_con_url = [profile for profile in perfiles_sin_procesar if profile.get('profileUrl') and profile['profileUrl'].strip()]
    
    print(f"DEBUG: Perfiles con URL válida: {len(perfiles_con_url)}")
    
    if len(perfiles_con_url) == 0:
        print("No hay perfiles con URLs válidas para procesar.")
        return
    
    # Limpiar URLs para el actor de emails (remover parámetros de query)
    listprofile = []
    for profile in perfiles_con_url:
        url = profile['profileUrl']
        # Remover parámetros de query para obtener la URL base del perfil
        clean_url = url.split('?')[0]
        listprofile.append(clean_url)
        print(f"DEBUG: URL original: {url}")
        print(f"DEBUG: URL limpia para email search: {clean_url}")
    print(f"URLs a procesar: {len(listprofile)}")
    
    # Limitar el número de URLs por lote para evitar timeouts
    batch_size = 50
    total_processed = 0
    
    for i in range(0, len(listprofile), batch_size):
        batch_urls = listprofile[i:i + batch_size]
        print(f"\nProcesando lote {i//batch_size + 1} de {(len(listprofile) + batch_size - 1)//batch_size}")
        print(f"URLs en este lote: {len(batch_urls)}")
        
        # Prepare the Actor input
        run_input = { "profileUrls": batch_urls }
        
        # Run the Actor and wait for it to finish
        print("Iniciando búsqueda de emails y números...")
        run = client.actor("2SyF0bVxmgGr8IVCZ").call(run_input=run_input)
        
        if not run:
            print("Error: No se pudo ejecutar el actor de Apify para búsqueda de emails")
            continue
        
        # Lista para almacenar actualizaciones de contactos
        contact_updates = []
        
        # Fetch and process Actor results
        print("Procesando resultados...")
        results_count = 0
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            results_count += 1
            # El actor de emails devuelve 'linkedinUrl'
            profile_url = item.get('linkedinUrl', '') or item.get('profileUrl', '')
            email = item.get('email', '')
            mobile = item.get('mobileNumber', '')
            
            # DEBUG: Mostrar información del resultado
            print(f"DEBUG: Resultado {results_count} - URL: {profile_url}")
            print(f"DEBUG: Email encontrado: {email}")
            print(f"DEBUG: Teléfono encontrado: {mobile}")
            print(f"DEBUG: Todos los campos: {list(item.keys())}")
            
            if profile_url:
                # Buscar el perfil original en la base de datos usando la URL limpia
                clean_url = profile_url.split('?')[0]
                # Buscar perfiles que coincidan con esta URL limpia
                matching_profiles = [p for p in perfiles_con_url if p['profileUrl'].split('?')[0] == clean_url]
                
                if matching_profiles:
                    # Usar la URL original del perfil para la actualización
                    original_url = matching_profiles[0]['profileUrl']
                    contact_updates.append({
                        'profileUrl': original_url,
                        'email': email if email is not None else '',
                        'mobileNumber': mobile if mobile is not None else ''
                    })
                    print(f"DEBUG: Perfil encontrado para actualizar: {matching_profiles[0].get('fullName', 'N/A')}")
                else:
                    print(f"DEBUG: No se encontró perfil coincidente para URL: {clean_url}")
        
        print(f"DEBUG: Total de resultados procesados: {results_count}")
        
        # Actualizar la base de datos con los nuevos datos de contacto
        if contact_updates:
            updated_count = db_manager.update_profiles_contact_info_batch(contact_updates)
            print(f"Perfiles actualizados en este lote: {updated_count}")
            total_processed += updated_count
        
        # Pequeña pausa entre lotes para evitar sobrecarga
        if i + batch_size < len(listprofile):
            print("Esperando 5 segundos antes del siguiente lote...")
            import time
            time.sleep(5)
    
    # Obtener estadísticas finales
    total_profiles = db_manager.get_profile_count()
    profiles_with_emails = len(db_manager.get_profiles_with_emails())
    
    print(f"\n=== Resumen de la búsqueda de emails ===")
    print(f"Total de perfiles en la base de datos: {total_profiles}")
    print(f"Perfiles con emails: {profiles_with_emails}")
    print(f"Perfiles procesados en esta sesión: {total_processed}")
    print(f"Perfiles restantes sin verificar: {len(perfiles_sin_procesar) - total_processed}")

def main():
    print("=== Iniciando proceso de búsqueda de emails ===")
    
    # Solo buscar emails y números (el scraping se hace desde el servidor)
    find_emails()
    
    print("\n=== Proceso completado ===")

if __name__ == "__main__":
    main() 