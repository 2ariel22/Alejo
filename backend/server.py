from flask import Flask, jsonify, request
from apify_client import ApifyClient
from flask_cors import CORS
import pandas as pd
import os
import logging
import numpy as np
import io
import codecs
import threading
import time
from queue import Queue
import traceback
import requests
from datetime import datetime
import pytz
import secrets
from functools import wraps
from dotenv import load_dotenv
from database import db_manager
from main import scrape_linkedin_profiles, find_emails

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Cargar variables de entorno primero
load_dotenv()

# Configuración de HubSpot
HUBSPOT_API_TOKEN = os.getenv('HUBSPOT_API_TOKEN')
HUBSPOT_CLIENT_SECRET = os.getenv('HUBSPOT_CLIENT_SECRET')

# Verificar que las variables de entorno estén configuradas
if not HUBSPOT_API_TOKEN:
    logger.warning("HUBSPOT_API_TOKEN no está configurado en las variables de entorno")
else:
    logger.info("HUBSPOT_API_TOKEN configurado correctamente")
    
if not HUBSPOT_CLIENT_SECRET:
    logger.warning("HUBSPOT_CLIENT_SECRET no está configurado en las variables de entorno")
else:
    logger.info("HUBSPOT_CLIENT_SECRET configurado correctamente")
HUBSPOT_BASE_URL = "https://api.hubapi.com"

# Cola para almacenar los resultados del scraping
scraping_results = Queue()

# Diccionario para almacenar el estado de los procesos de scraping
scraping_processes = {}

# Sesiones simples en memoria (para demo)
SESSIONS = set()

LOGIN_USER = os.getenv('LOGIN_USER')
LOGIN_PASS = os.getenv('LOGIN_PASS')

# Verificar que las credenciales de login estén configuradas
if not LOGIN_USER or not LOGIN_PASS:
    logger.warning("LOGIN_USER o LOGIN_PASS no están configurados en las variables de entorno")

def clean_nan_values(obj):
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif pd.isna(obj) or obj is None:
        return ""
    return obj

def run_scraping_process(urlsearch, process_id, search_name=None, search_description=None):
    try:
        logger.info(f"Iniciando proceso de scraping {process_id} para URL: {urlsearch}")
        
        # NO crear la búsqueda aquí. Solo la función scrape_linkedin_profiles debe crearla si hay resultados.
        # Ejecutar el scraping directamente usando la función importada
        result = scrape_linkedin_profiles(
            search_name or '', 
            search_description or '', 
            urlsearch
        )
        
        if result:
            logger.info(f"Proceso de scraping {process_id} completado exitosamente")
            scraping_results.put({
                'status': 'success',
                'message': 'Scraping completado exitosamente',
                'search_name': search_name,
                'search_id': result
            })
        else:
            logger.error(f"Error en el proceso de scraping {process_id}: No se pudo completar el scraping")
            scraping_results.put({
                'status': 'error',
                'error': 'No se pudo completar el scraping o no se encontraron perfiles'
            })
            
    except Exception as e:
        logger.error(f"Error en el proceso de scraping {process_id}: {str(e)}")
        logger.error(traceback.format_exc())
        scraping_results.put({
            'status': 'error',
            'error': f'Error al ejecutar el scraping: {str(e)}'
        })
    finally:
        # Limpiar el proceso del diccionario
        if process_id in scraping_processes:
            del scraping_processes[process_id]

@app.route('/api/profiles')
def get_profiles():
    try:
        # Obtener todos los perfiles de la base de datos
        profiles = db_manager.get_all_profiles()
        
        if profiles:
            logger.info(f"Perfiles obtenidos de la base de datos. Número de registros: {len(profiles)}")
            logger.info(f"Primer registro: {profiles[0]}")
        else:
            logger.warning("No se encontraron registros en la base de datos")
        
        return jsonify(profiles)
    except Exception as e:
        logger.error(f"Error al obtener perfiles de la base de datos: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': f'Error al obtener perfiles de la base de datos: {str(e)}'
        }), 500

@app.route('/api/status')
def get_status():
    try:
        profile_count = db_manager.get_profile_count()
        logger.info(f"Estado de la base de datos - Total de perfiles: {profile_count}")
        return jsonify({
            'database_exists': True,
            'profile_count': profile_count,
            'database_path': db_manager.db_path
        })
    except Exception as e:
        logger.error(f"Error al verificar estado de la base de datos: {str(e)}")
        return jsonify({
            'database_exists': False,
            'error': str(e)
        }), 500

@app.route('/api/run-scraper', methods=['POST'])
def run_scraper():
    try:
        data = request.get_json()
        urlsearch = data.get('urlsearch')
        search_name = data.get('search_name')
        search_description = data.get('search_description')
        
        if not urlsearch:
            logger.error("URL de búsqueda no proporcionada")
            return jsonify({
                'error': 'URL de búsqueda no proporcionada'
            }), 400

        # Generar un ID único para el proceso
        process_id = str(int(time.time()))
        logger.info(f"Iniciando nuevo proceso de scraping con ID: {process_id}")

        # Iniciar el proceso de scraping en un hilo separado
        thread = threading.Thread(
            target=run_scraping_process,
            args=(urlsearch, process_id, search_name, search_description),
            daemon=True
        )
        thread.start()

        # Almacenar el proceso en el diccionario
        scraping_processes[process_id] = {
            'thread': thread,
            'start_time': time.time(),
            'status': 'running',
            'search_name': search_name,
            'search_description': search_description
        }

        logger.info(f"Proceso de scraping {process_id} iniciado en hilo separado")
        
        return jsonify({
            'status': 'started',
            'process_id': process_id,
            'message': 'El proceso de scraping ha comenzado. Esto puede tomar varios minutos.',
            'search_name': search_name
        })

    except Exception as e:
        logger.error(f"Error al iniciar el scraping: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': f'Error al iniciar el scraping: {str(e)}'
        }), 500

@app.route('/api/scraping-status', methods=['GET'])
def get_scraping_status():
    try:
        # Verificar si hay algún resultado disponible
        try:
            result = scraping_results.get_nowait()
            logger.info(f"Resultado del scraping obtenido: {result['status']}")
            return jsonify(result)
        except:
            # Si no hay resultado disponible, devolver el estado actual
            return jsonify({
                'status': 'running',
                'message': 'El proceso de scraping está en ejecución',
                'active_processes': len(scraping_processes)
            })
    except Exception as e:
        logger.error(f"Error al verificar estado del scraping: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    
    # Verificar que las credenciales estén configuradas
    if not LOGIN_USER or not LOGIN_PASS:
        logger.error("Credenciales de login no configuradas en variables de entorno")
        return jsonify({"success": False, "error": "Configuración del servidor incompleta"}), 500
    
    if username == LOGIN_USER and password == LOGIN_PASS:
        token = secrets.token_hex(16)
        SESSIONS.add(token)
        return jsonify({"success": True, "token": token})
    return jsonify({"success": False, "error": "Credenciales inválidas"}), 401

# Decorador para proteger endpoints
def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        logger.info(f"Verificando autenticación - Token recibido: {token}")
        logger.info(f"Sesiones activas: {SESSIONS}")
        if not token or token not in SESSIONS:
            logger.warning(f"Autenticación fallida - Token: {token}")
            return jsonify({"error": "No autorizado"}), 401
        logger.info("Autenticación exitosa")
        return f(*args, **kwargs)
    return decorated

@app.route('/api/test-hubspot', methods=['GET'])
def test_hubspot_connection():
    """Probar la conexión con HubSpot"""
    try:
        if not HUBSPOT_API_TOKEN:
            return jsonify({
                'error': 'HUBSPOT_API_TOKEN no está configurado'
            }), 500
        
        headers = {
            'Authorization': f'Bearer {HUBSPOT_API_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        # Probar con un endpoint simple
        response = requests.get(
            f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts?limit=1",
            headers=headers
        )
        
        logger.info(f"Test HubSpot - Status: {response.status_code}")
        logger.info(f"Test HubSpot - Response: {response.text}")
        
        if response.status_code == 200:
            return jsonify({
                'success': True,
                'message': 'Conexión con HubSpot exitosa',
                'status_code': response.status_code
            })
        else:
            return jsonify({
                'error': f'Error de conexión con HubSpot: {response.status_code}',
                'response': response.text
            }), response.status_code
            
    except Exception as e:
        logger.error(f"Error en test_hubspot_connection: {str(e)}")
        return jsonify({
            'error': f'Error al probar conexión con HubSpot: {str(e)}'
        }), 500

@app.route('/api/send-to-hubspot', methods=['POST'])
# @require_auth  # Comentado temporalmente para pruebas
def send_to_hubspot():
    try:
        data = request.get_json()
        logger.info(f"Datos recibidos en send_to_hubspot: {data}")
        selected_contacts = data.get('contacts', [])
        logger.info(f"Contactos seleccionados: {selected_contacts}")
        
        if not selected_contacts:
            logger.warning("No se encontraron contactos en la petición")
            return jsonify({
                'error': 'No se proporcionaron contactos para enviar'
            }), 400

        logger.info(f"Enviando {len(selected_contacts)} contactos a HubSpot")
        
        # Verificar que el token de HubSpot esté configurado
        if not HUBSPOT_API_TOKEN:
            logger.error("HUBSPOT_API_TOKEN no está configurado")
            return jsonify({
                'error': 'Token de HubSpot no configurado en las variables de entorno'
            }), 500
        
        logger.info(f"Token de HubSpot configurado: {HUBSPOT_API_TOKEN[:10]}...")
        
        # Configurar headers para la API de HubSpot
        headers = {
            'Authorization': f'Bearer {HUBSPOT_API_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        logger.info(f"Headers configurados: {headers}")
        
        # Zona horaria GMT-5
        tz = pytz.timezone('America/New_York')
        current_time = datetime.now(tz)
        
        successful_contacts = []
        failed_contacts = []
        
        for contact in selected_contacts:
            try:
                # Preparar los datos del contacto para HubSpot usando los nombres internos estándar
                contact_data = {
                    "properties": {
                        "firstname": contact.get('fullName', '').split()[0] if contact.get('fullName') else '',
                        "lastname": ' '.join(contact.get('fullName', '').split()[1:]) if contact.get('fullName') and len(contact.get('fullName', '').split()) > 1 else '',
                        "email": contact.get('email', ''),
                        "phone": contact.get('mobileNumber', ''),
                        "city": contact.get('location', ''),
                        "lifecyclestage": "lead",
                        "hs_lead_status": "NEW"
                    }
                }
                
                logger.info(f"Enviando contacto a HubSpot: {contact.get('fullName', 'N/A')}")
                logger.info(f"Datos del contacto: {contact_data}")
                
                # Enviar contacto a HubSpot
                logger.info(f"URL de HubSpot: {HUBSPOT_BASE_URL}/crm/v3/objects/contacts")
                response = requests.post(
                    f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts",
                    headers=headers,
                    json=contact_data
                )
                
                logger.info(f"Respuesta de HubSpot - Status: {response.status_code}")
                logger.info(f"Respuesta de HubSpot - Headers: {dict(response.headers)}")
                logger.info(f"Respuesta de HubSpot - Body: {response.text}")
                
                if response.status_code in [200, 201]:
                    logger.info(f"Contacto {contact.get('fullName', 'N/A')} enviado exitosamente a HubSpot")
                    successful_contacts.append({
                        'name': contact.get('fullName', 'N/A'),
                        'hubspot_id': response.json().get('id')
                    })
                else:
                    logger.error(f"Error al enviar contacto {contact.get('fullName', 'N/A')}: {response.status_code} - {response.text}")
                    failed_contacts.append({
                        'name': contact.get('fullName', 'N/A'),
                        'error': f"HTTP {response.status_code}: {response.text}"
                    })
                    
            except Exception as e:
                logger.error(f"Error procesando contacto {contact.get('fullName', 'N/A')}: {str(e)}")
                failed_contacts.append({
                    'name': contact.get('fullName', 'N/A'),
                    'error': str(e)
                })
        
        return jsonify({
            'success': True,
            'message': f'Proceso completado. {len(successful_contacts)} contactos enviados exitosamente, {len(failed_contacts)} fallidos.',
            'successful_contacts': successful_contacts,
            'failed_contacts': failed_contacts
        })
        
    except Exception as e:
        logger.error(f"Error en send_to_hubspot: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': f'Error al enviar contactos a HubSpot: {str(e)}'
        }), 500

# Endpoints para manejar búsquedas
@app.route('/api/searches', methods=['GET'])
def get_searches():
    """Obtener todas las búsquedas"""
    try:
        searches = db_manager.get_all_searches()
        return jsonify(searches)
    except Exception as e:
        logger.error(f"Error al obtener búsquedas: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/searches', methods=['POST'])
def create_search():
    """Crear una nueva búsqueda"""
    try:
        data = request.get_json()
        name = data.get('name')
        description = data.get('description', '')
        search_url = data.get('search_url')
        
        if not name or not search_url:
            return jsonify({'error': 'Nombre y URL de búsqueda son requeridos'}), 400
        
        search_id = db_manager.create_search(name, description, search_url)
        if search_id:
            search = db_manager.get_search_by_id(search_id)
            return jsonify(search), 201
        else:
            return jsonify({'error': 'Error al crear la búsqueda'}), 500
            
    except Exception as e:
        logger.error(f"Error al crear búsqueda: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/searches/<int:search_id>', methods=['GET'])
def get_search(search_id):
    """Obtener una búsqueda específica"""
    try:
        search = db_manager.get_search_by_id(search_id)
        if search:
            return jsonify(search)
        else:
            return jsonify({'error': 'Búsqueda no encontrada'}), 404
    except Exception as e:
        logger.error(f"Error al obtener búsqueda: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/searches/<int:search_id>', methods=['PUT'])
def update_search(search_id):
    """Actualizar una búsqueda"""
    try:
        data = request.get_json()
        name = data.get('name')
        description = data.get('description')
        status = data.get('status')
        
        success = db_manager.update_search(search_id, name, description, status)
        if success:
            search = db_manager.get_search_by_id(search_id)
            return jsonify(search)
        else:
            return jsonify({'error': 'Error al actualizar la búsqueda'}), 500
            
    except Exception as e:
        logger.error(f"Error al actualizar búsqueda: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/searches/<int:search_id>', methods=['DELETE'])
def delete_search(search_id):
    """Eliminar una búsqueda"""
    try:
        success = db_manager.delete_search(search_id)
        if success:
            return jsonify({'message': 'Búsqueda eliminada exitosamente'})
        else:
            return jsonify({'error': 'Error al eliminar la búsqueda'}), 500
            
    except Exception as e:
        logger.error(f"Error al eliminar búsqueda: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/searches/<int:search_id>/profiles', methods=['GET'])
def get_search_profiles(search_id):
    """Obtener todos los perfiles de una búsqueda específica"""
    try:
        profiles = db_manager.get_profiles_by_search(search_id)
        return jsonify(profiles)
    except Exception as e:
        logger.error(f"Error al obtener perfiles de búsqueda: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/searches/statistics', methods=['GET'])
def get_search_statistics():
    """Obtener estadísticas de todas las búsquedas"""
    try:
        statistics = db_manager.get_search_statistics()
        return jsonify(statistics)
    except Exception as e:
        logger.error(f"Error al obtener estadísticas de búsquedas: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/run-email-search', methods=['POST'])
def run_email_search():
    """Ejecutar búsqueda de emails para perfiles sin verificar"""
    try:
        logger.info("Iniciando búsqueda de emails...")
        
        # Ejecutar la búsqueda de emails
        find_emails()
        
        return jsonify({
            'status': 'success',
            'message': 'Búsqueda de emails completada exitosamente'
        })
        
    except Exception as e:
        logger.error(f"Error al ejecutar búsqueda de emails: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': f'Error al ejecutar búsqueda de emails: {str(e)}'
        }), 500

if __name__ == '__main__':
    logger.info(f"Iniciando servidor en http://{os.getenv('HOST', 'localhost')}:5000")
    app.run(debug=True, port=5000, threaded=True, host=os.getenv('HOST', 'localhost')) 