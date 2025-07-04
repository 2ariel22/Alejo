import pandas as pd
from apify_client import ApifyClient
import time
import sys
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def validate_emails_batch(emails, client):
    # Preparar el input para el actor
    run_input = {
        "emails": emails
    }
    
    try:
        # Ejecutar el actor de validación de correos
        run = client.actor("Bo8mCdzyMcSK2mbTN").call(run_input=run_input)
        
        # Obtener los resultados
        results = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            # Un correo es válido si es entregable
            is_valid = item.get('Deliverable Email', 'False') == 'True'
            results.append(is_valid)
        
        return results
    
    except Exception as e:
        print(f"Error al validar el lote de correos: {str(e)}")
        return [False] * len(emails)

try:
    # Leer el archivo Excel original
    input_file = 'Hubsport 20.xlsx'
    df = pd.read_excel(input_file)

    # Obtener API key desde variables de entorno
    apify_token = os.getenv('APIFY_API_KEY')
    if not apify_token:
        print("Error: No se encontró la variable de entorno APIFY_API_KEY")
        sys.exit(1)
    
    # Inicializar el cliente de Apify
    client = ApifyClient(apify_token)
    
    # Si la columna no existe, crearla
    if 'CorreoIsvalidated' not in df.columns:
        df['CorreoIsvalidated'] = None
    
    # Guardar una copia de los correos actuales para comparar
    if 'Correo_Anterior' not in df.columns:
        df['Correo_Anterior'] = df['Correo']
    
    # Identificar correos que necesitan validación:
    # 1. Correos que no han sido validados (isna)
    # 2. Correos que han sido editados (diferentes de Correa_Anterior)
    emails_to_validate = df[
        (df['CorreoIsvalidated'].isna()) | 
        (df['Correo'] != df['Correo_Anterior'])
    ]['Correo'].dropna().tolist()
    
    total_emails = len(emails_to_validate)
    
    if total_emails > 0:
        print(f"Iniciando validación de {total_emails} correos...")
        
        # Validar en lotes de 10 correos
        batch_size = 10
        validated_results = []
        
        for i in range(0, total_emails, batch_size):
            batch = emails_to_validate[i:i + batch_size]
            print(f"\nValidando correos {i+1} a {min(i+batch_size, total_emails)} de {total_emails}...")
            
            # Validar el lote actual
            batch_results = validate_emails_batch(batch, client)
            validated_results.extend(batch_results)
            
            # Esperar 1 segundo entre lotes para no sobrecargar la API
            if i + batch_size < total_emails:
                time.sleep(1)
        
        # Crear un diccionario de correos y sus resultados
        email_validation_dict = dict(zip(emails_to_validate, validated_results))
        
        # Actualizar los correos validados
        for email, is_valid in email_validation_dict.items():
            df.loc[df['Correo'] == email, 'CorreoIsvalidated'] = is_valid
        
        # Actualizar la columna de correos anteriores
        df['Correo_Anterior'] = df['Correo']
        
        try:
            # Guardar en el mismo archivo
            df.to_excel(input_file, index=False)
            print("\nValidación completada:")
            print(f"Total de correos en el archivo: {len(df)}")
            print(f"Correos válidos: {df['CorreoIsvalidated'].sum()}")
            print(f"Correos inválidos: {len(df) - df['CorreoIsvalidated'].sum()}")
            print(f"Correos pendientes de validar: {df['CorreoIsvalidated'].isna().sum()}")
        except PermissionError:
            print("\nError: No se puede escribir en el archivo '10 Datos.xlsx'")
            print("Por favor, asegúrate de:")
            print("1. Cerrar el archivo Excel si está abierto")
            print("2. Tener permisos de escritura en el archivo")
            sys.exit(1)
    else:
        print("No hay correos nuevos o modificados para validar")

except FileNotFoundError:
    print("\nError: No se encontró el archivo '10 Datos.xlsx'")
    print("Asegúrate de que el archivo existe en el directorio actual")
    sys.exit(1)
except Exception as e:
    print(f"\nError inesperado: {str(e)}")
    sys.exit(1)
