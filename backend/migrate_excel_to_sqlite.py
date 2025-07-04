#!/usr/bin/env python3
"""
Script para migrar datos desde archivos Excel existentes a la base de datos SQLite3
"""

import os
import sys
import logging
from database import db_manager

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def migrate_excel_files():
    """Migrar todos los archivos Excel de la carpeta Excels a la base de datos"""
    
    excel_dir = "Excels"
    if not os.path.exists(excel_dir):
        logger.error(f"El directorio {excel_dir} no existe")
        return False
    
    # Listar todos los archivos Excel
    excel_files = [f for f in os.listdir(excel_dir) if f.endswith('.xlsx')]
    
    if not excel_files:
        logger.warning(f"No se encontraron archivos Excel en {excel_dir}")
        return False
    
    logger.info(f"Archivos Excel encontrados: {excel_files}")
    
    total_migrated = 0
    
    for excel_file in excel_files:
        excel_path = os.path.join(excel_dir, excel_file)
        logger.info(f"Migrando archivo: {excel_file}")
        
        try:
            # Migrar el archivo a la base de datos
            migrated_count = db_manager.migrate_from_excel(excel_path)
            total_migrated += migrated_count
            
            logger.info(f"Migrados {migrated_count} perfiles desde {excel_file}")
            
        except Exception as e:
            logger.error(f"Error al migrar {excel_file}: {str(e)}")
            continue
    
    logger.info(f"Migración completada. Total de perfiles migrados: {total_migrated}")
    
    # Mostrar estadísticas finales
    total_profiles = db_manager.get_profile_count()
    profiles_with_emails = len(db_manager.get_profiles_with_emails())
    
    logger.info(f"Estadísticas finales:")
    logger.info(f"- Total de perfiles en la base de datos: {total_profiles}")
    logger.info(f"- Perfiles con emails: {profiles_with_emails}")
    logger.info(f"- Perfiles sin emails: {total_profiles - profiles_with_emails}")
    
    return True

def backup_excel_files():
    """Crear una copia de seguridad de los archivos Excel antes de eliminarlos"""
    
    excel_dir = "Excels"
    backup_dir = "Excels_backup"
    
    if not os.path.exists(excel_dir):
        logger.warning(f"El directorio {excel_dir} no existe, no hay nada que respaldar")
        return True
    
    # Crear directorio de backup si no existe
    os.makedirs(backup_dir, exist_ok=True)
    
    excel_files = [f for f in os.listdir(excel_dir) if f.endswith('.xlsx')]
    
    if not excel_files:
        logger.warning(f"No se encontraron archivos Excel para respaldar")
        return True
    
    logger.info(f"Creando backup de {len(excel_files)} archivos Excel...")
    
    for excel_file in excel_files:
        source_path = os.path.join(excel_dir, excel_file)
        backup_path = os.path.join(backup_dir, excel_file)
        
        try:
            import shutil
            shutil.copy2(source_path, backup_path)
            logger.info(f"Backup creado: {excel_file}")
        except Exception as e:
            logger.error(f"Error al crear backup de {excel_file}: {str(e)}")
            return False
    
    logger.info(f"Backup completado en el directorio: {backup_dir}")
    return True

def delete_excel_files():
    """Eliminar los archivos Excel originales después de la migración"""
    
    excel_dir = "Excels"
    
    if not os.path.exists(excel_dir):
        logger.warning(f"El directorio {excel_dir} no existe")
        return True
    
    excel_files = [f for f in os.listdir(excel_dir) if f.endswith('.xlsx')]
    
    if not excel_files:
        logger.warning(f"No se encontraron archivos Excel para eliminar")
        return True
    
    logger.info(f"Eliminando {len(excel_files)} archivos Excel...")
    
    for excel_file in excel_files:
        file_path = os.path.join(excel_dir, excel_file)
        try:
            os.remove(file_path)
            logger.info(f"Eliminado: {excel_file}")
        except Exception as e:
            logger.error(f"Error al eliminar {excel_file}: {str(e)}")
            return False
    
    # Intentar eliminar el directorio si está vacío
    try:
        os.rmdir(excel_dir)
        logger.info(f"Directorio {excel_dir} eliminado")
    except OSError:
        logger.info(f"El directorio {excel_dir} no está vacío o no se puede eliminar")
    
    return True

def main():
    """Función principal del script de migración"""
    
    print("=== Migración de Excel a SQLite3 ===")
    print()
    
    # Verificar que la base de datos esté inicializada
    try:
        profile_count = db_manager.get_profile_count()
        logger.info(f"Base de datos inicializada. Perfiles existentes: {profile_count}")
    except Exception as e:
        logger.error(f"Error al inicializar la base de datos: {str(e)}")
        return False
    
    # Paso 1: Crear backup de los archivos Excel
    print("1. Creando backup de archivos Excel...")
    if not backup_excel_files():
        logger.error("Error al crear backup de archivos Excel")
        return False
    
    # Paso 2: Migrar datos a SQLite3
    print("2. Migrando datos a SQLite3...")
    if not migrate_excel_files():
        logger.error("Error al migrar datos a SQLite3")
        return False
    
    # Paso 3: Confirmar eliminación
    print()
    response = input("¿Desea eliminar los archivos Excel originales? (s/n): ").lower().strip()
    
    if response in ['s', 'si', 'sí', 'y', 'yes']:
        print("3. Eliminando archivos Excel originales...")
        if not delete_excel_files():
            logger.error("Error al eliminar archivos Excel")
            return False
        print("Archivos Excel eliminados. El backup se mantiene en 'Excels_backup/'")
    else:
        print("Los archivos Excel originales se mantienen.")
    
    print()
    print("=== Migración completada exitosamente ===")
    print("Los datos ahora están almacenados en la base de datos SQLite3")
    print("Puede usar la aplicación normalmente con la nueva base de datos")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 