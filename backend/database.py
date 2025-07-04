import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str = "profiles.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Inicializar la base de datos y crear las tablas necesarias"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Crear tabla de búsquedas
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS searches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        description TEXT,
                        search_url TEXT NOT NULL,
                        status TEXT DEFAULT 'active',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Crear tabla de perfiles
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS profiles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fullName TEXT,
                        headline TEXT,
                        linkedin_id TEXT,
                        lastName TEXT,
                        location TEXT,
                        picture TEXT,
                        profileId TEXT,
                        profileUrl TEXT UNIQUE,
                        email TEXT,
                        mobileNumber TEXT,
                        email_checked BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Crear tabla de relación muchos a muchos entre búsquedas y perfiles
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS search_profiles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        search_id INTEGER NOT NULL,
                        profile_id INTEGER NOT NULL,
                        found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (search_id) REFERENCES searches (id) ON DELETE CASCADE,
                        FOREIGN KEY (profile_id) REFERENCES profiles (id) ON DELETE CASCADE,
                        UNIQUE(search_id, profile_id)
                    )
                ''')
                
                # Crear índices para búsquedas rápidas
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_profile_url ON profiles(profileUrl)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_email_checked ON profiles(email_checked)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_search_profiles_search_id ON search_profiles(search_id)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_search_profiles_profile_id ON search_profiles(profile_id)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_searches_status ON searches(status)
                ''')
                
                conn.commit()
                logger.info("Base de datos inicializada correctamente")
                
        except Exception as e:
            logger.error(f"Error al inicializar la base de datos: {str(e)}")
            raise
    
    def insert_profile(self, profile_data: Dict) -> bool:
        """Insertar un nuevo perfil en la base de datos"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR IGNORE INTO profiles 
                    (fullName, headline, linkedin_id, lastName, location, picture, profileId, profileUrl, email, mobileNumber, email_checked)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    profile_data.get('fullName', ''),
                    profile_data.get('headline', ''),
                    profile_data.get('id', ''),
                    profile_data.get('lastName', ''),
                    profile_data.get('location', ''),
                    profile_data.get('picture', ''),
                    profile_data.get('profileId', ''),
                    profile_data.get('profileUrl', ''),
                    profile_data.get('email', ''),
                    profile_data.get('mobileNumber', ''),
                    profile_data.get('email_checked', False)
                ))
                
                conn.commit()
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Error al insertar perfil: {str(e)}")
            return False
    
    def insert_profiles_batch(self, profiles: List[Dict]) -> int:
        """Insertar múltiples perfiles en la base de datos"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                data_to_insert = []
                for profile in profiles:
                    data_to_insert.append((
                        profile.get('fullName', ''),
                        profile.get('headline', ''),
                        profile.get('id', ''),
                        profile.get('lastName', ''),
                        profile.get('location', ''),
                        profile.get('picture', ''),
                        profile.get('profileId', ''),
                        profile.get('profileUrl', ''),
                        profile.get('email', ''),
                        profile.get('mobileNumber', ''),
                        profile.get('email_checked', False)
                    ))
                
                cursor.executemany('''
                    INSERT OR IGNORE INTO profiles 
                    (fullName, headline, linkedin_id, lastName, location, picture, profileId, profileUrl, email, mobileNumber, email_checked)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_to_insert)
                
                conn.commit()
                return cursor.rowcount
                
        except Exception as e:
            logger.error(f"Error al insertar perfiles en lote: {str(e)}")
            return 0
    
    def get_all_profiles(self) -> List[Dict]:
        """Obtener todos los perfiles de la base de datos"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM profiles ORDER BY created_at DESC
                ''')
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error al obtener perfiles: {str(e)}")
            return []
    
    def get_profiles_without_email(self) -> List[Dict]:
        """Obtener perfiles que no han sido verificados para email"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM profiles 
                    WHERE email_checked = FALSE 
                    AND profileUrl IS NOT NULL 
                    AND profileUrl != ''
                    ORDER BY created_at DESC
                ''')
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error al obtener perfiles sin email: {str(e)}")
            return []
    
    def update_profile_contact_info(self, profile_url: str, email: str, mobile_number: str) -> bool:
        """Actualizar información de contacto de un perfil"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE profiles 
                    SET email = ?, mobileNumber = ?, email_checked = TRUE, updated_at = CURRENT_TIMESTAMP
                    WHERE profileUrl = ?
                ''', (email, mobile_number, profile_url))
                
                conn.commit()
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Error al actualizar información de contacto: {str(e)}")
            return False

    def update_profile_contact(self, profile_id: int, email: str, mobile_number: str) -> bool:
        """Actualizar información de contacto de un perfil por ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE profiles 
                    SET email = ?, mobileNumber = ?, email_checked = TRUE, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (email, mobile_number, profile_id))
                
                conn.commit()
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Error al actualizar información de contacto: {str(e)}")
            return False
    
    def update_profiles_contact_info_batch(self, contact_updates: List[Dict]) -> int:
        """Actualizar información de contacto de múltiples perfiles"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                updated_count = 0
                for contact in contact_updates:
                    profile_url = contact.get('profileUrl', '')
                    email = contact.get('email', '')
                    mobile_number = contact.get('mobileNumber', '')
                    
                    if profile_url:
                        cursor.execute('''
                            UPDATE profiles 
                            SET email = ?, mobileNumber = ?, email_checked = TRUE, updated_at = CURRENT_TIMESTAMP
                            WHERE profileUrl = ?
                        ''', (email, mobile_number, profile_url))
                        updated_count += cursor.rowcount
                
                conn.commit()
                return updated_count
                
        except Exception as e:
            logger.error(f"Error al actualizar información de contacto en lote: {str(e)}")
            return 0
    
    def get_profile_count(self) -> int:
        """Obtener el número total de perfiles"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM profiles')
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error al obtener conteo de perfiles: {str(e)}")
            return 0

    def get_profile_id_by_url(self, profile_url: str) -> Optional[int]:
        """Obtener el ID de un perfil por su URL"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT id FROM profiles WHERE profileUrl = ?', (profile_url,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Error al obtener ID de perfil por URL: {str(e)}")
            return None

    def get_profile_ids_by_urls(self, profile_urls: List[str]) -> Dict[str, int]:
        """Obtener los IDs de múltiples perfiles por sus URLs"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Filtrar URLs válidas
                valid_urls = [url for url in profile_urls if url is not None and url.strip()]
                if not valid_urls:
                    return {}
                
                # Crear placeholders para la consulta SQL
                placeholders = ','.join(['?' for _ in valid_urls])
                cursor.execute(f'SELECT id, profileUrl FROM profiles WHERE profileUrl IN ({placeholders})', valid_urls)
                
                results = cursor.fetchall()
                return {row[1]: row[0] for row in results}
                
        except Exception as e:
            logger.error(f"Error al obtener IDs de perfiles por URLs: {str(e)}")
            return {}
    
    def get_profiles_with_emails(self) -> List[Dict]:
        """Obtener perfiles que tienen emails"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM profiles 
                    WHERE email IS NOT NULL AND email != '' 
                    ORDER BY created_at DESC
                ''')
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error al obtener perfiles con emails: {str(e)}")
            return []

    def debug_profiles_status(self) -> Dict:
        """Método de debug para verificar el estado de los perfiles"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total de perfiles
                cursor.execute('SELECT COUNT(*) FROM profiles')
                total_profiles = cursor.fetchone()[0]
                
                # Perfiles con email
                cursor.execute('SELECT COUNT(*) FROM profiles WHERE email IS NOT NULL AND email != ""')
                profiles_with_email = cursor.fetchone()[0]
                
                # Perfiles marcados como email_checked = TRUE
                cursor.execute('SELECT COUNT(*) FROM profiles WHERE email_checked = TRUE')
                profiles_checked = cursor.fetchone()[0]
                
                # Perfiles marcados como email_checked = FALSE
                cursor.execute('SELECT COUNT(*) FROM profiles WHERE email_checked = FALSE')
                profiles_unchecked = cursor.fetchone()[0]
                
                # Perfiles con URL válida
                cursor.execute('SELECT COUNT(*) FROM profiles WHERE profileUrl IS NOT NULL AND profileUrl != ""')
                profiles_with_url = cursor.fetchone()[0]
                
                return {
                    'total_profiles': total_profiles,
                    'profiles_with_email': profiles_with_email,
                    'profiles_checked': profiles_checked,
                    'profiles_unchecked': profiles_unchecked,
                    'profiles_with_url': profiles_with_url
                }
                
        except Exception as e:
            logger.error(f"Error en debug_profiles_status: {str(e)}")
            return {}
    
    def export_to_excel(self, filename: str = "profiles_export.xlsx") -> bool:
        """Exportar todos los perfiles a un archivo Excel"""
        try:
            profiles = self.get_all_profiles()
            if not profiles:
                logger.warning("No hay perfiles para exportar")
                return False
            
            df = pd.DataFrame(profiles)
            df.to_excel(filename, index=False)
            logger.info(f"Perfiles exportados a {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error al exportar a Excel: {str(e)}")
            return False
    
    def migrate_from_excel(self, excel_path: str) -> int:
        """Migrar datos desde un archivo Excel existente"""
        try:
            if not os.path.exists(excel_path):
                logger.error(f"Archivo Excel no encontrado: {excel_path}")
                return 0
            
            df = pd.read_excel(excel_path)
            profiles = df.to_dict('records')
            
            # Limpiar datos NaN
            for profile in profiles:
                for key, value in profile.items():
                    if pd.isna(value):
                        profile[key] = ''
            
            # Insertar en la base de datos
            inserted_count = self.insert_profiles_batch(profiles)
            logger.info(f"Migrados {inserted_count} perfiles desde {excel_path}")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error al migrar desde Excel: {str(e)}")
            return 0
    
    def delete_profile(self, profile_id: int) -> bool:
        """Eliminar un perfil por ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM profiles WHERE id = ?', (profile_id,))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error al eliminar perfil: {str(e)}")
            return False
    
    def search_profiles(self, search_term: str) -> List[Dict]:
        """Buscar perfiles por nombre o ubicación"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM profiles 
                    WHERE fullName LIKE ? OR location LIKE ? OR headline LIKE ?
                    ORDER BY created_at DESC
                ''', (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%'))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error al buscar perfiles: {str(e)}")
            return []

    # Métodos para manejar búsquedas
    def create_search(self, name: str, description: str, search_url: str) -> Optional[int]:
        """Crear una nueva búsqueda"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO searches (name, description, search_url)
                    VALUES (?, ?, ?)
                ''', (name, description, search_url))
                
                conn.commit()
                return cursor.lastrowid
                
        except Exception as e:
            logger.error(f"Error al crear búsqueda: {str(e)}")
            return None

    def get_all_searches(self) -> List[Dict]:
        """Obtener todas las búsquedas"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM searches ORDER BY created_at DESC
                ''')
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error al obtener búsquedas: {str(e)}")
            return []

    def get_search_by_id(self, search_id: int) -> Optional[Dict]:
        """Obtener una búsqueda por ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM searches WHERE id = ?
                ''', (search_id,))
                
                row = cursor.fetchone()
                return dict(row) if row else None
                
        except Exception as e:
            logger.error(f"Error al obtener búsqueda: {str(e)}")
            return None

    def update_search(self, search_id: int, name: Optional[str] = None, description: Optional[str] = None, status: Optional[str] = None) -> bool:
        """Actualizar una búsqueda"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                updates = []
                params = []
                
                if name is not None:
                    updates.append("name = ?")
                    params.append(name)
                
                if description is not None:
                    updates.append("description = ?")
                    params.append(description)
                
                if status is not None:
                    updates.append("status = ?")
                    params.append(status)
                
                if not updates:
                    return False
                
                updates.append("updated_at = CURRENT_TIMESTAMP")
                params.append(search_id)
                
                query = f"UPDATE searches SET {', '.join(updates)} WHERE id = ?"
                cursor.execute(query, params)
                
                conn.commit()
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Error al actualizar búsqueda: {str(e)}")
            return False

    def delete_search(self, search_id: int) -> bool:
        """Eliminar una búsqueda y sus relaciones"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Eliminar relaciones primero (CASCADE debería hacerlo automáticamente)
                cursor.execute('DELETE FROM search_profiles WHERE search_id = ?', (search_id,))
                
                # Eliminar la búsqueda
                cursor.execute('DELETE FROM searches WHERE id = ?', (search_id,))
                
                conn.commit()
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Error al eliminar búsqueda: {str(e)}")
            return False

    def get_profiles_by_search(self, search_id: int) -> List[Dict]:
        """Obtener todos los perfiles de una búsqueda específica"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT p.*, sp.found_at 
                    FROM profiles p
                    INNER JOIN search_profiles sp ON p.id = sp.profile_id
                    WHERE sp.search_id = ?
                    ORDER BY sp.found_at DESC
                ''', (search_id,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error al obtener perfiles de búsqueda: {str(e)}")
            return []

    def get_search_profile_count(self, search_id: int) -> int:
        """Obtener el número de perfiles de una búsqueda específica"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT COUNT(*) FROM search_profiles WHERE search_id = ?
                ''', (search_id,))
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error al obtener conteo de perfiles de búsqueda: {str(e)}")
            return 0

    def add_profile_to_search(self, search_id: int, profile_id: int) -> bool:
        """Agregar un perfil a una búsqueda específica"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR IGNORE INTO search_profiles (search_id, profile_id)
                    VALUES (?, ?)
                ''', (search_id, profile_id))
                
                conn.commit()
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Error al agregar perfil a búsqueda: {str(e)}")
            return False

    def add_profiles_to_search_batch(self, search_id: int, profile_ids: List[int]) -> int:
        """Agregar múltiples perfiles a una búsqueda específica"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                data_to_insert = [(search_id, profile_id) for profile_id in profile_ids]
                
                cursor.executemany('''
                    INSERT OR IGNORE INTO search_profiles (search_id, profile_id)
                    VALUES (?, ?)
                ''', data_to_insert)
                
                conn.commit()
                return cursor.rowcount
                
        except Exception as e:
            logger.error(f"Error al agregar perfiles a búsqueda en lote: {str(e)}")
            return 0

    def get_search_statistics(self) -> Dict:
        """Obtener estadísticas de todas las búsquedas"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total de búsquedas
                cursor.execute('SELECT COUNT(*) FROM searches')
                total_searches = cursor.fetchone()[0]
                
                # Búsquedas activas
                cursor.execute("SELECT COUNT(*) FROM searches WHERE status = 'active'")
                active_searches = cursor.fetchone()[0]
                
                # Total de perfiles únicos
                cursor.execute('SELECT COUNT(DISTINCT profile_id) FROM search_profiles')
                unique_profiles = cursor.fetchone()[0]
                
                # Búsquedas con más perfiles
                cursor.execute('''
                    SELECT s.name, COUNT(sp.profile_id) as profile_count
                    FROM searches s
                    LEFT JOIN search_profiles sp ON s.id = sp.search_id
                    GROUP BY s.id, s.name
                    ORDER BY profile_count DESC
                    LIMIT 5
                ''')
                top_searches = [{'name': str(row[0]), 'count': row[1]} for row in cursor.fetchall()]
                
                return {
                    'total_searches': total_searches,
                    'active_searches': active_searches,
                    'unique_profiles': unique_profiles,
                    'top_searches': top_searches
                }
                
        except Exception as e:
            logger.error(f"Error al obtener estadísticas de búsquedas: {str(e)}")
            return {}

# Instancia global de la base de datos
db_manager = DatabaseManager() 