"""
Конфигурационный файл для CRM-системы ломбарда
Все настройки загружаются из .env файла
"""
import os
from dotenv import load_dotenv
from pathlib import Path

# Загружаем переменные окружения из .env файла
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

def get_database_uri():
    """Формирует строку подключения к БД для asyncpg из переменных окружения"""
    # Если указан DATABASE_URL, используем его
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        # Если URL использует postgresql://, заменяем на postgresql+asyncpg:// для asyncpg
        if database_url.startswith('postgresql://') and not database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif database_url.startswith('postgresql+psycopg://'):
            database_url = database_url.replace('postgresql+psycopg://', 'postgresql+asyncpg://', 1)
        return database_url
    
    # Иначе формируем из отдельных параметров из .env
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', '')
    db_name = os.getenv('DB_NAME', '')
    
    # Используем postgresql+asyncpg:// для asyncpg
    return f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

class Config:
    """Базовая конфигурация - все настройки из .env"""
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    DATABASE_URI = get_database_uri()
    FLASK_ENV = os.getenv('FLASK_ENV', 'development')

class DevelopmentConfig(Config):
    """Конфигурация для разработки"""
    DEBUG = True
    FLASK_ENV = 'development'

class ProductionConfig(Config):
    """Конфигурация для продакшена - настройки из .env"""
    DEBUG = os.getenv('DEBUG', 'False').lower() in ('true', '1', 'yes')
    FLASK_ENV = os.getenv('FLASK_ENV', 'production')
    SECRET_KEY = os.getenv('SECRET_KEY')  # В продакшене обязательно из переменных окружения

class TestingConfig(Config):
    """Конфигурация для тестирования"""
    TESTING = True
    DB_NAME = os.getenv('TEST_DB_NAME', 'lombard_db_test')
    
    @property
    def DATABASE_URI(self):
        """Переопределяем для тестовой БД - настройки из .env"""
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', '')
        return f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{self.DB_NAME}"

# Словарь конфигураций
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}

