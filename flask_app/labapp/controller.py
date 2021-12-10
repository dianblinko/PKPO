from config import DATABASE_URI                 # параметры подключения к БД из модуля конфигурации config.py
from .repository.connectorfactory import *       # подключаем фабрику коннекторов к БД
from .repository.sql_api import *                # подключаем API для работы с БД

"""
    В данном модуле реализуются логика обработки клиентских запросов.
    Здесь также могут применяться SQL-методы, представленные в модуле repository.sql_api
"""

# Структура основного навигационнго меню веб-приложения,
# оформленное в виде массива dict объектов
navmenu = [
    {
        'name': 'Home',
        'addr': '/'
    },
    {
        'name': 'About us',
        'addr': '/aboutus'
    },
    {
        'name': 'Suicides country',
        'addr': '/suicides_country'
    },
    {
        'name': 'Suicides year',
        'addr': '/suicides_year'
    },
    {
        'name': 'Suicides age',
        'addr': '/suicides_age'
    },
]

# Получаем список обработанных файлов
def get_suicides_country():
    db_connector = SQLStoreConnectorFactory().get_connector(DATABASE_URI)  # получаем объект соединения
    result = select_all_from_suicides_country(db_connector)  # получаем список всех обработанных файлов
    # Завершаем работу с БД
    db_connector.close()
    return result


def get_suicides_year():
    db_connector = SQLStoreConnectorFactory().get_connector(DATABASE_URI)  # получаем объект соединения
    result = select_all_from_suicides_year(db_connector)  # получаем список всех обработанных файлов
    # Завершаем работу с БД
    db_connector.close()
    return result

def get_suicides_age():
    db_connector = SQLStoreConnectorFactory().get_connector(DATABASE_URI)  # получаем объект соединения
    result = select_all_from_suicides_age(db_connector)  # получаем список всех обработанных файлов
    # Завершаем работу с БД
    db_connector.close()
    return result