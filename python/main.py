import os
from processor.dataprocessorfactory import *
from repository.connectorfactory import *       # подключаем фабрику коннекторов к БД
from repository.sql_api import *                # подключаем API для работы с БД
"""
    Пример простейшей функции, которая запускает обработчик данных и выводит результат обработки (возвращает None).
    
    ВАЖНО! Обратите внимание, что функция принимает в качестве аргумента базовый абстрактный класс DataProcessor
    и будет выполняться для любого типа обработчика данных (CSV или TXT), что позволяет в дальнейшем расширять
    приложение, просто добавляя другие классы обработчиков, которые, например, работают с базой данных или FTP-сервером.
    Основное условие для расширения - это сохранение формата выходных данных 
    (в данном примере результатом обработки является тип pandas.DataFrame)
"""
DATASOURCE = "suicide.csv"
DB_URL = 'sqlite:///test.db'
# В зависимости от расширения файла вызываем соответствующий фабричный метод
def init_processor(source: str) -> DataProcessor:
    proc = None
    if source.endswith('.csv'):
        proc = CsvDataProcessorFactory().get_processor(source)
    elif source.endswith('.txt'):
        proc = TxtDataProcessorFactory().get_processor(source)
    return proc

# Запуск обработки
def run_processor(proc: DataProcessor): # -> DataFrame:
    proc.run()
    proc.print_result()
    # list_result = [proc.result_country, proc.result_year, proc.result_age]
    # return list_result


if __name__ == '__main__':
    proc = init_processor(DATASOURCE)
    if proc is not None:
        run_processor(proc)
    # Работа с БД
    if proc.result_country is not None:
        db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)   # получаем объект соединения
        insert_into_source_files(db_connector, DATASOURCE)                # сохраняем в БД информацию о файле с набором данных
        print(select_all_from_source_files(db_connector))                 # вывод списка всеъ обработанных файлов
        # insert_rows_into_processed_data(db_connector, list_result, DATASOURCE)     # записываем в БД
        insert_rows_into_suicides_country(db_connector, proc.result_country)
        insert_rows_into_suicides_age(db_connector, proc.result_age)
        insert_rows_into_suicides_year(db_connector, proc.result_year)
        # Завершаем работу с БД
        db_connector.close()