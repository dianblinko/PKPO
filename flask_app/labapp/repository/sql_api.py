from .connector import StoreConnector
from pandas import DataFrame, Series
from datetime import datetime

"""
    В данном модуле реализуется API (Application Programming Interface)
    для взаимодействия с БД с помощью объектов-коннекторов.
    
    ВАЖНО! Методы должны быть названы таким образом, чтобы по названию
    можно было понять выполняемые действия.
"""


def select_all_from_suicides_country(connector: StoreConnector):
    connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT sc.id, country, ins.sex, gdp_per_capita, ing.level_gdp, suicides_no, population, ' \
            f'suicides_on_100KPopulation, source_file FROM suicides_country sc, index_gdp ing, index_sex ins ' \
            f'WHERE sc.Level_gdp = ing.id AND ins.id = sc.sex'
    result = connector.execute(query).fetchall()
    connector.end_transaction()  # завершаем выполнение запросов
    return result

def select_all_from_suicides_year(connector: StoreConnector):
    connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT sy.id, sy."year", ins.sex, suicides_no, population, suicides_on_100KPopulation, source_file  ' \
            f'FROM suicides_year sy, index_sex ins WHERE ins.id = sy.sex'
    result = connector.execute(query).fetchall()
    connector.end_transaction()  # завершаем выполнение запросов
    return result

def select_all_from_suicides_age(connector: StoreConnector):
    connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT sa.id, sa.age, ins.sex, suicides_no, population, suicides_on_100KPopulation, source_file  ' \
            f'FROM suicides_age sa, index_sex ins WHERE ins.id = sa.sex '
    result = connector.execute(query).fetchall()
    connector.end_transaction()  # завершаем выполнение запросов
    return result

def select_all_from_source_files(connector: StoreConnector):
    connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT * FROM source_files ORDER BY processed'
    result = connector.execute(query).fetchall()
    connector.end_transaction()  # завершаем выполнение запросов
    return result


# Вставка в таблицу обработанных файлов
def insert_into_source_files(connector: StoreConnector, filename: str):
    now = datetime.now()  # текущая дата и время
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")  # преобразуем в формат SQL
    connector.start_transaction()
    query = f'INSERT INTO source_files (filename, processed) VALUES (\'{filename}\', \'{date_time}\')'
    result = connector.execute(query)
    connector.end_transaction()
    return result

# Вставка строк в таблицу suicides_country
def insert_rows_into_suicides_country(connector: StoreConnector, df: DataFrame):
    connector.start_transaction()
    foreign_key = connector.execute(f'SELECT MAX(id) FROM source_files').fetchone()[0]
    connector.execute(f'DELETE FROM suicides_country;')
    connector.execute(f'UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME=\'suicides_country\';')

    row_country = df.to_dict('records')
    for row in row_country:
        connector.execute(f'INSERT INTO suicides_country (country, sex, gdp_per_capita, suicides_no, population, '
                          f'Suicides_on_100KPopulation, Level_gdp, source_file) VALUES (\'{row["country"]}\', '
                          f'\'{row["sex"]}\', \'{row["gdp_per_capita"]}\', \'{row["suicides_no"]}\','
                          f'\'{row["population"]}\', \'{row["Suicides/100KPopulation"]}\', \'{row["Level_gdp"]}\', '
                          f'\'{foreign_key}\')')
    connector.end_transaction()

# Вставка строк в таблицу suicides_age
def insert_rows_into_suicides_age(connector: StoreConnector, df: DataFrame):
    connector.start_transaction()
    foreign_key = connector.execute(f'SELECT MAX(id) FROM source_files').fetchone()[0]
    connector.execute(f'DELETE FROM suicides_age;')
    connector.execute(f'UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME=\'suicides_age\';')

    row_age = df.to_dict('records')
    for row in row_age:
        connector.execute(f'INSERT INTO suicides_age (age, sex, suicides_no, population, Suicides_on_100KPopulation, '
                          f'source_file) VALUES (\'{row["age"]}\', \'{row["sex"]}\',  \'{row["suicides_no"]}\','
                          f'\'{row["population"]}\', \'{row["Suicides/100KPopulation"]}\', \'{foreign_key}\')')
    connector.end_transaction()

# Вставка строк в таблицу suicides_year
def insert_rows_into_suicides_year(connector: StoreConnector, df: DataFrame):
    connector.start_transaction()
    foreign_key = connector.execute(f'SELECT MAX(id) FROM source_files').fetchone()[0]
    connector.execute(f'DELETE FROM suicides_year;')
    connector.execute(f'UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME=\'suicides_year\';')

    row_year = df.to_dict('records')
    for row in row_year:
        connector.execute(f'INSERT INTO suicides_year (year, sex, suicides_no, population, Suicides_on_100KPopulation, '
                          f'source_file) VALUES (\'{row["year"]}\', \'{row["sex"]}\',  \'{row["suicides_no"]}\','
                          f'\'{row["population"]}\', \'{row["Suicides/100KPopulation"]}\', \'{foreign_key}\')')
    connector.end_transaction()
