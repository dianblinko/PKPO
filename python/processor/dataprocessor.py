from abc import ABC, abstractmethod     # подключаем инструменты для создания абстрактных классов
import pandas   # библиотека для работы с датасетами
import os

"""
    В данном модуле реализуются классы обработчиков для 
    применения алгоритма обработки к различным типам файлов (csv или txt).
    
    ВАЖНО! Если реализация различных обработчиков занимает большое 
    количество строк, то необходимо оформлять каждый класс в отдельном файле
"""

# Родительский класс для обработчиков файлов
class DataProcessor(ABC):
    def __init__(self, datasource):
        # общие атрибуты для классов обработчиков данных
        self._datasource = datasource   # путь к источнику данных
        self._dataset = None            # входной набор данных
        self.result_country = None              # выходной набор данных (результат обработки)
        self.result_year = None
        self.result_age = None

    # Метод, инициализирующий источник данных
    # Все методы, помеченные декоратором @abstractmethod, ОБЯЗАТЕЛЬНЫ для переобределения
    @abstractmethod
    def read(self) -> bool:
        pass

    # Точка запуска методов обработки данных
    @abstractmethod
    def run(self):
        pass

    # Абстрактный метод для вывоа результата на экран
    @abstractmethod
    def print_result(self):
        pass

    """
        Пример одного из общих методов обработки данных.
        В данном случае метод просто сортирует входной датасет по значению заданной колонки (аргумент col)
        
        ВАЖНО! Следует логически разделять методы обработки, например, отдельный метод для сортировки, 
        отдельный метод для удаления "пустот" в датасете и т.д. Это позволит гибко применять необходимые
        методы при переопределении метода run для того или иного типа обработчика.
        НАПРИМЕР, если ваш источник данных это не файл, а база данных, тогда метод сортировки будет не нужен,
        т.к. сортировку можно сделать при выполнении SQL-запроса типа SELECT ... ORDER BY...
    """
    def calculate_suicides_100K(self, df) -> pandas.DataFrame:
        df["Suicides/100KPopulation"] = df.suicides_no / df.population * 100000
        # df = df.assign(Suicides/100KPopulation=)
        return df

    # Проверить работоспособность очистки данных
    def clear_data(self, df) -> pandas.DataFrame:
        feature_cols = list(df.columns)
        for col_name in feature_cols:
            df.drop(df.loc[df[col_name].isna(), [col_name]].index, inplace=True)
        return df

    def encoding_sex(self, df):
        df.loc[df['sex'] == "male", 'sex'] = 1
        df.loc[df['sex'] == "female", 'sex'] = 2
        return df

    def process_country_table(self, df) -> pandas.DataFrame:
        df1 = df.groupby(["country", "sex"]).agg({'gdp_per_capita': 'mean', 'suicides_no': 'mean', 'population': "mean"})
        df1 = self.calculate_suicides_100K(df1)
        df1 = df1.sort_values(by=["Suicides/100KPopulation"], ascending=False).reset_index()
        df1["Level_gdp"] = 0
        for row in df1.index:
            df1['Level_gdp'][row] = 1 if df1['gdp_per_capita'][row] > 12616 else 3 if df1['gdp_per_capita'][row] < 1035 else 2
        feature_cols = list(df1.columns)
        df1 = self.encoding_sex(df1)
        df1 = df1.round({'gdp_per_capita': 2, 'suicides_no': 2, 'population': 2, 'Suicides/100KPopulation': 3})
        return df1

    def process_year_table(self, df) -> pandas.DataFrame:
        df2 = df.groupby(["year", "sex"]).agg({'suicides_no': 'sum', 'population': "sum"})
        df2 = self.calculate_suicides_100K(df2)
        df2 = df2.sort_values(by=['year', 'sex'], ascending=False).reset_index()
        df2 = self.encoding_sex(df2)
        df2 = df2.round({ 'Suicides/100KPopulation': 3})
        return df2

    def process_age_table(self, df) -> pandas.DataFrame:
        df3 = df.groupby(["age", "sex"]).agg({'suicides_no': 'sum', 'population': "sum"})
        df3 = self.calculate_suicides_100K(df3)
        df3 = df3.sort_values(by=['age', 'sex'], ascending=False) .reset_index()
        df3 = self.encoding_sex(df3)
        df3 = df3.round({ 'Suicides/100KPopulation': 3})
        return df3

# Реализация класса-обработчика csv-файлов
class CsvDataProcessor(DataProcessor):
    # Переобпределяем конструктор родительского класса
    def __init__(self, datasource):
        DataProcessor.__init__(self, datasource)    # инициализируем конструктор родительского класса для получения общих атрибутов
        self.separator = ';'        # дополнительный атрибут - сепаратор по умолчанию
    """
        Переопределяем метод инициализации источника данных.
        Т.к. данный класс предназначен для чтения CSV-файлов, то используем метод read_csv
        из библиотеки pandas
    """
    def read(self):
        try:
            self._dataset = pandas.read_csv(self._datasource, sep=self.separator, header='infer', names=None, encoding="utf-8")
            # Читаем имена колонок из файла данных
            col_names = self._dataset.columns
            # Если количество считанных колонок < 2 возвращаем false
            if len(col_names) < 2:
                return False
            return True
        except Exception as e:
            print(str(e))
            return False

    def run(self):
        self._dataset = self.clear_data(self._dataset)
        self.result_country = self.process_country_table(self._dataset)
        self.result_year = self.process_year_table(self._dataset)
        self.result_age = self.process_age_table(self._dataset)

    def print_result(self):
        print(f'Running CSV-file processor!\n', self.result_country, f'\n', self.result_year, f'\n', self.result_age)


# Реализация класса-обработчика txt-файлов
class TxtDataProcessor(DataProcessor):
    # Реализация метода для чтения TXT-файла
    def read(self):
        try:
            self._dataset = pandas.read_table(self._datasource, sep='\t', engine='python')
            col_names = self._dataset.columns
            if len(col_names) < 2:
                return False
            return True
        except Exception as e:
            print(str(e))
            return False

    def run(self):
        self._dataset = self.clear_data(self._dataset)
        self.result_country = self.process_country_table(self._dataset)
        self.result_year = self.process_year_table(self._dataset)
        self.result_age = self.process_age_table(self._dataset)


    def print_result(self):
        print(f'Running TXT-file processor!\n', self.result_country, f'\n', self.result_year, f'\n', self.result_age)
