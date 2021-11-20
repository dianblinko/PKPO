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
        self.result = None              # выходной набор данных (результат обработки)
        self.result2 = None
        self.result3 = None

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

    # Проверить работоспособность очистки данных
    def clear_data(self, df) -> pandas.DataFrame:
        feature_cols = list(df.columns)
        for col_name in feature_cols:
            df.drop(df.loc[df[col_name].isna(), [col_name]].index, inplace=True)
        return df

    def process_first_table(self, df) -> pandas.DataFrame:
        df1 = df.groupby(["country", "sex"]).agg({'gdp_per_capita': 'mean', 'suicides_no': 'mean', 'population': "mean"})
        df1["Suicides/100KPopulation"] = df1.suicides_no / df1.population * 100000
        df1 = df1.sort_values(by=['Suicides/100KPopulation'], ascending=False).reset_index()
        df1["Level_gdp"] = 0
        # ????????????????????????????
        # Должна ли загружаться таблица индексов, где 1 - Высокий, 2 - Средний, 3 - Низкий, для ВВП при каждой обработке данных, или достаточно заранее загрузить ее в SQL????????
        # ????????????????????????????
        for row in df1.index:
            df1['Level_gdp'][row] = 1 if df1['gdp_per_capita'][row] > 12616 else 3 if df1['gdp_per_capita'][row] < 1035 else 2
        return df1

    def process_second_table(self, df) -> pandas.DataFrame:
        df2 = df.groupby(["year", "sex"]).agg({'suicides_no': 'sum', 'population': "sum"})
        df2["Suicides/100KPopulation"] = df2.suicides_no / df2.population * 100000
        df2 = df2.sort_values(by=['year', 'sex'], ascending=False).reset_index()
        return df2

    def process_third_table(self, df) -> pandas.DataFrame:
        df3 = df.groupby(["age", "sex"]).agg({'suicides_no': 'sum', 'population': "sum"})
        df3["Suicides/100KPopulation"] = df3.suicides_no / df3.population * 100000
        df3 = df3.sort_values(by=['age', 'sex'], ascending=False) .reset_index()
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
        self.result = self.process_first_table(self._dataset)
        self.result2 = self.process_second_table(self._dataset)
        self.result3 = self.process_third_table(self._dataset)

    def print_result(self):
        print(f'Running CSV-file processor!\n', self.result, f'\n', self.result2, f'\n', self.result3)


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
        self.result = self.process_first_table(self._dataset)
        self.result2 = self.process_second_table(self._dataset)
        self.result3 = self.process_third_table(self._dataset)


    def print_result(self):
        print(f'Running TXT-file processor!\n', self.result, f'\n', self.result2, f'\n', self.result3)
