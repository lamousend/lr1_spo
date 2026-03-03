import mysql.connector
import pandas as pd

class SQLTable:
    def __init__(self, db_config, table_name):
        self.db_config = db_config
        self.table_name = table_name
        self.connection = mysql.connector.connect(**db_config)
        self.cursor = self.connection.cursor()
        self.columns = []

        if self._check_table_exists():
            self._update_column_names()

    def _check_table_exists(self):
        query = f"SHOW TABLES LIKE '{self.table_name}'"
        self.cursor.execute(query)
        return self.cursor.fetchone() is not None

    def _update_column_names(self):
        query = f"SHOW COLUMNS FROM {self.table_name}"
        self.cursor.execute(query)
        self.columns = [row[0] for row in self.cursor.fetchall()]

                    # создание таблицы
    def create_table(self, columns):
        column_definition = ', '.join(f"`{name}` {type}" for name, type in columns.items())
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            {column_definition}
        )
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        finally:
            cursor.close()
        self._update_column_names()

                #insert
    def insert(self, data):
        columns = ', '.join(f"`{k}`" for k in data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({values})"
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, tuple(data.values()))
            self.connection.commit()
        finally:
            cursor.close()

                #select
    def select(self):
        cursor = self.connection.cursor()
        try:
            query = f'SELECT * FROM `{self.table_name}`'
            cursor.execute(query)
            films = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            # сделаем вывод в консоль для наглядности
            result = []
            for row in films:
                row_dict = dict(zip(col_names, row))
                result.append(row_dict)
                print(row_dict)
        finally:
            cursor.close()

            # добавление столбцов + проверка на существование
    def add_column(self, column_name, data_type):
            if column_name in self.columns:
                print(f'column `{column_name}` already exist in table')
                return
            query = f'ALTER TABLE `{self.table_name}` ADD COLUMN `{column_name}` {data_type}'
            cursor = self.connection.cursor()
            try:
                cursor.execute(query)
                self.connection.commit()
            finally:
                cursor.close()
            print(f"Column `{column_name}` of type '{data_type}' added to table '{self.table_name}'.")
            self._update_column_names()

            # update
    def update(self, data, where):
        set_part = ', '.join(f"`{k}`=%s" for k in data)
        where_part = ' AND '.join(f"`{k}`=%s" for k in where)

        query = f"""
        UPDATE `{self.table_name}`
        SET {set_part}
        WHERE {where_part}
        """

        params = tuple(data.values()) + tuple(where.values())
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        finally:
            cursor.close()

                # delete
    def delete(self, where):
        where_part = ' AND '.join(f"`{k}`=%s" for k in where)
        query = f"DELETE FROM `{self.table_name}` WHERE {where_part}"
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, tuple(where.values()))
            self.connection.commit()
        finally:
            cursor.close()

            # метод find_primary_key
    def _find_primary_key(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SHOW KEYS FROM {self.table_name} WHERE Key_name = 'PRIMARY'")
            result = cursor.fetchone()
            if result:
                return result[4]
        finally:
            cursor.close()
        return None

            # вывод конкретного столбца в порядке возрастания или убывания
    def sorted_column(self, column_name, order='ASC'):
        if column_name not in self.columns:
            print(f"Column `{column_name}` does not exist'")
            return

        query = f""" 
        SELECT `{column_name}`
        FROM `{self.table_name}`
        ORDER BY `{column_name}` {order}
        """

        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                print(row[0])
        finally:
            cursor.close()

            # вывод диапазона строк по айди
    def select_row_pk(self, start, end):
        pk = self._find_primary_key()
        if not pk:
            print('Primary key not found')
            return
        query = f"""
        SELECT * FROM `{self.table_name}`
        WHERE `{pk}` BETWEEN %s AND %s
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (start, end))
            rows = cursor.fetchall()
            col_name = [desc[0] for desc in cursor.description]

            for row in rows:
                print(dict(zip(col_name, row)))
        finally:
            cursor.close()

            # удаление диапазона строк по айди
    def del_row_pk(self, start, end):
        pk = self._find_primary_key()
        if not pk:
            print('Primary key not found')
            return
        query = f"""
        DELETE FROM `{self.table_name}`
        WHERE `{pk}` BETWEEN %s AND %s
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (start, end))
            self.connection.commit()
            print(f"Rows deleted where {pk} between {start} and {end}")
        finally:
            cursor.close()

            # вывод структуры таблицы
    def stucture(self):
        query = f"DESCRIBE `{self.table_name}`"

        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            struct = cursor.fetchall()

            for column in struct:
                print(column)
        finally:
            cursor.close()

            # вывод строки содержащей конкретное значение в конкретном столбце
    def find_value(self, column_name, value):
        if column_name not in self.columns:
            print(f"Column `{column_name}` does not exist")
            return

        query = f"""
        SELECT * FROM `{self.table_name}`
        WHERE `{column_name}` = %s
        """

        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (value,))
            rows = cursor.fetchall()
            col_name = [desc[0] for desc in cursor.description]
            for row in rows:
                print(dict(zip(col_name, row)))
        finally:
            cursor.close()

            # удаление таблицы
    def drop_table(self):
        query = f"DROP TABLE IF EXISTS `{self.table_name}`"

        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            print(f"`{self.table_name}` deleted")
        finally:
            cursor.close()

            # экспорт и импорт таблицы в csv
    def export_to_csv(self, file_name):
        cursor = self.connection.cursor()  # <- нужно, чтобы не вылезало предупреждения pandas
                                            # (более безопасный вариант)
        try:
            cursor.execute(f"SELECT * FROM `{self.table_name}`")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            data_frame = pd.DataFrame(rows, columns=columns)
            data_frame.to_csv(file_name, index=False)
            print(f"Exported to {file_name}")
        finally:
            cursor.close()

    def import_from_csv(self, file_name):
        data_frame = pd.read_csv(file_name)

        pk = self._find_primary_key()

        if pk and pk in data_frame.columns:
            data_frame=data_frame.drop(columns=[pk])

        data_frame = data_frame.where(pd.notnull(data_frame), None)

        for _, row in data_frame.iterrows():
            self.insert(row.to_dict())
        print(f"Imported from {file_name}")

                # использование
# создание таблицы
films_table = SQLTable(db_config, 'films')
films_table.create_table(columns= {
    'title': 'VARCHAR(255)',
    'age limit': 'INT'
})

# добавить столбец
films_table.add_column('genre', 'VARCHAR(255)')
films_table.add_column('film_score', 'INT')

# заполнить таблицу
films_table.insert({
    'title': 'Game of Thrones',
    'age limit': 18,
    'genre': 'fantasy',
    'film_score': 9
})

films_table.insert({
    'title': 'Zootopia',
    'age limit': 6,
    'genre': 'comedy',
    'film_score': 8
})

films_table.insert({
    'title': 'Astral',
    'age limit': 18,
    'genre': 'horror',
    'film_score': 7
})

films_table.insert({
    'title': 'Home alone',
    'age limit': 0,
    'genre': 'comedy',
    'film_score': 8
})

# Вывод конкретного столбца в порядке убывания или возрастания
films_table.sorted_column('age limit', 'ASC')

# Вывод диапазона строк по айди
films_table.select_row_pk(2,4)

# Удаление диапазона строк по айди
films_table.del_row_pk(1, 3)

# Вывод структуры таблицы
films_table.stucture()

# Вывод строки содержащей конкретное значение в конкретном столбце
films_table.find_value('genre', 'comedy')

# Экспорт и импорт таблицы в csv
films_table.export_to_csv('films.csv')
films_table.import_from_csv('films.csv')

# Добавление и удаление нового столбца
films_table.add_column('year_of_release', 'INT')
films_table.delete({'title': 'Astral'})

# Удаление таблицы
films_table.drop_table()
