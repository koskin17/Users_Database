import pathlib
import os
import tempfile
import subprocess

import psycopg2
from psycopg2 import pool
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
from typing import Optional

from PyQt5.QtWidgets import QMainWindow, QLabel, QPushButton, QMessageBox, QFileDialog, QInputDialog, QWidget, \
    QVBoxLayout
from PyQt5.QtGui import QIcon, QPixmap, QFont

import pandas as pd
from datetime import datetime

df_users = pd.DataFrame
countries = set()
df_scans = pd.DataFrame

class MainWindow(QMainWindow):
    db_pool: Optional[SimpleConnectionPool] = None

    def __init__(self):
        super().__init__()

        load_dotenv()
        self.db_connection()
        
        self.resize(620, 600)
        self.setWindowTitle("Данные по пользователя и сканам в приложении AXOR")
        self.setWindowIcon(QIcon('Pictures/axor.ico'))

        self.label = QLabel()
        self.label.setPixmap(QPixmap('Pictures/axor_logo.png'))

        self.btn_about_users = QPushButton("Все пользователи в базе", self)
        self.btn_about_users.move(0, 175)
        self.btn_about_users.setFont(QFont('Font/pfdintextpro-thinitalic.ttf', 14, 50, False))
        self.btn_about_users.clicked.connect(self.cleaning_the_user_base_from_spam)

        self.btn_users_by_country = QPushButton("Пользователи по странам", self)
        self.btn_users_by_country.move(0, 205)
        self.btn_users_by_country.clicked.connect(self.users_by_country)

        self.btn_last_authorization_in_app = QPushButton("Авторизация пользователей в приложении", self)
        self.btn_last_authorization_in_app.move(0, 235)
        self.btn_last_authorization_in_app.clicked.connect(self.last_authorization_in_app)

        self.btn_authorization_in_period = QPushButton("Авторизация пользователей за период", self)
        self.btn_authorization_in_period.move(0, 265)
        self.btn_authorization_in_period.clicked.connect(self.authorization_during_period)

        self.btn_points_by_users_and_countries = QPushButton("Общая информация по баллам на текущий момент", self)
        self.btn_points_by_users_and_countries.move(0, 295)
        self.btn_points_by_users_and_countries.clicked.connect(self.points_by_users_and_countries)

        self.btn_about_scans = QPushButton("Загрузить базу сканирований", self)
        self.btn_about_scans.move(0, 345)
        self.btn_about_scans.setFont(QFont('Font/pfdintextpro-thinitalic.ttf', 14, 50, False))
        self.btn_about_scans.move(0, 175)
        # self.btn_about_scans.clicked.connect(self.check_file_with_scans)

        self.btn_data_about_scan_users_in_current_year = QPushButton(
            "Кол-во сканировавших пользователей в текущем году на данный момент", self)
        self.btn_data_about_scan_users_in_current_year.move(0, 375)
        self.btn_data_about_scan_users_in_current_year.clicked.connect(self.data_about_scan_users_in_current_year)

        self.btn_data_about_points = QPushButton("Насканировано баллов в текущем году на данный момент",
                                                 self)
        self.btn_data_about_points.move(0, 405)
        self.btn_data_about_points.clicked.connect(self.data_about_points)

        self.btn_scanned_users_by_months = QPushButton("Кол-во сканировавших пользователей в текущем году по месяцам",
                                                       self)
        self.btn_scanned_users_by_months.move(0, 435)
        self.btn_scanned_users_by_months.clicked.connect(self.scanned_users_by_months)

        self.btn_data_about_scans_during_period = QPushButton("Кол-во пользователей и насканированных баллов за период",
                                                              self)
        self.btn_data_about_scans_during_period.move(0, 465)
        self.btn_data_about_scans_during_period.clicked.connect(self.data_about_scans_during_period)

        self.btn_top_users_by_scans = QPushButton("ТОП пользователей по сканам в текущем году на данный момент", self)
        self.btn_top_users_by_scans.move(0, 495)
        self.btn_top_users_by_scans.clicked.connect(self.top_users_by_scans)

        layout = QVBoxLayout()
        layout.addWidget(self.label)
        layout.addWidget(self.btn_about_users)
        layout.addWidget(self.btn_users_by_country)
        layout.addWidget(self.btn_last_authorization_in_app)
        layout.addWidget(self.btn_authorization_in_period)
        layout.addWidget(self.btn_points_by_users_and_countries)
        layout.addWidget(self.btn_about_scans)
        layout.addWidget(self.btn_data_about_scan_users_in_current_year)
        layout.addWidget(self.btn_data_about_points)
        layout.addWidget(self.btn_scanned_users_by_months)
        layout.addWidget(self.btn_data_about_scans_during_period)
        layout.addWidget(self.btn_top_users_by_scans)

        container = QWidget()
        container.setLayout(layout)

        self.setCentralWidget(container)

    def db_connection(self):
        """Connection to database"""
        try:
            self.db_pool = SimpleConnectionPool(
                minconn=5,
                maxconn=20,
                user = os.getenv('DB_USER'),
                password = os.getenv('DB_PASSWORD'),
                database = os.getenv('DB_NAME'),
                host = os.getenv('DB_HOST'),
                port = int(os.getenv('DB_PORT', 5432)),
            )
            print("Connection to database established successfully.")   #TODO delete after testing
        except Exception as e:
            print(f"Error connecting to database: {e}") #TODO delete after testing
            self.db_pool = None

    def execute_query(self, query, params=None):
        """Execute SQL query with automatic connection management"""
        if not self.db_pool:
            print("Database pool not intialized.") #TODO delete after testing
            return None, None
        
        conn = None

        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return results, columns
            else:
                conn.commit()
                return cursor.rowcount 
        except Exception as e:
            print(f"Error executing query: {e}") #TODO delete after testing
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                cursor.close()
                self.db_pool.putconn(conn)

    def query_to_dataframe(self, query, params=None):
        """Run query, load results into pandas DataFrame with query column names."""
        results, columns = self.execute_query(query, params)
        if not results or not columns:
            QMessageBox.warning(self, "Внимание!", "Dataframe is empty.")
            return pd.DataFrame()
        return pd.DataFrame(results, columns=columns)

    def open_dataframe_in_excel(self, df):
        """Open DataFrame in Excel using a temporary file."""

        if df is None or df.empty:
            QMessageBox.warning(self, "Внимание!", "DataFrane is empty.")
            return
        
        try:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp_path=tmp.name

            df.to_excel(tmp_path, index=False)

            if os.name == "nt":
                os.startfile(tmp_path)
            else:
                opener = "open" if sys.platform == "darwin" else "xdg-open"
                subprocess.Popen([opener, tmp_path])
        except Exception as e:
            QMessageBox.warning(self, "Внимание!", f"Не удалось открыть файл Excel: {e}")


    def cleaning_the_user_base_from_spam(self):        
        """Clean spam (exception empty row in "Страна" and 'Клиент' as spam) and test accounts in DataFrame"""

        exclude_users = ('kazah89', 'kazah1122', 'russia89', 'sanin', 'samoilov', 'axorindustry', 'kreknina', 'zeykin', 'berdnikova', 'ostashenko', 'bellaruss89@gmail.com', 'skalar', 'test',
                      'malyigor', 'ihormaly', 'axor', 'kosits')
        
        exclude_conditions = " AND ".join([f"u.email NOT LIKE '%{user}%'" for user in exclude_users])

        # Query with filters
        query = f"""
        SELECT * FROM users AS u
        JOIN countries с ON u.country_id = с.id
        JOIN user_type ut ON u.user_type_id = ut.id
        WHERE u.phone != ''
            AND u.phone IS NOT NULL
            AND ut.user_type != 'Клиент'
            AND {exclude_conditions}
        """

        df = self.query_to_dataframe(query)

        if df.empty:
            QMessageBox.warning(self, "Внимание!", "После очистки база пустая.")

        self.open_dataframe_in_excel(df)
        QMessageBox.information(self, "Информация", "База пользователей очищена от спама")

        print(df) #TODO delete after testing

    def users_by_country(self):
        """Formation general statistics about users by countries."""
        if df_users.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по пользователям.")
        else:
            list_for_df = []
            for country in countries:
                tmp_list = [country]
                data = df_users[(df_users["Страна"] == country) &
                                (df_users["Тип пользователя"] == "Дилер")]
                tmp_list.append(len(data["ID"]))

                data = df_users[(df_users["Страна"] == country) &
                                (df_users["Тип пользователя"] == "Монтажник")]
                tmp_list.append(len(data["ID"]))

                tmp_list.insert(1, sum(tmp_list[1:]))
                list_for_df.append(tmp_list)

            columns_for_df = ['Страна', 'Всего пользователей', 'Дилеров', 'Монтажников']
            index_for_df = range(len(list_for_df))
            total_stat_df = pd.DataFrame(list_for_df, index_for_df, columns_for_df).sort_values(
                by="Всего пользователей",
                ascending=False)

            total_stat_df.to_excel(f'{dir_for_output_data}/total_stats_about_users_for_{datetime.now().date()}.xlsx')
            subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
            # os.startfile(f'{dir_for_output_data}/total_stats_about_users_for_{datetime.now().date()}.xlsx') # вариант для запуса созданного файла с данными

    def last_authorization_in_app(self):
        """ Information about last authorisation users in app by years"""

        def amount_users_by_type(country_for_amount_users: str, user_type: str):
            """ Count amount users in country. """

            data = df_users[(df_users["Страна"] == country_for_amount_users) &
                            (df_users["Тип пользователя"] == user_type)]

            return len(data["ID"])

        def last_authorization(year: int, user_type: str, country_for_last_authorization: str):
            """Counting quantity of users with last authorisation in specific year."""

            if year == 0:
                data = df_users[
                    (df_users['Year'] == 0) &
                    (df_users['Тип пользователя'] == user_type) &
                    (df_users['Страна'] == country_for_last_authorization)]

                last = len(data["ID"])
            else:
                data = df_users[(df_users['Year'] == year) &
                                (df_users["Тип пользователя"] == user_type) &
                                (df_users["Страна"] == country_for_last_authorization)]

                last = len(data["ID"])

            return last

        if df_users.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по пользователям.")
        else:
            df_users['Year'] = df_users['Последняя авторизация в приложении'].dt.year
            df_users['Year'].fillna(0, inplace=True)
            years = sorted([year for year in set(df_users['Year']) if year != 0])
            last_authorization_in_app_list = []

            for country in countries:
                last_authorization_in_app_list.append([country, 'Дилеры', amount_users_by_type(country, 'Дилер')] +
                                                      [last_authorization(year, 'Дилер', country) for year in years] +
                                                      [last_authorization(0, 'Дилер', country)])

            last_authorization_in_app_list.append(['', '', '', '', '', '', '', '', ])

            for country in countries:
                last_authorization_in_app_list.append(
                    [country, 'Монтажники', amount_users_by_type(country, 'Монтажник')] +
                    [last_authorization(year, 'Монтажник', country) for year in years] +
                    [last_authorization(0, 'Монтажник', country)])

            columns = ['Страна', 'Тип пользователей', 'Всего в базе'] + [year for year in years] + ['Не авторизовались']
            index = [_ for _ in range(len(last_authorization_in_app_list))]
            last_authorization_in_app_df = pd.DataFrame(last_authorization_in_app_list, index, columns)

            last_authorization_in_app_df.to_excel(
                f'{dir_for_output_data}/last_authorization_in_app {datetime.now().date()}.xlsx')
            subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
            # os.startfile(f'{dir_for_output_data}/last_authorization_in_app {datetime.now().date()}.xlsx') # вариант для запуска созданного файла с данными

    def authorization_during_period(self):
        """ information about the amount of authorized users for the period """
        end_date = datetime(1900, 1, 1)

        def period_data(start_period_of_authorisation: datetime, end_period_of_authorization: datetime, user_type: str,
                        authorization_in_country: str):
            """ Counting the amount of users authorized in App during period """

            data = df_users[(df_users['Тип пользователя'] == user_type) &
                            (df_users['Страна'] == authorization_in_country) &
                            (df_users['Последняя авторизация в приложении'] >= start_period_of_authorisation) &
                            (df_users['Последняя авторизация в приложении'] <= end_period_of_authorization)]

            return len(data['ID'])

        if df_users.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по пользователям.")
        else:
            start_date, btn = QInputDialog.getText(self, "Начало периода",
                                                   "Укажите начало периода в формате mm.dd.yyyy (через точку): ")
            if btn:
                try:
                    start_date = datetime.strptime(start_date, '%d.%m.%Y')
                    end_date, btn = QInputDialog.getText(self, "Конец периода",
                                                         "Укажите конец периода в формате mm.dd.yyyy (через точку): ")
                    if btn:
                        try:
                            end_date = datetime.strptime(end_date, '%d.%m.%Y')
                        except ValueError:
                            QMessageBox.warning(self, "Внимание!", "Конечная дата введена неверно!")
                except TypeError or ValueError:
                    QMessageBox.warning(self, "Внимание!", "Начальная дата введена неверно!")
                    # TODO разобраться с исключениями
                    pass

                total_amount = 0
                authorization_during_period_list = []
                for country in countries:
                    amount_of_dealers = period_data(start_date, end_date, 'Дилер', country)
                    authorization_during_period_list.append([country, 'Дилеры', amount_of_dealers])
                    total_amount += amount_of_dealers
                    amount_of_adjusters = period_data(start_date, end_date, 'Монтажник', country)
                    authorization_during_period_list.append([country, 'Монтажники', amount_of_adjusters])
                    total_amount += amount_of_adjusters
                    authorization_during_period_list.append(['', '', ''])

                authorization_during_period_list.append(['Всего:', '', total_amount])

                columns = ['Страна', 'Тип пользователей', 'Авторизовалось пользователей']
                index = [_ for _ in range(len(authorization_during_period_list))]
                authorization_during_period_df = pd.DataFrame(authorization_during_period_list, index, columns)

                start = datetime.strftime(start_date, "%d-%m-%Y")
                end = datetime.strftime(end_date, "%d-%m-%Y")

                authorization_during_period_df.to_excel(
                    f'{dir_for_output_data}/authorization_during_period {start}-{end}.xlsx')
                subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
                # os.startfile(f'{dir_for_output_data}/authorization_during_period {start}-{end}.xlsx') # вариант для запуска созданного файла с данными

    def points_by_users_and_countries(self):
        """ Information about points by users and countries """

        def sum_of_points(type_of_user: str, country_for_sum_points: str):
            """ Count point of users by country"""

            data = df_users[(df_users['Тип пользователя'] == type_of_user) &
                            (df_users['Страна'] == country_for_sum_points)]

            return sum(data['Баллы'])

        if df_users.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по пользователям.")
        else:
            points_by_users_and_countries_list = []
            for country in countries:
                total_points = 0
                points = sum_of_points('Дилер', country)
                points_by_users_and_countries_list.append([country, 'Дилеры', points])
                total_points += points
                points = sum_of_points('Монтажник', country)
                points_by_users_and_countries_list.append([country, 'Монтажники', points])
                total_points += points
                points_by_users_and_countries_list.append(['Всего баллов:', '', total_points])
                points_by_users_and_countries_list.append(['', '', ''])

            columns = ['Страна', 'Тип пользователей', 'Сумма баллов']
            index = [_ for _ in range(len(points_by_users_and_countries_list))]
            points_by_users_and_countries_df = pd.DataFrame(points_by_users_and_countries_list, index, columns)

            points_by_users_and_countries_df.to_excel(
                f"{dir_for_output_data}/points_by_users_and_countries {datetime.now().date()}.xlsx")
            subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
            # os.startfile(f'{dir_for_output_data}/points_by_users_and_countries {datetime.now().date()}.xlsx') # вариант для запуска созданного файла с данными

    def data_about_scan_users_in_current_year(self):
        """ Information about scanned users in current year """
        global countries

        def scanned_users(country_for_scanned_users: str, user_type: str, himself=True):
            """ Count amount of users scanned in current year"""

            count = set()

            if himself:
                if user_type == 'Дилер':
                    data = df_scans[(df_scans['Страна'] == country_for_scanned_users) &
                                    (df_scans['Сам себе'] == user_type) &
                                    (df_scans['Монтажник.1'] == '')]

                    count = set(data['UF_USER_ID'])

                elif user_type == 'Монтажник':
                    data = df_scans[(df_scans['Страна'] == country_for_scanned_users) &
                                    (df_scans['Сам себе'] == 'Монтажник')]

                    count = set(data['UF_USER_ID'])

            else:
                data = df_scans[(df_scans['Страна'] == country_for_scanned_users) &
                                (df_scans['Монтажник.1'] == 'Монтажник')]

                count = set(data['Монтажник'])

            return len(count)

        if df_scans.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по сканам.")
        else:
            countries = list(set(df_scans["Страна"]))  # list of countries in DataFrame
            table_about_scan_users_in_year_list = []
            for country in countries:
                dealers_himself = scanned_users(country, 'Дилер')
                adjusters_himself = scanned_users(country, 'Монтажник')
                adjusters_for_dealers = scanned_users(country, 'Монтажник', False)
                table_about_scan_users_in_year_list.append([country, 'Дилеры', 'Сами себе', dealers_himself])
                table_about_scan_users_in_year_list.append(['', 'Монтажники', 'Сами себе', adjusters_himself])
                table_about_scan_users_in_year_list.append(
                    ['', 'Монтажники', 'Сканировали дилеру', adjusters_for_dealers])
                table_about_scan_users_in_year_list.append(['', '', 'Итого:',
                                                            dealers_himself +
                                                            adjusters_himself +
                                                            adjusters_for_dealers])
                table_about_scan_users_in_year_list.append(['', '', '', ''])

            columns = ['Страна', 'Тип пользователей', 'Сканировали', 'Кол-во пользователей']
            index = [_ for _ in range(len(table_about_scan_users_in_year_list))]
            table_about_scan_users_in_year_df = pd.DataFrame(table_about_scan_users_in_year_list, index, columns)

            table_about_scan_users_in_year_df.to_excel(
                f'{dir_for_output_data}/scanned_users_in_year {datetime.now().date()}.xlsx')
            subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
            # os.startfile(f'{dir_for_output_data}/scanned_users_in_year {datetime.now().date()}.xlsx') # вариант для запуска созданного файла с данными

    def data_about_points(self):
        """ Information about sum of points scanned in current year """
        global countries

        def total_amount_of_points_for_year(country_for_points, user_type):
            """Count the sum of points scanned in current year"""

            amount_of_points = 0

            if user_type == 'Дилер':
                data = df_scans[(df_scans['Страна'] == country_for_points) &
                                (df_scans['Сам себе'] == user_type)]

                amount_of_points = sum(data['UF_POINTS'])

            elif user_type == 'Монтажник':
                data = df_scans[(df_scans['Страна'] == country_for_points) &
                                (df_scans['Сам себе'] == user_type)]

                amount_of_points = sum(data['UF_POINTS'])

                data = df_scans[(df_scans['Страна'] == country_for_points) &
                                (df_scans['Монтажник.1'] == 'Монтажник')]

                amount_of_points += sum(data['UF_POINTS'])

            return amount_of_points

        if df_scans.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по сканам.")
        else:
            countries = list(set(df_scans["Страна"]))  # list of countries in DataFrame
            """ Output data about points """
            data_about_points_lst = []
            for country in countries:
                point_of_dealers = total_amount_of_points_for_year(country, 'Дилер')
                points_of_adjusters = total_amount_of_points_for_year(country, 'Монтажник')
                data_about_points_lst.append([country, 'Дилеры', point_of_dealers])
                data_about_points_lst.append(['', 'Монтажники', points_of_adjusters])
                data_about_points_lst.append(['', 'Итого:', point_of_dealers + points_of_adjusters])
                data_about_points_lst.append(['', '', ''])

            columns = ['Страна', 'Тип пользователей', 'Насканировано баллов']
            index = [_ for _ in range(len(data_about_points_lst))]
            data_about_points_df = pd.DataFrame(data_about_points_lst, index, columns)

            data_about_points_df.to_excel(
                f'{dir_for_output_data}/all_points_of_users_by_country {datetime.now().date()}.xlsx')
            subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
            # os.startfile(f'{dir_for_output_data}/all_points_of_users_by_country {datetime.now().date()}.xlsx') # вариант для запуска созданного файла с данными

    def scanned_users_by_months(self):
        """ Information about scanned users by country in each month """
        global countries

        if df_scans.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по сканам.")
        else:
            months = {1: 'Январь',
                      2: 'Февраль',
                      3: 'Март',
                      4: 'Апрель',
                      5: 'Май',
                      6: 'Июнь',
                      7: 'Июль',
                      8: 'Август',
                      9: 'Сентябрь',
                      10: 'Октябрь',
                      11: 'Ноябрь',
                      12: 'Декабрь'}
            countries = list(set(df_scans["Страна"]))  # list of countries in DataFrame
            df_scans['Месяц'] = df_scans['UF_CREATED_AT'].dt.month.map(months)

            scanned_users_by_months_list = []
            columns = ['Страна', 'Тип пользователей', 'Сканировали'] + [month for month in months.values()]

            for country in countries:
                dealers_himself = [country, 'Дилеры', 'Сами себе']
                adjusters_himself = ['', 'Монтажники', 'Сами себе']
                adjusters_for_dealers = ['', 'Монтажники', 'Сканировали дилеру']
                total_users_in_country = ['', '', 'Итого:']

                for month in months.values():
                    data = df_scans[(df_scans['Страна'] == country) &
                                    (df_scans['Месяц'] == month) &
                                    (df_scans['Сам себе'] == 'Дилер') &
                                    (df_scans['Монтажник.1'] == '')]

                    d_h = len(set(data['UF_USER_ID']))  # dealers himself
                    dealers_himself.append(d_h)

                    data = df_scans[(df_scans['Месяц'] == month) &
                                    (df_scans['Страна'] == country) &
                                    (df_scans['Сам себе'] == 'Монтажник')]

                    a_h = len(set(data['UF_USER_ID']))  # adjusters himself
                    adjusters_himself.append(a_h)

                    data = df_scans[(df_scans['Месяц'] == month) &
                                    (df_scans['Страна'] == country) &
                                    (df_scans['Монтажник.1'] == 'Монтажник')]

                    a_d = len(set(data['Монтажник']))  # adjusters for dealers
                    adjusters_for_dealers.append(a_d)

                    total_users_in_country.append(d_h + a_h + a_d)

                scanned_users_by_months_list.append(dealers_himself)
                scanned_users_by_months_list.append(adjusters_himself)
                scanned_users_by_months_list.append(adjusters_for_dealers)
                scanned_users_by_months_list.append(total_users_in_country)
                scanned_users_by_months_list.append(['', '', ''])

            index = [_ for _ in range(len(scanned_users_by_months_list))]
            scanned_users_by_months_df = pd.DataFrame(scanned_users_by_months_list, index, columns)

            scanned_users_by_months_df.to_excel(
                f'{dir_for_output_data}/scanned_users_by_months {datetime.now().date()}.xlsx')
            subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
            # os.startfile(f'{dir_for_output_data}/scanned_users_by_months {datetime.now().date()}.xlsx') # вариант для запуска созданного файла с данными

    def data_about_scans_during_period(self):
        """Output information about users and scans during period"""

        def scanned_users_per_period(country_for_scanned_users_in_period: str, user_type: str,
                                     start_period_of_scanned_users: datetime,
                                     end_period_of_scanned_users: datetime, himself=True):
            """ Count amount of users scanned during period"""

            count = set()
            if himself:
                if user_type == 'Дилер':
                    data = df_scans[(df_scans['UF_CREATED_AT'] >= start_period_of_scanned_users) &
                                    (df_scans['UF_CREATED_AT'] <= end_period_of_scanned_users) &
                                    (df_scans['Страна'] == country_for_scanned_users_in_period) &
                                    (df_scans['Сам себе'] == user_type) &
                                    (df_scans['Монтажник.1'] != 'Монтажник')]

                    count = set(data['UF_USER_ID'])

                elif user_type == 'Монтажник':
                    data = df_scans[(df_scans['UF_CREATED_AT'] >= start_period_of_scanned_users) &
                                    (df_scans['UF_CREATED_AT'] <= end_period_of_scanned_users) &
                                    (df_scans['Страна'] == country_for_scanned_users_in_period) &
                                    (df_scans['Сам себе'] == user_type)]

                    count = set(data['UF_USER_ID'])
            else:
                data = df_scans[(df_scans['UF_CREATED_AT'] >= start_period_of_scanned_users) &
                                (df_scans['UF_CREATED_AT'] <= end_period_of_scanned_users) &
                                (df_scans['Страна'] == country_for_scanned_users_in_period) &
                                (df_scans['Монтажник.1'] == 'Монтажник')]

                count = set(data['Монтажник'])

            return len(count)

        def sum_of_points_per_period(country_for_sum_points_in_period: str, user_type: str,
                                     start_period_for_sum_points: datetime, end_period_of_sum_points: datetime,
                                     himself=True):
            """ Count sum of scanned points during period """

            if himself:
                if user_type == 'Дилер':
                    data = df_scans[(df_scans['UF_CREATED_AT'] >= start_period_for_sum_points) &
                                    (df_scans['UF_CREATED_AT'] <= end_period_of_sum_points) &
                                    (df_scans['Страна'] == country_for_sum_points_in_period) &
                                    (df_scans['Сам себе'] == user_type) &
                                    (df_scans['Монтажник.1'] != 'Монтажник')]

                elif user_type == 'Монтажник':
                    data = df_scans[(df_scans['UF_CREATED_AT'] >= start_period_for_sum_points) &
                                    (df_scans['UF_CREATED_AT'] <= end_period_of_sum_points) &
                                    (df_scans['Страна'] == country_for_sum_points_in_period) &
                                    (df_scans['Сам себе'] == user_type)]
            else:
                data = df_scans[(df_scans['UF_CREATED_AT'] >= start_period_for_sum_points) &
                                (df_scans['UF_CREATED_AT'] <= end_period_of_sum_points) &
                                (df_scans['Страна'] == country_for_sum_points_in_period) &
                                (df_scans['Монтажник.1'] == 'Монтажник')]

            return sum(data['UF_POINTS'])

        start_date = datetime(1900, 1, 1)
        end_date = datetime(1900, 1, 1)

        if df_scans.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по сканам.")
        else:
            start_date, btn = QInputDialog.getText(self, "Начало периода",
                                                   "Укажите начало периода в формате mm.dd.yyyy (через точку): ")
            if btn:
                try:
                    start_date = datetime.strptime(start_date, '%d.%m.%Y')
                    end_date, btn = QInputDialog.getText(self, "Конец периода",
                                                         "Укажите конец периода в формате mm.dd.yyyy (через точку): ")
                    if btn:
                        try:
                            end_date = datetime.strptime(end_date, '%d.%m.%Y')
                        except ValueError:
                            QMessageBox.warning(self, "Внимание!", "Конечная дата введена неверно!")
                except TypeError or ValueError:
                    QMessageBox.warning(self, "Внимание!", "Начальная дата введена неверно!")
                    # TODO разобраться с исключениями
                    pass

        data_about_scans_during_period_list = []

        for country in countries:
            dealers_themselves = scanned_users_per_period(country, 'Дилер', start_date, end_date)
            adjusters_themselves = scanned_users_per_period(country, 'Монтажник', start_date, end_date)
            adjusters_for_dealers = scanned_users_per_period(country, 'Монтажник', start_date, end_date, False)
            points_of_dealers_themselves = sum_of_points_per_period(country, 'Дилер', start_date, end_date)
            points_of_adjusters_themselves = sum_of_points_per_period(country, 'Монтажник', start_date, end_date)
            points_of_adjusters_for_dealers = sum_of_points_per_period(country, 'Монтажник', start_date, end_date,
                                                                       False)

            data_about_scans_during_period_list.append([country, 'Дилеры', 'Сами себе', dealers_themselves,
                                                        points_of_dealers_themselves + points_of_adjusters_for_dealers])
            data_about_scans_during_period_list.append(['', 'Монтажники', 'Сами себе', adjusters_themselves,
                                                        points_of_adjusters_themselves])
            data_about_scans_during_period_list.append(['', 'Монтажники', 'Сканировали дилеру', adjusters_for_dealers,
                                                        points_of_adjusters_for_dealers])
            data_about_scans_during_period_list.append(['', '', 'Итого:', dealers_themselves + adjusters_themselves +
                                                        adjusters_for_dealers,
                                                        points_of_dealers_themselves + points_of_adjusters_for_dealers +
                                                        points_of_adjusters_themselves +
                                                        points_of_adjusters_for_dealers])
            data_about_scans_during_period_list.append(['', '', '', '', ''])

        columns = ['Страна',
                   'Пользователи',
                   'Сканировали:',
                   'Кол-во пользователей',
                   'Баллов за указанный период']
        index = [_ for _ in range(len(data_about_scans_during_period_list))]
        data_about_scans_during_period_df = pd.DataFrame(data_about_scans_during_period_list, index, columns)

        start = datetime.strftime(start_date, "%d-%m-%Y")
        end = datetime.strftime(end_date, "%d-%m-%Y")

        data_about_scans_during_period_df.to_excel(
            f'{dir_for_output_data}/data_about_scans_during_period_{start}-{end}.xlsx')
        subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
        # os.startfile(f'{dir_for_output_data}/data_about_scans_during_period_{start}-{end}.xlsx') # вариант для запуска созданного файла с данными

    def top_users_by_scans(self):
        """ TOP dealers / adjusters by scans"""
        global countries

        if df_users.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по пользователям.")
        elif df_scans.empty:
            QMessageBox.warning(self, "Внимание!", "Загрузите данные по сканам.")
        else:
            countries = list(set(df_scans["Страна"]))  # list of countries in DataFrame
            users = ['Дилер', 'Монтажник']
            country = QInputDialog.getItem(self, "Страны", "Выберите страну...", countries, editable=False)
            if country[1]:
                user_type = QInputDialog.getItem(self, "Пользователи", "Выберите тип пользователей...", users,
                                                 editable=False)

                top_users = {}  # dictionary for TOP users by points
                top_users_by_scans_lst = []
                surname = {}  # dictionary for surnames

                """ Filling the dictionary of surnames"""
                for df_users_ID, df_users_surname in zip(df_users['ID'], df_users['Фамилия']):
                    surname[df_users_ID] = df_users_surname

                if user_type[0] == 'Дилер':
                    data = df_scans[(df_scans['Страна'] == country[0]) &
                                    (df_scans['Сам себе'] == user_type[0])]

                    for df_scans_dealer_id, df_scans_points in zip(data['UF_USER_ID'], data['UF_POINTS']):
                        if df_scans_dealer_id in top_users.keys():
                            top_users[df_scans_dealer_id] += df_scans_points
                        else:
                            top_users[df_scans_dealer_id] = df_scans_points

                    for df_scans_dealer_id in top_users.keys():
                        if df_scans_dealer_id in surname.keys():  # users with empty "Страна" don't count in df_users
                            top_users_by_scans_lst.append([df_scans_dealer_id,
                                                           surname[df_scans_dealer_id],
                                                           top_users[df_scans_dealer_id]])

                    top_users_by_scans_lst = sorted(top_users_by_scans_lst, key=lambda x: x[2], reverse=True)

                    top_users_by_scans_lst.append(['Итого:', '', sum(top_users.values())])

                    columns = ['ID пользователя', 'Фамилия', 'Сумма насканированных баллов']
                    index = [_ for _ in range(len(top_users_by_scans_lst))]
                    top_users_by_scans_list_df = pd.DataFrame(top_users_by_scans_lst, index, columns)

                    top_users_by_scans_list_df.to_excel(
                        f'{dir_for_output_data}/TOP_dealers_by_scans_in_{country[0]} {datetime.now().date()}.xlsx')
                    subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
                    # os.startfile(f'{dir_for_output_data}/TOP_dealers_by_scans_in_{country[0]} {datetime.now().date()}.xlsx') # вариант для запуска созданного файла с данными
                elif user_type[0] == 'Монтажник':
                    data = df_scans[(df_scans['Страна'] == country[0]) &
                                    (df_scans['Сам себе'] == user_type[0])]

                    for df_scans_adjuster_id, df_scans_point in zip(data['UF_USER_ID'], data['UF_POINTS']):
                        if df_scans_adjuster_id in top_users.keys():
                            top_users[df_scans_adjuster_id] += df_scans_point
                        else:
                            top_users[df_scans_adjuster_id] = df_scans_point

                    data = df_scans[(df_scans['Страна'] == country[0]) &
                                    (df_scans['Монтажник.1'] == user_type[0])]

                    for df_scans_adjuster_id, df_scans_point in zip(data['Монтажник'], data['UF_POINTS']):
                        if df_scans_adjuster_id in top_users.keys():
                            top_users[df_scans_adjuster_id] += df_scans_point
                        else:
                            top_users[df_scans_adjuster_id] = df_scans_point

                    for df_scans_adjuster_id in top_users.keys():
                        if df_scans_adjuster_id in surname.keys():  # users with empty "Страна" don't count in df_users
                            top_users_by_scans_lst.append([df_scans_adjuster_id,
                                                           surname[df_scans_adjuster_id],
                                                           top_users[df_scans_adjuster_id]])

                    top_users_by_scans_lst = sorted(top_users_by_scans_lst, key=lambda x: x[2], reverse=True)

                    top_users_by_scans_lst.append(['Итого:', '', sum(top_users.values())])

                    columns = ['ID пользователя', 'Фамилия', 'Сумма насканированных баллов']
                    index = [_ for _ in range(len(top_users_by_scans_lst))]
                    top_users_by_scans_list_df = pd.DataFrame(top_users_by_scans_lst, index, columns)

                    top_users_by_scans_list_df.to_excel(
                        f'{dir_for_output_data}/TOP_adjusters_by_scans_in_{country[0]} {datetime.now().date()}.xlsx')
                    subprocess.Popen(f'explorer /select,{dir_for_output_data},')  # вариант для открытия папки с данными
                    # os.startfile(f'{dir_for_output_data}/TOP_adjusters_by_scans_in_{country[0]} {datetime.now().date()}.xlsx') # вариант для запуска созданного файла с данными

    def close_db_connection(self):
        """ Closing connection to database """
        if self.db_pool:
            self.db_pool.closeall()
            print("Connection to database closed.") #TODO удалить или заменить на логирование

    def closeEvent(self, event):
        """Handle window close event"""
        self.close_db_connection()
        print("Database connection terminated.") #TODO delete after testing
        event.accept()
    