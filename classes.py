import pathlib

from PyQt5.QtWidgets import QMainWindow, QLabel, QPushButton, QMessageBox, QFileDialog, QInputDialog, QWidget, \
    QVBoxLayout
from PyQt5.QtGui import QIcon, QPixmap, QFont
from datetime import datetime
import pandas as pd
from pathlib import Path
import pathlib
import os
import subprocess

df_users = pd.DataFrame
countries = set()
df_scans = pd.DataFrame

try:
    Path.mkdir(pathlib.Path.cwd() / 'Данные по пользователям и сканам')
except FileExistsError:
    pass

dir_for_output_data = Path(pathlib.Path.cwd(), 'Данные по пользователям и сканам')


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.resize(620, 600)
        self.setWindowTitle("Данные по пользователя и сканам в приложении AXOR")
        self.setWindowIcon(QIcon('Pictures/axor.ico'))

        self.label = QLabel()
        self.label.setPixmap(QPixmap('Pictures/axor_logo.png'))

        self.btn_about_users = QPushButton("Загрузить базу пользователей", self)
        self.btn_about_users.move(0, 175)
        self.btn_about_users.setFont(QFont('Font/pfdintextpro-thinitalic.ttf', 14, 50, False))
        self.btn_about_users.clicked.connect(self.check_file_with_users)

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
        self.btn_about_scans.clicked.connect(self.check_file_with_scans)

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

    def check_file_with_users(self):
        """Loading and check file about users and the availability necessary columns in file about users"""
        global df_users
        global countries

        QMessageBox.information(self, "Важно!", "Перед загрузкой убедитесь, что файл с данными в формате .XLSX")

        file_with_users = QFileDialog.getOpenFileName(self, 'Open file', f'{Path.home() / "Desktop"}', '*.xlsx')

        if file_with_users[0] == '':
            pass
        else:
            """Columns for check data about users"""
            df_users_columns = ['ID',
                                'Баллы',
                                'Последняя авторизация в приложении',
                                'Город работы',
                                'Страна',
                                'Тип пользователя',
                                'Фамилия',
                                'Имя',
                                'Отчество',
                                'E-Mail']

            data_about_users = pd.read_excel(file_with_users[0],
                                             na_values="NA",
                                             converters={"ID": int, "Баллы": int})

            for col_name in df_users_columns:
                if col_name not in data_about_users.columns:
                    QMessageBox.warning(self, "Внимание!", f"В загруженных данных не хватает столбца {col_name}")
                    return False
                else:
                    data_about_users = data_about_users[['ID',
                                                         'Баллы',
                                                         'Последняя авторизация в приложении',
                                                         'Город работы',
                                                         'Страна',
                                                         'Тип пользователя',
                                                         'Фамилия',
                                                         'Имя',
                                                         'Отчество',
                                                         'E-Mail']]

            data_about_users['Последняя авторизация в приложении'] = pd.to_datetime(
                data_about_users['Последняя авторизация в приложении'],
                format='%d.%m.%Y %H:%M:%S').dt.normalize()

            data_about_users['Баллы'].fillna(0, inplace=True)
            data_about_users.fillna('', inplace=True)

            """Clean spam (exception empty row in "Страна" and 'Клиент' as spam) and test accounts in DataFrame"""
            data_about_users = data_about_users[(data_about_users['Страна'] != '') &
                                                (data_about_users['Тип пользователя'] != 'Клиент')]

            """List of test accounts, excludes from counting"""
            exclude_users = ['kazah89', 'kazah1122', 'russia89', 'sanin, ''samoilov', 'axorindustry', 'kreknina',
                             'zeykin', 'berdnikova', 'ostashenko', 'bellaruss89@gmail.com', 'skalar', 'test',
                             'malyigor', 'ihormaly', 'axor', 'kosits']

            """Creating list of excluded accounts"""
            exclude_list = set()
            for email in data_about_users['E-Mail']:
                for i in exclude_users:
                    if i in email:
                        exclude_list.add(email)

            """Clean DataFrame from exclude accounts"""
            data_about_users = data_about_users.loc[~data_about_users['E-Mail'].isin(exclude_list)]
            df_users = data_about_users
            countries = list(set(df_users["Страна"]))  # list of countries in DataFrame

            QMessageBox.information(self, "Информация", "Данные по пользователям загружены.")

    def check_file_with_scans(self):
        """Check the availability necessary columns in file about scans"""
        global df_scans

        QMessageBox.information(self, "Важно!", "Перед загрузкой убедитесь, что файл с данными в формате .XLSX")

        file_with_scans = QFileDialog.getOpenFileName(self, 'Open file', f'{Path.home() / "Desktop"}', '*.xlsx')

        if file_with_scans[0] == '':
            pass
        else:
            """Columns for check data about scans"""
            df_scans_columns = ['UF_TYPE',
                                'UF_POINTS',
                                'Дилер+Монтажник',
                                'UF_USER_ID',
                                'Монтажник',
                                'UF_CREATED_AT',
                                'Страна',
                                'Сам себе',
                                'Монтажник.1']
            data_about_scans = pd.read_excel(file_with_scans[0],
                                             converters={"UF_POINTS": int, "UF_USER_ID": int, "Монтажник": int})

            for col_name in df_scans_columns:
                if col_name not in data_about_scans.columns:
                    QMessageBox.warning(self, "Внимание!", f"В загруженных данных не хватает столбца {col_name}")
                    return False
                else:
                    data_about_scans = data_about_scans[['UF_TYPE',
                                                         'UF_POINTS',
                                                         'Дилер+Монтажник',
                                                         'UF_USER_ID',
                                                         'Монтажник',
                                                         'UF_CREATED_AT',
                                                         'Страна',
                                                         'Сам себе',
                                                         'Монтажник.1']]

            data_about_scans['UF_CREATED_AT'] = pd.to_datetime(data_about_scans['UF_CREATED_AT'],
                                                               format='%d.%m.%Y %H:%M:%S').dt.normalize()
            data_about_scans = data_about_scans.fillna('')
            df_scans = data_about_scans

            QMessageBox.information(self, "Информация", "Данные по сканам загружены.")

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
