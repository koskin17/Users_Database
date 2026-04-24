import os, tempfile, subprocess, sys
import gc

import pandas as pd
from datetime import datetime

from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
from typing import Optional

from PyQt5.QtWidgets import QMainWindow, QLabel, QPushButton, QMessageBox, QInputDialog, QWidget, QVBoxLayout
from PyQt5.QtGui import QIcon, QPixmap, QFont

from decorators import for_data_about_users, for_data_about_scans

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

        self.btn_about_users = QPushButton("All users", self)
        self.btn_about_users.setFont(QFont('Font/pfdintextpro-thinitalic.ttf', 14, 50, False))
        self.btn_about_users.clicked.connect(self.all_users)

        self.btn_users_by_country = QPushButton("Users by country", self)
        self.btn_users_by_country.clicked.connect(self.users_by_country)

        self.btn_last_authorization_in_app = QPushButton("Last user's authorization in the application", self)
        self.btn_last_authorization_in_app.clicked.connect(self.last_authorization_in_app)

        self.btn_authorization_in_period = QPushButton("User authorization for the period", self)
        self.btn_authorization_in_period.clicked.connect(self.authorization_during_period)

        self.btn_points_by_users_and_countries = QPushButton("Current number of points of users", self)
        self.btn_points_by_users_and_countries.clicked.connect(self.points_by_users_and_countries)

        self.btn_about_scans = QPushButton("All scans", self)
        self.btn_about_scans.setFont(QFont('Font/pfdintextpro-thinitalic.ttf', 14, 50, False))
        self.btn_about_scans.clicked.connect(self.all_scans)

        self.btn_data_about_scan_users_in_current_year = QPushButton(
            "Scanned users by year", self)
        self.btn_data_about_scan_users_in_current_year.clicked.connect(self.scanned_users_by_year)

        self.btn_data_about_scans_during_period = QPushButton("Кол-во пользователей и насканированных баллов за период",
                                                              self)
        self.btn_data_about_scans_during_period.clicked.connect(self.data_about_scans_during_period)

        self.btn_top_users_by_scans = QPushButton("TOP users by total points", self)
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
            QMessageBox.information(self, "Information", "Connection to the database has been established.")
        except Exception as e:
            QMessageBox.warning(self, "Attention!", f"Error connecting to database: {e}")
            self.db_pool = None

    def execute_query(self, query, params=None):
        """Execute SQL query with automatic connection management"""

        if not self.db_pool:
            QMessageBox.warning(self, "Attention!", "Database pool not initialized.")
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
                return cursor.rowcount, None
        except Exception as e:
            QMessageBox.warning(self, "Attention!", f"Error executing query: {e}")
            if conn:
                conn.rollback()
            return None, None
        finally:
            if conn:
                cursor.close()
                self.db_pool.putconn(conn)

    def query_to_dataframe(self, query, params=None):
        """Run query, load results into pandas DataFrame with query column names."""

        results, columns = self.execute_query(query, params) # type: ignore

        if not results or not columns:
            QMessageBox.information(self, "Information", "DataFrame is empty.")
            return pd.DataFrame()
        return pd.DataFrame(results, columns=columns)

    def open_dataframe_in_excel(self, df):
        """Open DataFrame in Excel using a temporary file."""

        if df is None or df.empty:
            QMessageBox.warning(self, "Attention!", "DataFrame is empty.")
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
            QMessageBox.warning(self, "Attention!", f"Unable to open Excel file: {e}")

    def load_and_clean_users(self):        
        """Clean spam and test accounts in DataFrame"""

        exclude_users = ('kazah89', 'kazah1122', 'russia89', 'sanin', 'samoilov', 'axorindustry', 'kreknina', 'zeykin', 'berdnikova', 'ostashenko', 'bellaruss89@gmail.com', 'skalar', 'test',
                      'malyigor', 'ihormaly', 'axor', 'kosits')
        
        exclude_conditions = " AND ".join([f"u.email NOT LIKE '%{user}%'" for user in exclude_users])

        query = f"""
        SELECT u.id AS user_id,
            u.points,
            u.sessions_count,
            u.login_email,
            u.email,
            u.first_name,
            u.last_name,
            u.phone,
            u.last_login,
            u.last_authorization,
            u.registration_date,
            c.country_name,
            ut.user_type,
            spk_name
        FROM users AS u
        JOIN countries AS c ON u.country_id = c.id
        JOIN user_type AS ut ON u.user_type_id = ut.id
        LEFT JOIN spk AS spk ON u.spk_id = spk.id
        WHERE u.phone IS NOT NULL
          AND u.phone <> ''
          AND ut.user_type <> 'Клиент'
          AND {exclude_conditions}
        """

        df = self.query_to_dataframe(query)

        self.df_users = df
        return df
    
    def load_data_about_scans(self):
        """Loading data about scans by all users"""

        query = """
        SELECT
            sh.id,
            sh.user_id,
            c.country_name AS country_of_user,
            ut.user_type AS user_type_for_user,
            sh.installer_id,
            installer_country.country_name AS country_of_installer,
            installer_ut.user_type AS user_type_for_installer,
            pr.product,
            sh.points,
            sh.qr_code,
            sh.created_at,
            companies.company_name
        FROM scan_history AS sh
        JOIN users AS u ON sh.user_id = u.id
        JOIN products AS pr ON sh.product_id = pr.id
        JOIN countries AS c ON u.country_id = c.id
        JOIN user_type AS ut ON u.user_type_id = ut.id
        JOIN companies ON sh.company_id = companies.id
        LEFT JOIN users AS installer ON sh.installer_id = installer.id
        LEFT JOIN user_type AS installer_ut ON installer.user_type_id = installer_ut.id
        LEFT JOIN countries AS installer_country ON installer.country_id = installer_country.id
        """

        df = self.query_to_dataframe(query)
        self.df_scans = df
        return df

    @for_data_about_users
    def all_users(self, df):
        """Getting information about all users in the database"""
        self.open_dataframe_in_excel(df)

    @for_data_about_users
    def users_by_country(self, df):
        """General statistics about users by countries."""

        users_by_countries = df.groupby(["country_name", "user_type"]).size().reset_index(name='count')
        self.open_dataframe_in_excel(users_by_countries)

    @for_data_about_users
    def last_authorization_in_app(self, df):
        """Quantity of authorized users by years with group by country and type of user"""
        
        df["Year"] = df["last_authorization"].dt.year.fillna(0).astype(int)

        df_grouped = (
            df[df["Year"] != 0].groupby(["country_name", "user_type", "Year"]).size().reset_index(name="user_count")
        )

        pivot_df = df_grouped.pivot_table(
            index = ["country_name", "user_type"],
            columns = "Year",
            values = "user_count",
            fill_value = 0
        ).reset_index()

        self.open_dataframe_in_excel(pivot_df)
        QMessageBox.information(self, "Information.", "Data on the number of authorized users has been generated.")

    @for_data_about_users
    def authorization_during_period(self, df):
        """ Information about the amount of authorized users for the period """

        start_date_str, ok = QInputDialog.getText(self, "Beginning of the period:", "Specify the beginning of the period in the format dd.mm.yyyy (separated by a dot):")

        if not ok or not start_date_str:
            return

        try:
            start_date = datetime.strptime(start_date_str, "%d.%m.%Y")
            print(start_date)  # TODO delete after testing
            print("Date correct")  # TODO delete after testing
        except ValueError:
            QMessageBox.warning(self, "Attention!", "The entered date of the beginning of period is incorrect!")
            return
        
        end_date_str, ok = QInputDialog.getText(self, "End of a period:", "Specify the end of the period in the format dd.mm.yyyy (separated by a dot):")

        if not ok or not end_date_str:
            return        
        
        try:    
            end_date = datetime.strptime(end_date_str, "%d.%m.%Y")
            print(end_date)   # TODO delete after testing
            print("Date correct")  # TODO delete after testing
        except ValueError:
            QMessageBox.warning(self, "Attention!", "The entered date of the end of period is incorrect!")
            return

        mask_for_filter = (df["last_authorization"] >= start_date) & (df["last_authorization"] <= end_date)
        df_period = df[mask_for_filter]

        grouped = (df_period.groupby(["country_name", "user_type"]).size().reset_index(name="authorized_count"))

        total = grouped["authorized_count"].sum()
        grouped = pd.concat([grouped, pd.DataFrame([["Total", "", total]], columns = grouped.columns)])

        self.open_dataframe_in_excel(grouped)
        QMessageBox.information(self, "Information.", "Information on the number of authorized users for the period has been generated.")

    @for_data_about_users
    def points_by_users_and_countries(self, df):
        """ Information about current sum of points by users and countries """
    
        grouped = (df.groupby(["country_name", "user_type"])["points"].sum().reset_index(name="sum_points"))

        total_points = grouped["sum_points"].sum()
        total_row = pd.DataFrame([["Total:", "", total_points]], columns = grouped.columns)
        grouped = pd.concat([grouped, total_row], ignore_index=True)

        self.open_dataframe_in_excel(grouped)
        QMessageBox.information(self, "Information", "Data about the current sum of points by type of users and countries has been generated.")

    @for_data_about_scans
    def all_scans(self, df):
        self.open_dataframe_in_excel(df)

    def scanned_users_by_year(self):
        """Information about the number of scanning users by year, country, and user type"""

        query_scanned_users_by_year = """
            SELECT
                combined.country_name,
                combined.user_type,
                EXTRACT (YEAR FROM combined.created_at) AS year,
                COUNT (DISTINCT combined.user_identifier) AS user_count
            FROM (
                
                -- Dealers scanned for himself
                SELECT
                    sh.user_id AS user_identifier,
                    c.country_name,
                    ut.user_type,
                    sh.created_at
                FROM scan_history AS sh
                JOIN users AS u ON sh.user_id = u.id
                JOIN countries AS c ON u.country_id = c.id
                JOIN user_type AS ut ON u.user_type_id = ut.id
                WHERE sh.installer_id IS NULL AND ut.id = 1
                
                UNION

                -- Installer scanned for himself
                SELECT
                    sh.user_id AS user_identifier,
                    c.country_name,
                    ut.user_type,
                    sh.created_at
                FROM scan_history AS sh
                JOIN users AS u ON sh.user_id = u.id
                JOIN countries AS c ON u.country_id = c.id
                JOIN user_type AS ut ON u.user_type_id = ut.id
                WHERE sh.installer_id IS NULL AND ut.id = 2

                UNION

                -- Installer scanned for dealers
                SELECT
                    sh.installer_id AS user_identifier,
                    installer_country.country_name,
                    installer_ut.user_type,
                    sh.created_at
                FROM scan_history AS sh
                JOIN users AS installer ON sh.installer_id = installer.id
                JOIN countries AS installer_country ON installer.country_id = installer_country.id
                JOIN user_type AS installer_ut ON installer.user_type_id = installer_ut.id
                WHERE sh.installer_id IS NOT NULL AND installer_ut.id = 2
            ) AS combined
            GROUP BY combined.country_name, combined.user_type, year
            ORDER BY combined.country_name, combined.user_type, year;
        """
        df_scanned_users_by_year = self.query_to_dataframe(query_scanned_users_by_year)

        if df_scanned_users_by_year is None or df_scanned_users_by_year.empty:
            QMessageBox.warning(self, "Attention!", "No scan data is available.")
            return

        df_scanned_users_by_year_pivot_df = (
            df_scanned_users_by_year.pivot_table(
                index=["country_name", "user_type"],
                columns="year",
                values="user_count",
                fill_value=0
            ).reset_index()
        )

        self.open_dataframe_in_excel(df_scanned_users_by_year_pivot_df)

        QMessageBox.information(self, "Information", "Statistics on scanning users by year have been compiled.")

        del df_scanned_users_by_year, df_scanned_users_by_year_pivot_df
        gc.collect()
        print("All dataFrame are deleted") #TODO delete after cleaning

    @for_data_about_scans
    def data_about_scans_during_period(self, df):
        """Data about number of users and scans during period"""

        start_date_str, ok = QInputDialog.getText(self, "Period start", "Enter the period start in dd.mm.yyyy (separated by dot):")

        if not ok or not start_date_str:
            return
        
        try:
            start_date = datetime.strptime(start_date_str, "%d.%m.%Y")
        except ValueError:
            QMessageBox.warning(self, "Attention!", "The entered date of the beginning of period is incorrect!")
            return
        
        end_date_str, ok = QInputDialog.getText(self, "End of period", "Enter the end of the period in dd.mm.yyyy (separated by dot):")

        if not ok or not end_date_str:
            return
        try:
            end_date = datetime.strptime(end_date_str, "%d.%m.%Y")
        except ValueError:
            QMessageBox.warning(self, "Attention!", "The end date of the period is entered incorrectly.")
            return
        
        mask_for_filter = (df["created_at"] >= start_date) & (df["created_at"] <= end_date)
        df_data_about_scans_during_period = df[mask_for_filter]

        self.open_dataframe_in_excel(df_data_about_scans_during_period)

    def top_users_by_scans(self):
        """ TOP dealers / adjusters by scans"""

        top_users_by_scans_query = """
            SELECT
                combined.user_id,
                combined.last_name,
                combined.user_type,
                combined.country,
                COUNT (*) AS scans_count,
                SUM (combined.points) AS total_points
            FROM (
                SELECT
                    sh.user_id,
                    sh.points,
                    ut.user_type,
                    u.last_name,
                    c.country_name AS country
                FROM scan_history AS sh

                -- Dealers and installer scanned for himself
                JOIN users AS u ON sh.user_id = u.id
                JOIN countries AS c ON u.country_id = c.id
                JOIN user_type AS ut ON u.user_type_id = ut.id
                WHERE installer_id IS NULL

                UNION ALL

                -- Installer scanned for dealer
                SELECT
                    sh.installer_id AS user_id,
                    sh.points,
                    installer_ut.user_type,
                    installer.last_name,
                    installer_country.country_name AS country
                FROM scan_history AS sh
                JOIN users AS installer ON sh.installer_id = installer.id
                JOIN user_type AS installer_ut ON installer.user_type_id = installer_ut.id
                JOIN countries AS installer_country ON installer.country_id = installer_country.id
                WHERE sh.installer_id IS NOT NULL
                ) AS combined
                GROUP BY combined.country, combined.user_type, combined.user_id, combined.last_name
                ORDER BY total_points DESC, scans_count DESC  
            """
        
        df_top_users = self.query_to_dataframe(top_users_by_scans_query)

        self.open_dataframe_in_excel(df_top_users)

        QMessageBox.information(self, "Information", "Information about TOP users have been compiled.")

        del df_top_users
        gc.collect()
        print("All dataFrame are deleted") #TODO delete after cleaning

    def close_db_connection(self):
        """ Closing connection to database """
        if self.db_pool:
            self.db_pool.closeall()
            print("Connection to database closed.") #TODO удалить или заменить на логирование

    def closeEvent(self, event):
        """Handle window close event"""
        self.close_db_connection()
        event.accept()
    