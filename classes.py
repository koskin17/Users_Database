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

        self.btn_scanned_users_by_year = QPushButton(
            "Scanned users by year", self)
        self.btn_scanned_users_by_year.clicked.connect(self.scanned_users_by_year)

        self.btn_scans_products_by_year = QPushButton("Data about scans and sum of point by year", self)
        self.btn_scans_products_by_year.clicked.connect(self.scans_products_by_year)

        self.btn_data_about_scans_during_period = QPushButton("Scans for the period",
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
        layout.addWidget(self.btn_scanned_users_by_year)
        layout.addWidget(self.btn_scans_products_by_year)
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
        """Execute query and return results, columns"""

        if not self.db_pool:
            QMessageBox.warning(self, "Attention!", "Database pool not initialized.")
            return None, None
        
        conn = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return results, columns
        except Exception as e:
            QMessageBox.warning(self, "Attention!", f"Error executing query: {e}")
            return None, None
        finally:
            if conn:
                cursor.close()
                self.db_pool.putconn(conn)

    def query_to_dataframe(self, query, params=None):
        """Run query, load results into pandas DataFrame with query column names."""

        results, columns = self.execute_query(query, params)

        if not results or not columns:
            QMessageBox.information(self, "Information", "Dataframe is empty.")
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
        
        exclude_patterns = [f"%{user}%" for user in exclude_users]
        
        placeholders = " AND ".join(["u.email NOT ILIKE %s"] * len(exclude_users))

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
            AND ({placeholders})
        """

        df = self.query_to_dataframe(query, params=exclude_patterns)

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
        QMessageBox.information(self, "Information.", "Data about all users in database has been generated.")

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

    def parse_date(self, prompt_title, prompt_text):
        """Helper to parse date from user input"""

        date_str, ok = QInputDialog.getText(self, prompt_title, prompt_text)
        if not ok or not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%d.%m.%Y")
        except ValueError:
            QMessageBox.warning(self, "Attention!", "The entered date is incorrect! Format: dd.mm.yyyy")
            return None

    def show_dataframe(self, df, message=None):
        """Helper to show DataFrame in Excel and show message"""
        if df is None or df.empty:
            QMessageBox.warning(self, "Attention!", "DataFrame is empty.")
            return
        self.open_dataframe_in_excel(df)
        if message:
            QMessageBox.information(self, "Information", message)

    @for_data_about_users
    def authorization_during_period(self, df):
        """Information about the amount of authorized users for the period"""
        
        start_date = self.parse_date("Beginning of the period:", "Specify the beginning of the period in the format dd.mm.yyyy (separated by a dot):")
        if start_date is None:
            return
        
        end_date = self.parse_date("End of a period:", "Specify the end of the period in the format dd.mm.yyyy (separated by a dot):")
        if end_date is None:
            return
        
        mask_for_filter = (df["last_authorization"].dt.date >= start_date.date()) & (df["last_authorization"].dt.date <= end_date.date())
        df_period = df[mask_for_filter]
        
        grouped = df_period.groupby(["country_name", "user_type"]).size().reset_index(name="authorized_count")
        total = grouped["authorized_count"].sum()
        total_row = pd.DataFrame([{
                "country_name": "TOTAL",
                "user_type": "",
                "authorized_count": total
            }])
        grouped = pd.concat([grouped, total_row], ignore_index=True)
                    
        self.show_dataframe(grouped, "Information on the number of authorized users for the period has been generated.")

    @for_data_about_users
    def points_by_users_and_countries(self, df):
        """ Information about current sum of points by users and countries """
    
        grouped = (df.groupby(["country_name", "user_type"])["points"].sum().reset_index(name="sum_points"))

        total_points = grouped["sum_points"].sum()
        total_row = pd.DataFrame([{
                "country_name": "TOTAL",
                "user_type": "",
                "sum_points": total_points
            }])
        grouped = pd.concat([grouped, total_row], ignore_index=True)

        self.open_dataframe_in_excel(grouped)
        QMessageBox.information(self, "Information", "Data about the current sum of points by type of users and countries has been generated.")

    @for_data_about_scans
    def all_scans(self, df):
        """Information about all scans in the database"""
        self.open_dataframe_in_excel(df)

    def scanned_users_by_year(self):
        """Information about the number of scanning users by year, country, and user type
        
        Generate yearly stats of distinct scanning users by country and user type.

        1. Select dealers and installers who scanned for themselves, i.e., the installer_id field is empty.
        2. Select installers who scanned for the dealer, i.e., the installer ID is specified in the installer_id field.
        3. Combine the results into a single table, and calculate the number of unique user IDs.
    """
    
        query_scanned_users_by_year = """
            WITH self_scans AS (
                -- Dealers and installers are scans for himself
                SELECT
                    u.id AS user_id,
                    c.country_name,
                    ut.user_type,
                    sh.created_at
                FROM scan_history AS sh
                JOIN users AS u ON sh.user_id = u.id
                JOIN countries AS c ON u.country_id = c.id
                JOIN user_type AS ut ON u.user_type_id = ut.id
                WHERE sh.installer_id IS NULL
                AND ut.id IN (1, 2)
            ),
            installer_scans AS (
                -- Installer ar scans for dealers
                SELECT
                    sh.installer_id AS user_id,
                    c.country_name,
                    ut.user_type,
                    sh.created_at
                FROM scan_history AS sh
                JOIN users AS u ON sh.installer_id = u.id
                JOIN countries AS c ON u.country_id = c.id
                JOIN user_type AS ut ON u.user_type_id = ut.id
                WHERE sh.installer_id IS NOT NULL
                AND ut.id = 2
            ),
            combined AS (
                SELECT * FROM self_scans
                UNION ALL
                SELECT * FROM installer_scans
            )
            SELECT
                country_name,
                user_type,
                EXTRACT(YEAR FROM created_at) AS year,
                COUNT(DISTINCT user_id) AS user_count
            FROM combined
            GROUP BY country_name, user_type, year
            ORDER BY country_name, user_type, year;
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
        QMessageBox.information(self, "Information", "Statistics about scanning users by year have been compiled.")
            
        del df_scanned_users_by_year, df_scanned_users_by_year_pivot_df
        gc.collect()

    def scans_products_by_year(self): 
        """Data about scans and sum of points of products by country, user type, year """

        query_scans_products_by_year="""
        SELECT
            c.country_name,
            ut.user_type,
            p.product,
            COUNT (sh.id) AS scans,
            SUM (sh.points) AS total_points,
            EXTRACT (YEAR FROM sh.created_at) AS year	
        FROM scan_history AS sh
        JOIN products AS p ON sh.product_id = p.id
        JOIN users AS u ON sh.user_id = u.id
        JOIN countries AS c ON u.country_id = c.id
        JOIN user_type AS ut ON u.user_type_id = ut.id
        GROUP BY country_name, user_type, product, user_type, year
        ORDER BY country_name
        """

        df_scans_products_by_year = self.query_to_dataframe(query_scans_products_by_year)
        if df_scans_products_by_year is None or df_scans_products_by_year.empty:
            QMessageBox.warning(self, "Information", "No scan data is availale.")

        df_scans_products_by_year_pivot_df = (
            df_scans_products_by_year.pivot_table(
                index = ["country_name", "user_type", "product"],
                columns = "year",
                values = ["scans", "total_points"],
                fill_value = 0
            ).reset_index()
        )

        df_scans_products_by_year_pivot_df.columns = [
            "_".join([str(c) for c in col if c]) if isinstance(col, tuple) else str(col)
            for col in df_scans_products_by_year_pivot_df.columns.values
        ]

        self.open_dataframe_in_excel(df_scans_products_by_year_pivot_df)
        QMessageBox.information(self, "Information", "Information about scans of products and total sum has been compiled.")

        del df_scans_products_by_year, df_scans_products_by_year_pivot_df
        gc.collect()

    @for_data_about_scans
    def data_about_scans_during_period(self, df):
        """Data about number of users and scans during period"""

        start_date = self.parse_date(
                "Period start",
                "Enter the period start in dd.mm.yyyy (separated by dot):"
            )
        if start_date is None:
            return

        end_date = self.parse_date(
            "End of period",
            "Enter the end of the period in dd.mm.yyyy (separated by dot):"
        )
        if end_date is None:
            return
    
        mask_for_filter = (df["created_at"].dt.date >= start_date.date()) & (df["created_at"].dt.date <= end_date.date())
        df_data_about_scans_during_period = df[mask_for_filter]

        # df_data_about_scans_during_period_pivot_df = (
        #     df_data_about_scans_during_period.pivot_table(
        #         index=["country_name", "product"],
        #         columns="year",
        #         values="user_count",
        #         fill_value=0
        #     ).reset_index()
        # )

        self.open_dataframe_in_excel(df_data_about_scans_during_period)
        QMessageBox.information(self, "Information", "Data about scans during period has been generated.")

    def top_users_by_scans(self):
        """ TOP dealers / adjusters by scans"""

        top_users_by_scans_query = """
            SELECT
                combined.user_id,
                combined.last_name,
                combined.first_name,
                combined.user_type,
                combined.country,
                combined.phone,
                COUNT (*) AS scans_count,
                SUM (combined.points) AS total_points
            FROM (
                SELECT
                    sh.user_id,
                    sh.points,
                    ut.user_type,
                    u.last_name,
                    u.first_name,
                    u.phone,
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
                    installer.first_name,
                    installer.phone,
                    installer_country.country_name AS country
                FROM scan_history AS sh
                JOIN users AS installer ON sh.installer_id = installer.id
                JOIN user_type AS installer_ut ON installer.user_type_id = installer_ut.id
                JOIN countries AS installer_country ON installer.country_id = installer_country.id
                WHERE sh.installer_id IS NOT NULL
                ) AS combined
                GROUP BY combined.country, combined.user_type, combined.user_id, combined.last_name, combined.first_name, combined.phone
                ORDER BY total_points DESC, scans_count DESC  
            """
        
        df_top_users = self.query_to_dataframe(top_users_by_scans_query)

        self.open_dataframe_in_excel(df_top_users)

        QMessageBox.information(self, "Information", "Information about TOP users have been compiled.")

        del df_top_users
        gc.collect()

    def close_db_connection(self):
        """ Closing connection to database """
        if self.db_pool:
            self.db_pool.closeall()

    def closeEvent(self, event):
        """Handle window close event"""
        self.close_db_connection()
        event.accept()
    