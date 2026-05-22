import os
import tempfile
import subprocess
import sys

import pandas as pd
from datetime import datetime
from functools import wraps

from PyQt5.QtWidgets import QMainWindow, QLabel, QPushButton, QMessageBox, QInputDialog, QWidget, QVBoxLayout
from PyQt5.QtGui import QIcon, QPixmap, QFont

from Docker_App.server_app.server import DatabaseService

import logging

def for_data_about_users(func):
    """Decorator for loading and cleaning user data"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):

        df = self.load_and_clean_users()

        if df is None or df.empty:
            logging.warning("Warning! After cleaning database is empty. Data about users cannot be generated.")
            QMessageBox.warning(self, "Warning!", "After cleaning database is empty.")
            return
        
        return func(self, df, *args, **kwargs)
    
    return wrapper

def for_data_about_scans(func):
    """Decorator for loading data about scans"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):

        df = self.load_data_about_scans()

        if df is None or df.empty:
            logging.warning("Warning! Database about scans is empty. Data about scans generated cannot be generated.")
            QMessageBox.warning(self, "Warning!", "Database about scans is empty.")
            return

        return func(self, df, *args, **kwargs)
    return wrapper

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()

        self.df_users = None
        self.df_scans = None
        self.data_service = DatabaseService()
        
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

    def load_and_clean_users(self, force_reload: bool = False):
        """Load and clean user data from database service"""
        
        if not force_reload and self.df_users is not None and not self.df_users.empty:
            logging.info("Using cached user data.")
            return self.df_users
        
        df = self.data_service.load_and_clean_users()
        self.df_users = df
        return df

    def load_data_about_scans(self, force_reload: bool = False):
        """Load and cache scan data from database service"""
        if not force_reload and self.df_scans is not None and not self.df_scans.empty:
            logging.info("Using cached scan data.")
            return self.df_scans

        df = self.data_service.load_data_about_scans()
        self.df_scans = df
        return df

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
            logging.error("Error opening excel file", exc_info = True)
            QMessageBox.warning(self, "Attention!", f"Unable to open Excel file: {e}")

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
        QMessageBox.information(self, "Information.", "Data of the number of authorized users has been generated.")

    def scanned_by_year(self):
        """Scanned users by year - call server method directly"""
        self.data_service.scanned_users_by_year()

    def scans_products_by_year(self):
        """Scans products by year - calls server method directly"""
        self.data_service.scans_products_by_year()

    def top_users_by_scans(self):
        """Top users by scans - call server method directly"""
        self.data_service.top_users_by_scans()
        
    def parse_date(self, prompt_title, prompt_text):
        """Helper to parse date from user input"""

        date_str, ok = QInputDialog.getText(self, prompt_title, prompt_text)
        if not ok or not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%d.%m.%Y")
        except ValueError:
            logging.error("Invalid date format entered by user", exc_info = True)
            QMessageBox.warning(self, "Attention!", "The entered date is incorrect! Format: dd.mm.yyyy")
            return None

    def show_dataframe(self, df, message=None):
        """Helper to show DataFrame in Excel and show message"""

        if df is None or df.empty:
            logging.error("DataFrame is empty.")
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
            logging.error("Start date is not valid. Authorization during period cannot be calculated.", exc_info = True)
            return
        
        end_date = self.parse_date("End of a period:", "Specify the end of the period in the format dd.mm.yyyy (separated by a dot):")
        if end_date is None:
            logging.error("End date is not valid. Authorization during period cannot be calculated.", exc_info = True)
            return

        if end_date < start_date:
            QMessageBox.warning(self, "Warning!", "End date must be greater than start date.")
            logging.error("End date is greated or equals start date. Authorization during period cannot be calculated.", exc_info = True)
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

        self.show_dataframe(grouped, "Information of the number of authorized users for the period has been generated.")

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

    @for_data_about_scans
    def data_about_scans_during_period(self, df):
        """Data about number of users and scans during period"""

        start_date = self.parse_date(
                "Period start",
                "Enter the period start in dd.mm.yyyy (separated by dot):"
            )
        if start_date is None:
            logging.error("Start date is not valid. Data about scans during period cannot be generated.", exc_info = True)
            return

        end_date = self.parse_date(
            "End of period",
            "Enter the end of the period in dd.mm.yyyy (separated by dot):"
        )
        if end_date is None:
            logging.error("End date is not valid. Data about scans during period cannot be generated.", exc_info = True)
            return

        if end_date < start_date:
            QMessageBox.warning(self, "Attention!", "End date must be the same or after start date.")
            logging.error("End date is before start date. Data about scans during period cannot be generated.")
            return
    
        mask_for_filter = (df["created_at"].dt.date >= start_date.date()) & (df["created_at"].dt.date <= end_date.date())
        df_data_about_scans_during_period = df[mask_for_filter]

        self.open_dataframe_in_excel(df_data_about_scans_during_period)
        QMessageBox.information(self, "Information", "Data about scans during period has been generated.")
