import os

import logging

import pandas as pd
from datetime import datetime

from psycopg2.pool import SimpleConnectionPool

class DatabaseService:

    def __init__(self):
        self.db_pool = None
        self.connect()

    def connect(self):
        """Connection to database"""

        try:
            logging.info("Connecting to database...")
            logging.debug(f"DB connection params: host={os.getenv('DB_HOST')}, db={os.getenv('DB_NAME')}, user={os.getenv('DB_USER')}")
            self.db_pool = SimpleConnectionPool(
                minconn=5,
                maxconn=20,
                user = os.getenv('DB_USER'),
                password = os.getenv('DB_PASSWORD'),
                database = os.getenv('DB_NAME'),
                host = os.getenv('DB_HOST'),
                port = int(os.getenv('DB_PORT', 5432)),
            )
            logging.info("Connection with database is established.")
        except Exception as e:
            logging.error("Error connecting to database", exc_info = True)
            self.db_pool = None

    def execute_query(self, query, params=None):
        """Execute query and return results, columns"""

        if not self.db_pool:
            logging.warning("Database pool is not initialized. Query aborted.")
            return None, None
        
        conn = None
        try:
            conn = self.db_pool.getconn()
            logging.info(f"Executing query: {query}")
            logging.debug(f"With params: {params}")

            with conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

            logging.info(f"Query executed successfully, {len(results)} rows and {len(columns)} columns")
            logging.debug(f"Columns: {columns}")

            return results, columns
        except Exception as e:
            logging.error("Error executing query", exc_info=True)
            return None, None
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def query_to_dataframe(self, query, params=None):
        """Run query, load results into pandas DataFrame with query column names."""

        try:
            results, columns = self.execute_query(query, params)
        except Exception as e:
            logging.error("Error converting query to DataFrame", exc_info = True)
            return pd.DataFrame()

        if not results or not columns:
            return pd.DataFrame()
        return pd.DataFrame(results, columns=columns)

    def load_and_clean_users(self):     
        """Clean spam and test accounts in DataFrame"""
        logging.info("Loading and cleaning user data.")

        exclude_users = (
            'kazah89', 'kazah1122', 'russia89', 'sanin', 'samoilov', 'axorindustry',
            'kreknina', 'zeykin', 'berdnikova', 'ostashenko', 'bellaruss89@gmail.com',
            'skalar', 'test', 'malyigor', 'ihormaly', 'axor', 'kosits'
        )

        exclude_patterns = [f"%{user}%" for user in exclude_users]

        query = """
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
            AND NOT (u.email ILIKE ANY (%s::text[]))
        """

        df = self.query_to_dataframe(query, params=(exclude_patterns,))
        logging.info("User data loaded and cleaned: %d rows.", len(df))

        self.df_users = df
        return df
    
    def load_data_about_scans(self):
        """Loading data about scans by all users"""
        logging.info("Loading scan history data.")

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
        logging.info("Scan history data loaded: %d rows.", len(df))
        self.df_scans = df
        return df

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
            logging.error("Data about scanning users by year is empty after query execution.", exc_info = True)
            return pd.DataFrame()
        
        df_scanned_users_by_year_pivot_df = (
            df_scanned_users_by_year.pivot_table(
                index=["country_name", "user_type"],
                columns="year",
                values="user_count",
                fill_value=0
            ).reset_index()
        )
            
        return df_scanned_users_by_year_pivot_df

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
        GROUP BY country_name, user_type, product, year
        ORDER BY country_name
        """

        df_scans_products_by_year = self.query_to_dataframe(query_scans_products_by_year)
        if df_scans_products_by_year is None or df_scans_products_by_year.empty:
            logging.error("Data about scans and points of products by year is empty after query execution", exc_info = True)
            return pd.DataFrame()

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

        return df_scans_products_by_year_pivot_df

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

        return df_top_users

    def close_db_connection(self):
        """Close database connection pool"""
        
        if self.db_pool:
            try:
                logging.info("Closing database connection pool...")
                self.db_pool.closeall()
                logging.info("Database connection pool closed successfully")
            except Exception as e:
                logging.error("Erro closing database connection pool", exc_info = True)
