from fastapi import FastAPI
from server_app.server import DatabaseService
from datetime import datetime
import pandas as pd
import numpy as np

from dotenv import load_dotenv
import os

from contextlib import asynccontextmanager

def df_to_json_records(df: pd.DataFrame):
    optimized_date_df = df.copy()
    for col in optimized_date_df.select_dtypes(include=['datetime64[ns]', 'datetime64']).columns:
        optimized_date_df[col] = optimized_date_df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    optimized_date_df = optimized_date_df.astype(object).where(pd.notnull(optimized_date_df), None)
    optimized_date_df = optimized_date_df.replace({np.nan: None})
    
    return optimized_date_df.to_dict(orient='records')

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(env_path)

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    db.close_db_connection()
    
app = FastAPI(lifespan = lifespan)
db = DatabaseService()

@app.get("/")
def home():
    return {"message": "Server is running..."}

@app.get("/users")
def get_users():
    df = db.load_and_clean_users()
    return df_to_json_records(df)

@app.get("/scanned_users_by_year")
def get_scanned_users_by_year():
    df = db.scanned_users_by_year()
    return df_to_json_records(df)

@app.get("/scans_products_by_year")
def scans_products_by_year():
    df = db.scans_products_by_year()
    return df_to_json_records(df)

@app.get("/top_users_by_scans")
def top_users_by_scans():
    df = db.top_users_by_scans()
    return df_to_json_records(df)

@app.get("/authorization_during_period")
def authorization_during_period(start_date: str, end_date: str):
    df = db.load_and_clean_users()

    df["last_authorization"] = pd.to_datetime(df["last_authorization"], errors="coerce")
    start = datetime.strptime(start_date, "%d.%m.%Y")
    end = datetime.strptime(end_date, "%d.%m.%Y")

    mask = (
            (df["last_authorization"].dt.date >= start.date())
            & (df["last_authorization"].dt.date <= end.date())
            )

    df_period = df.loc[mask].copy()

    grouped = df_period.groupby(["country_name", "user_type"]).size().reset_index(name="authorized_count")
    total = grouped["authorized_count"].sum()
    grouped = pd.concat(
        [
        grouped,
        pd.DataFrame([{"country_name": "TOTAL", "user_type": "", "authorized_count": total}]),
    ],
        ignore_index=True,
    )
    
    return df_to_json_records(grouped)

@app.get("/points_by_users_and_countries")
def points_by_users_and_countries():
    
    df = db.load_and_clean_users()

    grouped = (df.groupby(["country_name", "user_type"])["points"].sum().reset_index(name="sum_points"))

    total_points = grouped["sum_points"].sum()
    total_row = pd.DataFrame([{
            "country_name": "TOTAL",
            "user_type": "",
            "sum_points": total_points
        }])
    grouped = pd.concat([grouped, total_row], ignore_index=True)

    return df_to_json_records(grouped)

@app.get("/all_scans")
def all_scans():
    df = db.load_data_about_scans()
    return df_to_json_records(df)

@app.get("/data_about_scans_during_period")
def data_about_scans_during_period(start_date: str, end_date: str):
    df = db.load_data_about_scans()
    
    df["created_at"] = pd.to_datetime(df["created_at"], errors = "coerce")
    
    start = datetime.strptime(start_date, "%d.%m.%Y")
    end = datetime.strptime(end_date, "%d.%m.%Y")

    mask = (df["created_at"].dt.date >= start.date()) & (df["created_at"].dt.date <= end.date())
    df_period = df.loc[mask].copy()

    for col in df_period.select_dtypes(include=['datetime64[ns]','datetime64']).columns:
        df_period[col] = df_period[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    return df_to_json_records(df_period)
