from fastapi import FastAPI
from server_app.server import DatabaseService
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv
import os

from contextlib import asynccontextmanager


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
    return df.to_dict(orient = "records")

@app.get("/scanned_users_by_year")
def get_scanned_users_by_year():
    df = db.scanned_users_by_year()
    optimized_df = df.copy()
    for col in optimized_df.select_dtypes(include=['datetime64[ns]','datetime64']).columns:
        optimized_df[col] = optimized_df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
        
    return optimized_df.where(pd.notnull(optimized_df), None).to_dict(orient='records')

@app.get("/scans_products_by_year")
def scans_products_by_year():
    df = db.scans_products_by_year()
    optimized_df = df.copy()
    for col in optimized_df.select_dtypes(include=['datetime64[ns]','datetime64']).columns:
        optimized_df[col] = optimized_df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
        
    return optimized_df.where(pd.notnull(optimized_df), None).to_dict(orient='records')

@app.get("/top_users_by_scans")
def top_users_by_scans():
    df = db.top_users_by_scans()
    optimized_df = df.copy()
    for col in optimized_df.select_dtypes(include=['datetime64[ns]','datetime64']).columns:
        optimized_df[col] = optimized_df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
        
    return optimized_df.where(pd.notnull(optimized_df), None).to_dict(orient='records')

@app.get("/authorization_during_period")
def authorization_during_period(start_date: str, end_date: str):
    df = db.load_and_clean_users()
    
    optimized_df = df.copy()
    for col in optimized_df.select_dtypes(include=['datetime64[ns]','datetime64']).columns:
        optimized_df[col] = optimized_df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    
    df = optimized_df.where(pd.notnull(optimized_df), None).to_dict(orient='records')

    df["last_authorization"] = pd.to_datetime(df["last_authorization"], errors = "coerce")

    start = datetime.strptime(start_date, "%d.%m.%Y")
    end = datetime.strptime(end_date, "%d.%m.%Y")

    mask = (df["last_authorization"].dt.date >= start.date()) & (df["last_authorization"].dt.date <= end.date())
    df_period = df[mask]

    grouped = df_period.groupby(["country_name", "user_type"]).size().reset_index(name = "authorized_count")
    total = grouped["authorized_count"].sum()
    grouped = pd.concat([grouped, pd.DataFrame([{"country_name": "TOTAL", "user_type": "", "authorized_count": total}])])

    return grouped.to_dict(orient = "records")

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

    return grouped.to_dict(orient = "records")

@app.get("/all_scans")
def all_scans():
    df = db.load_data_about_scans()
    df = df.where(pd.notnull(df), None) #change NaN to None
    df = df.astype(object)
    df = df.fillna(value = None)
    return df.astype(object).to_dict(orient = "records")

@app.get("/data_about_scans_during_period")
def data_about_scans_during_period(start_date: str, end_date: str):
    df = db.load_data_about_scans()
    
    optimized_df = df.copy()
    for col in optimized_df.select_dtypes(include=['datetime64[ns]','datetime64']).columns:
        optimized_df[col] = optimized_df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    df["created_at"] = pd.to_datetime(optimized_df["created_at"], errors = "coerce")

    start = datetime.strptime(start_date, "%d.%m.%Y")
    end = datetime.strptime(end_date, "%d.%m.%Y")

    mask = (df["created_at"].dt.date >= start.date()) & (df["created_at"].dt.date <= end.date())
    
    df_data_about_scans_during_period = df[mask]

    return df_data_about_scans_during_period.to_dict(orient = "records")
