from fastapi import FastAPI
from server_app.server import DatabaseService

from dotenv import load_dotenv
import os


env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(env_path)

app = FastAPI()
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
    return df.to_dict(orient = "records")

@app.get("/scans_products_by_year")
def scans_products_by_year():
    df = db.scans_products_by_year()
    return df.to_dict(orient="records")
