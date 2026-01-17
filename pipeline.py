
import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os
load_dotenv()
KOBO_USERNAME= os.getenv("usolange_1988")
KOBO_PASSWORD= os.getenv("NGOGAshami2024@")
KOBO_CSV_URL= "https://kf.kobotoolbox.org/api/v2/assets/acWJxjAtawBYdhgpEZxuXZ/export-settings/esHNpJA4xq7t47LQ8SvqTyy/data.csv"
# PostgreSQL credentials
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = ("NGOGAshami@2024")
PG_PORT = os.getenv("5432")

# Schema and table details
schema_name = "project"
table_name = "Waste_management_project"
#Public
#flict # Please avoid special characters in the table name

# Step 1: Fetch data from Kobo Toolbox
print("Fetching data from KoboToolbox...") 
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code == 200:
    print("✅ Data fetched successfully")

    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data, sep=';', on_bad_lines='skip') 

    # Step 2 clean and transform data
    print("Processing data...")
    df.columns = [col.strip().replace(" ", "").replace("&", "and").replace("-", "") for col in df.columns]

    # Compute total HelpDesk_Feedbacks
    df["Waste_management_project"] = df[["Waste_management_project", "Waste_management_project"]].sum(axis=1)

    # Convert Date to proper format (Optional)
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')

    # Step 3: Upload to PostgreSQL
    print("Uploading data to PostgreSQL...")

    conn = psycopg2.connect(
        host=localhost,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=5432
    )

    cur = conn.cursor()

    # Create schema if it doesn't exist
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {Waste_management};")