
import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
import os

# 1. Configuration
KOBO_API_TOKEN = "55709dfbbffad50be70c72bd30c55ce623bcae6d"
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/aWRvnAAC4qxiV86SY6JmHa/export-settings/esrxUHk4AQr4wAQBx6SRuKC/data.csv"

print("Fetching data from KoboToolbox...")
response = requests.get(KOBO_CSV_URL, headers={"Authorization": f"Token {KOBO_API_TOKEN}"})

if response.status_code == 200:
    print("✅ Data fetched successfully")
    csv_data = io.StringIO(response.text)
    
    # We use sep=';' because your Kobo export seems to use semicolons
    df = pd.read_csv(csv_data, sep=';', on_bad_lines='skip')
    
    # IMPORTANT: Only strip whitespace, don't remove spaces yet so we can match the SQL
    df.columns = [col.strip() for col in df.columns]

    # Database connection
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="NGOGAshami@2024",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        print("Uploading data to PostgreSQL...")
        
        # Create schema and table
        cur.execute("CREATE SCHEMA IF NOT EXISTS waste_management;")
        cur.execute("DROP TABLE IF EXISTS waste_management.waste_management_project;")
        
        cur.execute("""
            CREATE TABLE waste_management.waste_management_project (
                id SERIAL PRIMARY KEY,
                "start" TIMESTAMP,
                "end" TIMESTAMP,
                "Gender" TEXT,
                "Age" TEXT,
                "District" TEXT,
                "Do you generate waste" TEXT,
                "Most common waste" TEXT,
                "Do you sort" TEXT,
                "Type of waste sort" TEXT,
                "Waste collector" TEXT,
                "How often" TEXT,
                "Know the rules" TEXT,
                "Source of information" TEXT,
                "Waste challenges" TEXT,
                "Plastic waste handling" TEXT,
                "Proper waste management is it important" TEXT,
                "How to improve waste management" TEXT
            );
        """)

        # SQL Insert Template (Note the double quotes for columns with spaces)
        insert_query = """
            INSERT INTO waste_management.waste_management_project (
                "start", "end", "Gender", "Age", "District", 
                "Do you generate waste", "Most common waste", "Do you sort", 
                "Type of waste sort", "Waste collector", "How often", 
                "Know the rules", "Source of information", "Waste challenges", 
                "Plastic waste handling", "Proper waste management is it important", 
                "How to improve waste management"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            cur.execute(insert_query, (
                row.get("start"),
                row.get("end"),
                row.get("Gender"),
                row.get("Age"),
                row.get("District"),
                row.get("Do you generate waste"),
                row.get("Most common waste"),
                row.get("Do you sort"),
                row.get("Type of waste sort"),
                row.get("Waste collector"),
                row.get("How often"),
                row.get("Know the rules"),
                row.get("Source of information"),
                row.get("Waste challenges"),
                row.get("Plastic waste handling"),
                row.get("Proper waste management is it important"),
                row.get("How to improve waste management")
            ))
        
        conn.commit()
        print("✅ Data successfully loaded! Refresh pgAdmin to see the values.")

    except Exception as e:
        print(f"❌ Database Error: {e}")
        if 'conn' in locals(): conn.rollback()
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()
else:
    print(f"❌ Failed to fetch data. Status: {response.status_code}")