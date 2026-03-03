import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import toml
import os

# ─── 1. CONFIGURATION ───
# 🔴 PASTE YOUR SPREADSHEET ID BELOW (From the URL)
SHEET_ID = "1jYRJe9APAlIZdMQ9svuOo9gR1DbYfrCUjThvtO1DXcI" 

# ─── 2. SETUP ───
print("🔑 Loading secrets...")
secrets = toml.load(".streamlit/secrets.toml")
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_info(secrets["gcp_service_account"], scopes=scopes)
gc = gspread.authorize(creds)

# ─── 3. FIND LOCAL DATA FILE ───
files = [f for f in os.listdir('.') if 'Mastersheet' in f and (f.endswith('.xlsx') or f.endswith('.csv'))]
if not files:
    print("❌ No Mastersheet file found in this folder.")
    exit()

TARGET_FILE = files[0]
print(f"📂 Reading local file: {TARGET_FILE}")

# ─── 4. READ DATA ───
try:
    if TARGET_FILE.endswith('.xlsx'):
        df = pd.read_excel(TARGET_FILE)
    else:
        try:
            df = pd.read_csv(TARGET_FILE, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(TARGET_FILE, encoding='latin-1')
    print(f"✅ Loaded {len(df)} rows.")
except Exception as e:
    print(f"❌ Read Failed: {e}")
    exit()

# ─── 5. MAP COLUMNS ───
print("✨ Mapping columns...")
new_df = pd.DataFrame()

def get_col(name):
    for col in df.columns:
        if name.lower() == str(col).lower(): return df[col]
    return ""

new_df["Date"] = get_col("Date")
new_df["Trainer"] = get_col("Trainer")
new_df["Session Title"] = get_col("Sessions")
new_df["Batch"] = get_col("Batch")
new_df["Duration"] = pd.to_numeric(get_col("Duration (Minutes)"), errors='coerce').fillna(0)
new_df["Peak"] = pd.to_numeric(get_col("Peak Attendance"), errors='coerce').fillna(0)
new_df["Unique"] = 0
new_df["End Count"] = 0 
new_df["Retention Score"] = 0
new_df["Overall Rating"] = pd.to_numeric(get_col("Overall rating"), errors='coerce').fillna(0)
new_df["Trainer Rating"] = pd.to_numeric(get_col("Trainer rating"), errors='coerce').fillna(0)
new_df["Responses"] = pd.to_numeric(get_col("Number of Responses"), errors='coerce').fillna(0)
new_df["NPS"] = 0 
new_df["Type"] = get_col("Live/Simulive")

new_df = new_df.fillna("")
new_df["Date"] = new_df["Date"].astype(str).replace("NaT", "")

# ─── 6. UPLOAD (SAFE METHOD) ───
print(f"🚀 Connecting to Sheet ID: {SHEET_ID}...")
try:
    sh = gc.open_by_key(SHEET_ID) # Open by ID (Unmistakable)
    print(f"   Found Sheet: '{sh.title}'")
    
    ws = sh.sheet1
    ws.clear()
    
    # Prepare Data
    data = [new_df.columns.values.tolist()] + new_df.values.tolist()
    
    # SAFE UPDATE: Specify range 'A1' explicitly to avoid confusion
    ws.update(range_name='A1', values=data)
    
    print("✅ SUCCESS! Go check your Google Sheet now.")
    
except Exception as e:
    print(f"❌ Upload Failed: {e}")
    print("TIP: Did you share the sheet with the streamlit-bot email?")