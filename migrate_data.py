import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import toml

# 1. LOAD SECRETS
secrets = toml.load(".streamlit/secrets.toml")
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_info(secrets["gcp_service_account"], scopes=scopes)
gc = gspread.authorize(creds)

# 2. CONFIG
CSV_FILE = "Mastersheet Trainer Feedbacks CL-2.xlsx - Sheet1.csv"  # Ensure this file is in your folder
NEW_SHEET_NAME = "Webinar Master Data"  # CHANGE THIS to your new sheet's name

# 3. READ OLD DATA
print("Reading old CSV...")
df = pd.read_csv(CSV_FILE)

# 4. MAP COLUMNS (Old -> New)
# We select only what we need. 
# Note: Old sheet doesn't have "Peak", "Unique", "End Count" for everything, so we fill 0.
clean_df = pd.DataFrame()
clean_df["Date"] = df["Date"]
clean_df["Trainer"] = df["Trainer"]
clean_df["Session Title"] = df["Sessions"] # Mapping 'Sessions' to 'Session Title'
clean_df["Batch"] = df["Batch"]
clean_df["Duration (Min)"] = pd.to_numeric(df["Duration (Minutes)"], errors='coerce').fillna(0)
clean_df["Peak Attendees"] = pd.to_numeric(df["Peak Attendance"], errors='coerce').fillna(0)
clean_df["Unique Attendees"] = 0 # Not in old sheet
clean_df["End Count"] = 0        # Not in old sheet
clean_df["Retention Score"] = "" # Not in old sheet
clean_df["Overall Rating"] = pd.to_numeric(df["Overall rating"], errors='coerce').fillna(0)
clean_df["Trainer Rating"] = pd.to_numeric(df["Trainer rating"], errors='coerce').fillna(0)
clean_df["Responses"] = pd.to_numeric(df["Number of Responses"], errors='coerce').fillna(0)
clean_df["NPS"] = 0              # Not in old sheet
clean_df["Session Type"] = df["Live/Simulive"]

# Replace NaN with empty string or 0 to avoid JSON errors
clean_df = clean_df.fillna("")

# 5. UPLOAD TO GOOGLE SHEET
print(f"Uploading {len(clean_df)} rows to '{NEW_SHEET_NAME}'...")
try:
    sh = gc.open(NEW_SHEET_NAME)
    ws = sh.sheet1
    # Prepare data as list of lists
    data = [clean_df.columns.values.tolist()] + clean_df.values.tolist()
    # Clear and Update
    ws.clear()
    ws.update(data)
    print("✅ Migration Successful!")
except Exception as e:
    print(f"❌ Error: {e}")