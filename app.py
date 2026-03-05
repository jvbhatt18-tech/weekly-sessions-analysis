import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
from datetime import date
import io
import plotly.express as px
import plotly.graph_objects as go
import json
import time

# ─── 1. CONFIGURATION ───
SHEET_ID = "1jYRJe9APAlIZdMQ9svuOo9gR1DbYfrCUjThvtO1DXcI"
DRIVE_FOLDER_ID = "0ADZkkxHLwZa9Uk9PVA"  # Shared Drive ID

# ─── COLOR PALETTE ───
COLOR_LIVE = "#2E86C1"      # Royal Blue
COLOR_SIMULIVE = "#E67E22"  # Sunset Orange
COLOR_MAP = {"Live": COLOR_LIVE, "Simulive": COLOR_SIMULIVE}

st.set_page_config(page_title="Session Command Center", page_icon="🚀", layout="wide")

# ─── PREMIUM UI STYLING ───
st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    div.block-container { padding-top: 2rem; }
    div[data-testid="stMetric"] { background-color: white; border: 1px solid #e0e0e0; border-radius: 10px; padding: 15px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
    .score-card { 
        background: linear-gradient(135deg, #2c3e50 0%, #4ca1af 100%); 
        color: white; border-radius: 15px; padding: 20px; text-align: center; 
        box-shadow: 0 4px 15px rgba(0,0,0,0.1); margin-bottom: 20px; 
    }
    .score-val { font-size: 2.5rem; font-weight: 800; margin: 0; }
    .score-label { font-size: 0.9rem; opacity: 0.95; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }
    .score-sub { font-size: 0.8rem; opacity: 0.85; margin-top: 5px; font-style: italic; }
    .stDataFrame { background-color: white; border-radius: 10px; padding: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
</style>
""", unsafe_allow_html=True)

# ─── STATE MANAGEMENT ───
if "upload_key" not in st.session_state: st.session_state.upload_key = 0

def reset_app():
    st.session_state.upload_key += 1

def mins_to_hhmm(minutes):
    try:
        m = int(minutes)
        return f"{m // 60}h {m % 60:02d}m"
    except: return "0h 00m"

# ─── CONNECTIONS (OPTIMIZED CACHING) ───
@st.cache_resource
def get_gcp_creds():
    if "gcp_service_account" not in st.secrets: 
        st.error("❌ Secrets not found.")
        return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)

@st.cache_resource
def connect_gsheet():
    """Cached connection to Google Sheet to avoid re-auth lag"""
    creds = get_gcp_creds()
    if not creds: return None
    gc = gspread.authorize(creds)
    try: return gc.open_by_key(SHEET_ID).sheet1
    except Exception as e:
        st.error(f"❌ Sheet Connection Error: {e}")
        return None

@st.cache_data(ttl=300)  # Cache data for 5 minutes
def get_history_df():
    """Fetches data from Google Sheet with caching for speed"""
    ws = connect_gsheet()
    if not ws: return pd.DataFrame()
    data = ws.get_all_records()
    return pd.DataFrame(data)

def upload_to_drive_robust(file_objs, folder_name, date_str, status_container, progress_bar):
    creds = get_gcp_creds()
    if not creds: return None
    
    try:
        service = build('drive', 'v3', credentials=creds)
        
        subfolder_name = f"{date_str} - {folder_name}"
        status_container.write(f"📂 Creating Drive Folder: `{subfolder_name}`...")
        
        file_metadata = {
            'name': subfolder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [DRIVE_FOLDER_ID] 
        }
        
        folder = service.files().create(
            body=file_metadata, 
            fields='id, webViewLink',
            supportsAllDrives=True
        ).execute()
        
        folder_id = folder.get('id')
        folder_link = folder.get('webViewLink')
        
        total_files = len(file_objs)
        for i, f in enumerate(file_objs):
            status_container.write(f"⬆️ Uploading ({i+1}/{total_files}): **{f.name}**...")
            
            if progress_bar:
                progress_val = int(((i + 1) / total_files) * 100)
                progress_bar.progress(progress_val)
            
            f.seek(0)
            file_data = f.read()
            buffer = io.BytesIO(file_data)
            
            mimetype = f.type if hasattr(f, 'type') else 'text/plain'
            filename = f.name if hasattr(f, 'name') else 'unknown_file'
            
            media = MediaIoBaseUpload(buffer, mimetype=mimetype, resumable=True)
            file_meta = {'name': filename, 'parents': [folder_id]}
            
            service.files().create(
                body=file_meta, 
                media_body=media, 
                fields='id',
                supportsAllDrives=True
            ).execute()
        
        return folder_link
        
    except HttpError as e:
        if "storageQuotaExceeded" in str(e):
            st.warning("⚠️ **Storage Quota Exceeded**: File upload skipped, but Data saved to Sheet.")
            return "SKIPPED_QUOTA"
        elif "notFound" in str(e):
            st.error(f"❌ **Folder Not Found (404)**. Check Shared Drive permissions.")
            return None
        else:
            st.error(f"❌ Drive Error: {e}")
            return None
    except Exception as e:
        st.error(f"❌ General Error: {e}")
        return None

# ─── PARSERS ───
def calculate_precise_duration(intervals):
    if not intervals: return 0
    intervals.sort(key=lambda x: x[0])
    merged = []
    for current in intervals:
        if not merged: merged.append(current)
        else:
            if current[0] <= merged[-1][1]: merged[-1] = (merged[-1][0], max(merged[-1][1], current[1]))
            else: merged.append(current)
    total_seconds = sum((end - start).total_seconds() for start, end in merged)
    return int(total_seconds / 60)

def generate_retention_curve(df, join_col, leave_col):
    events = []
    for _, row in df.iterrows():
        if pd.notnull(row[join_col]): events.append((row[join_col], 1))
        if pd.notnull(row[leave_col]): events.append((row[leave_col], -1))
    events.sort(key=lambda x: x[0])
    timeline, current, peak = [], 0, 0
    for t, change in events:
        current += change
        peak = max(peak, current)
        timeline.append({"Time": t, "Attendees": current})
    df_tl = pd.DataFrame(timeline)
    if not df_tl.empty:
        df_tl = df_tl.set_index("Time").resample("1min").last().ffill().reset_index()
    return df_tl, peak

def compress_curve(df_tl, points=30):
    if df_tl is None or df_tl.empty: return ""
    try:
        indices = [int(i * (len(df_tl) - 1) / (points - 1)) for i in range(points)]
        subset = df_tl.iloc[indices]
        counts = subset["Attendees"].astype(int).tolist()
        return "|".join(map(str, counts))
    except: return ""

def parse_attendee_smart(uploaded_file):
    metrics = {
        "trainer": "Unknown", "duration": 0, "peak": 0, "unique": 0, 
        "title": "Unknown", "date": date.today(), 
        "timeline": pd.DataFrame(), "end_count": 0, "stickiness": 0,
        "is_simulive": False, "curve_str": ""
    }
    try:
        content = uploaded_file.getvalue().decode("utf-8", errors='replace')
        lines = content.splitlines()
        
        for line in lines[:5]:
            if "Topic" in line and "Start Time" in line:
                try:
                    row = next(pd.read_csv(io.StringIO(lines[lines.index(line)+1]), header=None).iterrows())[1]
                    metrics["title"] = str(row[0]).strip()
                    metrics["date"] = pd.to_datetime(str(row[2]).split()[0]).date()
                except: pass
                break
        
        tail_lines = lines[-30:] if len(lines) > 30 else lines
        for line in tail_lines:
            if "Presenter" in line:
                metrics["is_simulive"] = True
                try:
                    parts = next(pd.read_csv(io.StringIO(line), header=None).iterrows())[1]
                    if len(parts) > 3 and isinstance(parts[3], (int, float)):
                        metrics["duration"] = int(parts[3])
                except: pass
                break

        p_start, a_start = -1, -1
        for i, line in enumerate(lines):
            if "Panelist Details" in line: p_start = i
            if "Attendee Details" in line: a_start = i
        
        if p_start != -1 and not metrics["is_simulive"]:
            chunk = lines[p_start+1:a_start if a_start!=-1 else len(lines)]
            p_head = next((j for j, l in enumerate(chunk) if "User Name" in l and "Join Time" in l), -1)
            if p_head != -1:
                df_p = pd.read_csv(io.StringIO("\n".join(chunk[p_head:])), index_col=False)
                name = next((c for c in df_p.columns if "User Name" in c), None)
                join = next((c for c in df_p.columns if "Join Time" in c), None)
                leave = next((c for c in df_p.columns if "Leave Time" in c), None)
                if name and join and leave:
                    excl = ['team be10x', 'host', 'notetaker', 'otter', 'admin', 'assistant']
                    df_p = df_p[~df_p[name].astype(str).str.lower().str.contains('|'.join(excl))]
                    df_p[join] = pd.to_datetime(df_p[join], errors='coerce')
                    df_p[leave] = pd.to_datetime(df_p[leave], errors='coerce')
                    df_p = df_p.dropna(subset=[join, leave])
                    stats = []
                    for p, g in df_p.groupby(name):
                        dur = calculate_precise_duration(list(zip(g[join], g[leave])))
                        stats.append((p, dur))
                    if stats:
                        stats.sort(key=lambda x: x[1], reverse=True)
                        metrics["trainer"], metrics["duration"] = stats[0]
        
        if a_start != -1:
            chunk = lines[a_start+1:]
            a_head = next((j for j, l in enumerate(chunk) if "User Name" in l and "Email" in l), -1)
            if a_head != -1:
                df_a = pd.read_csv(io.StringIO("\n".join(chunk[a_head:])), index_col=False)
                email = next((c for c in df_a.columns if "Email" in c), None)
                join = next((c for c in df_a.columns if "Join Time" in c), None)
                leave = next((c for c in df_a.columns if "Leave Time" in c), None)
                if email: metrics["unique"] = df_a[email].astype(str).str.strip().str.lower().nunique()
                if join and leave:
                    df_a[join] = pd.to_datetime(df_a[join], errors='coerce')
                    df_a[leave] = pd.to_datetime(df_a[leave], errors='coerce')
                    df_a = df_a.dropna(subset=[join, leave])
                    timeline, peak = generate_retention_curve(df_a, join, leave)
                    metrics["peak"] = peak
                    metrics["timeline"] = timeline
                    metrics["curve_str"] = compress_curve(timeline)
                    if not timeline.empty:
                        total_mins = len(timeline)
                        tail_mins = max(1, int(total_mins * 0.10))
                        metrics["end_count"] = timeline.iloc[-tail_mins:]["Attendees"].mean()
                        metrics["stickiness"] = (metrics["end_count"] / peak) if peak > 0 else 0
    except: pass
    return metrics

def parse_poll_dynamic(uploaded_file):
    try:
        uploaded_file.seek(0)
        lines = uploaded_file.getvalue().decode("utf-8", errors='replace').splitlines()
        h_idx = next((i for i, l in enumerate(lines) if "User Name" in l and "Email" in l), -1)
        if h_idx == -1: return None
        data = [lines[h_idx]]
        for l in lines[h_idx+1:]:
            if "Feedback Poll" in l: break
            data.append(l)
        df = pd.read_csv(io.StringIO("\n".join(data)), index_col=False)
        df.columns = [c.strip() for c in df.columns]
        return df
    except: return None

def analyze_dynamic_columns(df):
    metrics = {"ratings": {}, "nps": {}, "responses": len(df), "json_dist": "{}"}
    dist_storage = {}
    
    time_col = next((c for c in df.columns if "Submitted Date" in c), None)
    time_range_str = ""
    if time_col:
        try:
            df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
            min_t = df[time_col].min().strftime("%H:%M")
            max_t = df[time_col].max().strftime("%H:%M")
            time_range_str = f"Captured: {min_t} - {max_t}"
        except: pass

    for col in df.columns:
        clean = col.lower()
        if any(x in clean for x in ['user', 'email', 'date', 'time', '#']): continue
        num = pd.to_numeric(df[col], errors='coerce')
        if num.notna().sum() > (len(df)*0.1): 
            avg = num.mean()
            if 0 <= avg <= 5: 
                counts = num.value_counts().reindex([5,4,3,2,1], fill_value=0)
                clean_dist = pd.DataFrame({"Rating": counts.index.astype(str), "Count": counts.values})
                metrics["ratings"][col] = {"avg": round(avg, 2), "dist": clean_dist, "time_str": time_range_str}
                if "overall" in clean: key_type = "Overall"
                elif "trainer" in clean: key_type = "Trainer"
                else: key_type = col 
                dist_storage[key_type] = {str(k): int(v) for k, v in counts.items()}
            
            if "recommend" in clean or "friend" in clean:
                if num.max() > 5:
                    prom = (num >= 9).sum()
                    det = (num <= 6).sum()
                    pas = (num == 7).sum() + (num == 8).sum()
                    metrics["nps"][col] = round(((prom - det) / num.notna().sum()) * 100)
                    dist_storage["NPS"] = {"Promoters": int(prom), "Detractors": int(det), "Passives": int(pas)}
                else:
                    prom = (num == 5).sum()
                    det = (num <= 3).sum()
                    metrics["nps"][col] = round(((prom - det) / num.notna().sum()) * 100)
                    dist_storage["NPS"] = {"Promoters": int(prom), "Detractors": int(det)}
    metrics["json_dist"] = json.dumps(dist_storage)
    return metrics

# ─── UI ────────────────────────────────────────────────────────────────────────

tab_upload, tab_list, tab_analytics = st.tabs(["📤 Upload Center", "🔍 Sessions History", "📊 Analysis"])

# ==========================================
# TAB 1: OPS COMMAND CENTER
# ==========================================
with tab_upload:
    status_area = st.empty()

    with st.sidebar:
        st.header("1. Who is uploading?")
        uploader_name = st.text_input("Enter Your full Name *", placeholder="e.g. Yasin Kaif")
        
        st.divider()
        st.header("2. Session's Zoom Attendee & Poll(s)")
        attendee_file = st.file_uploader("Attendee CSV", type=["csv"], key=f"att_{st.session_state.upload_key}")
        poll_files = st.file_uploader("Poll CSV(s)", type=["csv"], accept_multiple_files=True, key=f"poll_{st.session_state.upload_key}")
        
        st.divider()
        st.header("3. Additional Resources if any")
        asset_files = st.file_uploader("Files (PDF, Chat Log)", accept_multiple_files=True, key=f"asset_{st.session_state.upload_key}")
        if asset_files:
            st.caption(f"✅ {len(asset_files)} file(s) attached")
            
        session_links = st.text_area("Any Links/text (Google Docs/sheet URL)", placeholder="Paste links/text here...", height=100)
        
        st.divider()
        btn_disabled = not (uploader_name and attendee_file)
        save_btn = st.button("💾 Save All Data", type="primary", use_container_width=True, disabled=btn_disabled)
        if btn_disabled:
            st.caption("⚠️ Enter Name & Upload Attendee CSV to enable Save.")

    if poll_files and attendee_file:
        stats = parse_attendee_smart(attendee_file)
        if stats["is_simulive"]: st.info("🟣 Detected **Simulive**")
        
        st.subheader("📝 Verify Details")
        c1, c2 = st.columns(2)
        with c1:
            session_date = st.date_input("Date", value=stats["date"])
            trainer = st.text_input("Trainer", value="Simulive Host" if stats["is_simulive"] else stats["trainer"])
        with c2:
            title = st.text_input("Title", value=stats["title"])
            batch = st.text_input("Batch", placeholder="e.g. AI CAP B5")
        
        session_type = st.radio("Type", ["Live", "Simulive"], index=1 if stats["is_simulive"] else 0, horizontal=True)
        
        st.subheader("🛠️ Adjust Metrics (If needed)")
        mc1, mc2, mc3 = st.columns(3)
        duration_val = mc1.number_input("Duration (mins)", value=stats["duration"])
        peak_val = mc2.number_input("Peak Attendees", value=stats["peak"])
        unique_val = mc3.number_input("Unique Users", value=stats["unique"])
        
        all_polls = []
        for p in poll_files:
            p_df = parse_poll_dynamic(p)
            if p_df is not None:
                all_polls.append(p_df)
        
        final_ov_val = 0
        final_tr_val = 0
        final_nps_val = "N/A"
        final_json_dist = "{}"
        total_responses = 0
        
        analyzed_polls = []
        if all_polls:
            df_merged = pd.concat(all_polls, ignore_index=True)
            for p_df in all_polls:
                analyzed_polls.append(analyze_dynamic_columns(p_df))
            
            merged_analysis = analyze_dynamic_columns(df_merged)
            ov_key = next((k for k in merged_analysis["ratings"] if "overall" in k.lower()), None)
            tr_key = next((k for k in merged_analysis["ratings"] if "trainer" in k.lower()), None)
            
            final_ov_val = merged_analysis["ratings"][ov_key]["avg"] if ov_key else 0
            final_tr_val = merged_analysis["ratings"][tr_key]["avg"] if tr_key else 0
            final_nps_val = list(merged_analysis["nps"].values())[0] if merged_analysis["nps"] else "N/A"
            final_json_dist = merged_analysis["json_dist"]
            total_responses = merged_analysis["responses"]

        stickiness = (stats["end_count"] / peak_val) if peak_val > 0 else 0
        trainer_score = (stickiness * final_tr_val) if final_tr_val > 0 else 0

        st.divider()
        sc1, sc2 = st.columns(2)
        sc1.markdown(f"""<div class="score-card"><div class="score-label">Retention Score</div><div class="score-val">{trainer_score:.2f}</div><div class="score-sub">Retained {int(stickiness*100)}% × Rating {final_tr_val}</div></div>""", unsafe_allow_html=True)
        sc2.markdown(f"""<div class="score-card secondary"><div class="score-label">Stickiness Ratio</div><div class="score-val">{int(stickiness*100)}%</div><div class="score-sub">End/Peak %</div></div>""", unsafe_allow_html=True)
        
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Duration", mins_to_hhmm(duration_val))
        m2.metric("Unique Users", unique_val)
        m3.metric("Overall Rating", final_ov_val)
        m4.metric("Trainer Rating", final_tr_val)
        m5.metric("NPS", final_nps_val)
        
        if stats["timeline"] is not None and not stats["timeline"].empty:
            st.subheader("📉 Retention Curve Preview")
            fig = px.area(stats["timeline"], x="Time", y="Attendees", template="plotly_white")
            fig.update_traces(line_color="#9b59b6", fillcolor="rgba(155, 89, 182, 0.2)", line_shape="spline") # Purple
            st.plotly_chart(fig, use_container_width=True)
        
        if analyzed_polls:
            st.subheader("📈 Poll Charts Preview")
            for idx, p_data in enumerate(analyzed_polls):
                if p_data["ratings"]:
                    st.caption(f"**Poll File {idx+1}**")
                    cols = st.columns(len(p_data["ratings"]))
                    for i, (q, m) in enumerate(p_data["ratings"].items()):
                        with cols[i]:
                            t_str = m.get("time_str", "")
                            st.caption(f"{q} \n *{t_str}*")
                            fig_bar = px.bar(m["dist"], x="Rating", y="Count", text="Count", template="plotly_white")
                            fig_bar.update_layout(height=200, margin=dict(l=0, r=0, t=0, b=0))
                            fig_bar.update_traces(marker_color=COLOR_LIVE) # Blue
                            st.plotly_chart(fig_bar, use_container_width=True)
                    st.divider()

        if save_btn:
            with status_area.container():
                status = st.status("🚀 Starting Upload Process...", expanded=True)
                p_bar = status.progress(0)
                
                ws = connect_gsheet()
                if ws:
                    try:
                        status.write("📊 Saving Stats to Google Sheet...")
                        date_str = session_date.strftime("%Y-%m-%d")
                        row = [date_str, trainer, title, batch, duration_val, peak_val, unique_val, int(stats["end_count"]), f"{trainer_score:.2f}", final_ov_val, final_tr_val, total_responses, final_nps_val, session_type, stats["curve_str"], final_json_dist, uploader_name]
                        ws.append_row(row, value_input_option="USER_ENTERED")
                        status.write("✅ Sheet Updated!")
                        
                        # FORCE CLEAR CACHE TO SHOW NEW DATA IMMEDIATELY
                        get_history_df.clear()
                        
                        p_bar.progress(30)
                        
                        all_files_to_upload = [attendee_file] + poll_files + (asset_files if asset_files else [])
                        if session_links.strip():
                            link_file = io.BytesIO(session_links.encode('utf-8'))
                            link_file.name = "Session_Links.txt"
                            link_file.type = "text/plain"
                            all_files_to_upload.append(link_file)
                        
                        folder_link = upload_to_drive_robust(all_files_to_upload, title, date_str, status, p_bar)
                        
                        if folder_link and folder_link != "SKIPPED_QUOTA":
                            status.update(label="✅ Success! Session Saved.", state="complete", expanded=True)
                            st.success(f"**Files Uploaded!** 👉 [**Open Google Drive Folder**]({folder_link})")
                            st.button("🔄 Start New Upload", on_click=reset_app, type="primary")
                        elif folder_link == "SKIPPED_QUOTA":
                            status.update(label="⚠️ Saved (Drive Skipped)", state="complete", expanded=False)
                            st.button("🔄 Start New Upload", on_click=reset_app)
                        else:
                            status.update(label="⚠️ Drive Error", state="error")
                            st.warning("Stats saved, but Drive upload failed.")
                            st.button("🔄 Start New Upload", on_click=reset_app)
                            
                    except Exception as e:
                        status.update(label="❌ Error", state="error")
                        st.error(f"Error: {e}")
    else:
        st.info("👈 Please select files in the sidebar to begin.")

# ==========================================
# TAB 2: INTERACTIVE HISTORY
# ==========================================
with tab_list:
    st.header("🔍 Recent Sessions Registry")
    
    # Optimized Refresh Button (Clears Cache)
    if st.button("🔄 Refresh List", type="primary"):
        get_history_df.clear()
        st.rerun()
    
    # LOAD CACHED DATA
    df = get_history_df().copy()
    
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
        date_col = next((c for c in df.columns if "Date" in c), None)
        title_col = next((c for c in df.columns if "Title" in c or "Session" in c), None)
        trainer_col = next((c for c in df.columns if "Trainer" in c), None)
        batch_col = next((c for c in df.columns if "Batch" in c), None)
        rating_col = next((c for c in df.columns if "Overall" in c), None)
        tr_rating_col = next((c for c in df.columns if "Trainer Rating" in c), None)
        peak_col = next((c for c in df.columns if "Peak" in c), None)
        end_col = next((c for c in df.columns if "End" in c), None)
        dur_col = next((c for c in df.columns if "Duration" in c), None)
        type_col = next((c for c in df.columns if "Type" in c), None)
        curve_col = next((c for c in df.columns if "Curve" in c), None)
        dist_col = next((c for c in df.columns if "Rating" in c and "Dist" in c or "json" in c), None)
        nps_col = next((c for c in df.columns if "NPS" in c), None)
        resp_col = next((c for c in df.columns if "Response" in c or "Count" in c), None)
        
        if date_col and title_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df_disp = df.sort_values(by=date_col, ascending=False).copy()
            
            # Convert mins to hours for display
            if dur_col:
                df_disp["Duration (hrs)"] = (pd.to_numeric(df_disp[dur_col], errors='coerce') / 60).round(1)
            
            # Column Order
            disp_cols = [date_col, trainer_col, title_col, batch_col, "Duration (hrs)", type_col, tr_rating_col, rating_col, nps_col, resp_col, peak_col]
            disp_cols = [c for c in disp_cols if c] # Clean Nones
            
            event = st.dataframe(
                df_disp[disp_cols],
                use_container_width=True,
                hide_index=True,
                selection_mode="single-row",
                on_select="rerun",
                column_config={
                    date_col: st.column_config.DateColumn("Date", format="DD MM YY"),
                    rating_col: st.column_config.ProgressColumn("Rating", format="%.2f", min_value=1, max_value=5),
                }
            )
            
            if event.selection.rows:
                idx = event.selection.rows[0]
                row = df_disp.iloc[idx]
                
                st.divider()
                st.markdown(f"## 📄 {row[title_col]}")
                st.caption(f"📅 {row[date_col].date()} | 👤 {row[trainer_col]} | 🎓 {row[batch_col]}")
                
                peak = row[peak_col] if peak_col else 0
                end = row[end_col] if end_col else 0
                ov_rate = row[rating_col] if rating_col else 0
                tr_rate = row[tr_rating_col] if tr_rating_col else 0
                duration = row[dur_col] if dur_col else 0
                
                stickiness = (end / peak * 100) if peak > 0 else 0
                ret_score = (end/peak * ov_rate) if peak > 0 else 0

                sc1, sc2 = st.columns(2)
                sc1.markdown(f"""<div class="score-card"><div class="score-label">Retention Score</div><div class="score-val">{ret_score:.2f}</div></div>""", unsafe_allow_html=True)
                sc2.markdown(f"""<div class="score-card secondary"><div class="score-label">Stickiness Ratio</div><div class="score-val">{int(stickiness)}%</div><div class="score-sub">End/Peak %</div></div>""", unsafe_allow_html=True)
                
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Overall Rating", f"{ov_rate:.2f}" if ov_rate else "-")
                m2.metric("Trainer Rating", f"{tr_rate:.2f}" if tr_rate else "-")
                m3.metric("Peak", peak)
                m4.metric("Duration", mins_to_hhmm(duration))
                m5.metric("End Count", end)
                
                st.divider()
                
                st.subheader("📉 Retention Curve")
                curve_data = str(row[curve_col]) if curve_col else ""
                if curve_data and "|" in curve_data:
                    try:
                        counts = [float(x) for x in curve_data.split("|")]
                        x_axis = [i * (duration/len(counts)) for i in range(len(counts))]
                        chart_df = pd.DataFrame({"Time (min)": x_axis, "Attendees": counts})
                        fig = px.area(chart_df, x="Time (min)", y="Attendees", template="plotly_white")
                        fig.update_traces(line_color="#9b59b6", fillcolor="rgba(155, 89, 182, 0.2)", line_shape="spline") # Purple Area
                        st.plotly_chart(fig, use_container_width=True)
                    except: st.caption("⚠️ Error parsing curve data.")
                else: st.info("No retention data available.")

                st.subheader("📊 Rating Breakdown")
                dist_json = str(row[dist_col]) if dist_col else ""
                if dist_json and "{" in dist_json:
                    try:
                        d_data = json.loads(dist_json)
                        rc1, rc2, rc3 = st.columns(3)
                        with rc1:
                            if "Overall" in d_data:
                                vals = d_data["Overall"]
                                df_d = pd.DataFrame(list(vals.items()), columns=['Rating', 'Count'])
                                fig_bar = px.bar(df_d, x="Rating", y="Count", text="Count", title="Overall", template="plotly_white")
                                fig_bar.update_traces(marker_color=COLOR_LIVE) # Blue
                                st.plotly_chart(fig_bar, use_container_width=True)
                        with rc2:
                            if "Trainer" in d_data:
                                vals = d_data["Trainer"]
                                df_d = pd.DataFrame(list(vals.items()), columns=['Rating', 'Count'])
                                fig_bar = px.bar(df_d, x="Rating", y="Count", text="Count", title="Trainer", template="plotly_white")
                                fig_bar.update_traces(marker_color=COLOR_SIMULIVE) # Orange
                                st.plotly_chart(fig_bar, use_container_width=True)
                        with rc3:
                            # Robust NPS Key Finder
                            nps_key = next((k for k in d_data.keys() if "recommend" in k.lower() or "nps" in k.lower() or "friend" in k.lower()), None)
                            
                            if nps_key: 
                                if nps_key == "NPS": 
                                    vals = d_data[nps_key]
                                    df_d = pd.DataFrame(list(vals.items()), columns=['Category', 'Count'])
                                    fig_pie = px.pie(df_d, values='Count', names='Category', title="NPS Groups", template="plotly_white", hole=0.4)
                                    st.plotly_chart(fig_pie, use_container_width=True)
                                else:
                                    vals = d_data[nps_key]
                                    df_d = pd.DataFrame(list(vals.items()), columns=['Rating', 'Count'])
                                    fig_bar = px.bar(df_d, x="Rating", y="Count", text="Count", title="NPS", template="plotly_white")
                                    fig_bar.update_traces(marker_color="#e74c3c")
                                    st.plotly_chart(fig_bar, use_container_width=True)
                            else: st.info("No NPS data found.")
                    except Exception as e: st.caption(f"Error parsing details: {e}")

# ==========================================
# TAB 3: EXECUTIVE DASHBOARD
# ==========================================
with tab_analytics:
    st.header("📊 Analysis Dashboard")
    # Optimized Refresh (Clears Cache)
    if st.button("🔄 Refresh Analysis"):
        get_history_df.clear()
        st.rerun()
    
    df = get_history_df().copy()
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
        date_col = next((c for c in df.columns if "Date" in c), None)
        trainer_col = next((c for c in df.columns if "Trainer" in c), None)
        rating_col = next((c for c in df.columns if "Overall" in c), None)
        nps_col = next((c for c in df.columns if "NPS" in c), None)
        type_col = next((c for c in df.columns if "Type" in c), None)
        dur_col = next((c for c in df.columns if "Duration" in c), None)
        title_col = next((c for c in df.columns if "Title" in c), None)
        tr_rating_col = next((c for c in df.columns if "Trainer Rating" in c), None)
        
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.sort_values(by=date_col)
            min_d, max_d = df[date_col].min(), df[date_col].max()
            sel = st.date_input("Filter Date Range", value=(min_d, max_d), key="exec_d")
            if len(sel)==2: df = df[(df[date_col] >= pd.to_datetime(sel[0])) & (df[date_col] <= pd.to_datetime(sel[1]))]
            
            # 1. TRAINER MATRIX
            if trainer_col and rating_col:
                st.markdown("### 🏆 Trainer Performance")
                with st.expander("ℹ️ How to read this chart"):
                    st.caption("""
                    * **X-Axis (Count):** Number of sessions conducted.
                    * **Y-Axis (Avg):** Average Overall Rating.
                    * **Bubble Size:** Volume of feedback received.
                    * **Color (Star %):** Percentage of sessions where the rating was > 4.6 (High Performance).
                    """)
                t_stats = df.groupby(trainer_col).agg(
                    Count=(rating_col,'count'),
                    Avg=(rating_col,'mean'),
                    High=(rating_col, lambda x: (x>4.6).sum())
                ).reset_index()
                t_stats['Star %'] = (t_stats['High']/t_stats['Count']*100).round(1)
                t_stats['Avg'] = t_stats['Avg'].round(2)
                fig_bub = px.scatter(t_stats, x="Count", y="Avg", size="Count", color="Star %", hover_name=trainer_col, 
                                     color_continuous_scale="RdYlGn", size_max=60, template="plotly_white")
                fig_bub.add_hline(y=4.5, line_dash="dot")
                st.plotly_chart(fig_bub, use_container_width=True)
            st.divider()

            # 2. CHRONOLOGICAL PERFORMANCE (Timeline)
            if rating_col and tr_rating_col:
                st.markdown("### 📅 Chronological Weekly Performance Trend")
                chron_perf = df.groupby(date_col)[[rating_col, tr_rating_col]].mean().reset_index()
                chron_melt = chron_perf.melt(id_vars=date_col, value_vars=[rating_col, tr_rating_col], var_name='Metric', value_name='Rating')
                
                fig_trend = px.line(chron_melt, x=date_col, y='Rating', color='Metric', markers=True, 
                                    template="plotly_white", title="Average Rating per Date")
                fig_trend.update_traces(line_width=3)
                st.plotly_chart(fig_trend, use_container_width=True)
            
            st.divider()

            # 3. WEEKLY PERFORMANCE (Day-Wise Bar Chart)
            if type_col and rating_col:
                st.markdown("### 🗓️ Average ofSaturdays/Sundays Live vs Simulive)")
                df['Day'] = df[date_col].dt.day_name()
                days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                df['Day'] = pd.Categorical(df['Day'], categories=days_order, ordered=True)
                
                day_stats = df.groupby(['Day', type_col], observed=True)[rating_col].agg(
                    Average=('mean'),
                    Sessions=('count')
                ).reset_index()
                
                fig_day = px.bar(day_stats, x="Day", y="Average", color=type_col, barmode="group", 
                                 text_auto=".2f",
                                 hover_data={"Average": True, "Sessions": True, "Day": False},
                                 color_discrete_map=COLOR_MAP,
                                 template="plotly_white")
                fig_day.update_traces(marker_line_width=0)
                st.plotly_chart(fig_day, use_container_width=True)
            
            st.divider()

            # 4. LIVE vs SIMULIVE & NPS
            st.markdown("### 📊 Ratings Distribution & Trends")
            with st.expander("ℹ️ Understanding Box Plots"):
                st.caption("The box shows the middle 50% of ratings. The line inside is the median. Dots outside are outliers (unusually high or low ratings).")
                
            c1, c2 = st.columns([2, 1])
            with c1:
                if type_col and rating_col:
                    st.markdown("**🔴 Live vs. 🟣 Simulive Distribution**")
                    fig = px.box(df, x=type_col, y=rating_col, color=type_col, points="all", 
                                 color_discrete_map=COLOR_MAP,
                                 template="plotly_white")
                    st.plotly_chart(fig, use_container_width=True)
            
            with c2:
                if nps_col:
                    st.markdown("**❤️ Weekly NPS Trend**")
                    df[nps_col] = pd.to_numeric(df[nps_col], errors='coerce')
                    nps_daily = df.groupby(date_col)[nps_col].mean().reset_index()
                    fig_nps = px.line(nps_daily, x=date_col, y=nps_col, markers=True, template="plotly_white")
                    fig_nps.update_traces(line_color="#e74c3c", line_width=3) # Red
                    st.plotly_chart(fig_nps, use_container_width=True)
            st.divider()

            # 5. DURATION IMPACT
            if dur_col and rating_col:
                st.markdown("### ⏱️ Session Duration Impact on Rating")
                hover_cols = [title_col] if title_col else []
                fig_s = px.scatter(df, x=dur_col, y=rating_col, color=type_col if type_col else None, 
                                   hover_name=title_col if title_col else None,
                                   hover_data=[rating_col, dur_col],
                                   color_discrete_map=COLOR_MAP,
                                   template="plotly_white", opacity=0.8)
                fig_s.update_traces(marker=dict(size=14, line=dict(width=1, color='DarkSlateGrey')))
                st.plotly_chart(fig_s, use_container_width=True)