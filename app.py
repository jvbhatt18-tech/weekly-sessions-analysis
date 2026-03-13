import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
from datetime import date, datetime
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
    .score-card.secondary { background: linear-gradient(145deg, #E67E22, #D35400); }
    
    .score-val { font-size: 2.2rem; font-weight: 800; margin: 0; }
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
    creds = get_gcp_creds()
    if not creds: return None
    gc = gspread.authorize(creds)
    try: return gc.open_by_key(SHEET_ID).sheet1
    except Exception as e:
        st.error(f"❌ Sheet Connection Error: {e}")
        return None

@st.cache_data(ttl=300)
def get_history_df():
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
        folder = service.files().create(body=file_metadata, fields='id, webViewLink', supportsAllDrives=True).execute()
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
            service.files().create(body=file_meta, media_body=media, fields='id', supportsAllDrives=True).execute()
        return folder_link
    except HttpError as e:
        if "storageQuotaExceeded" in str(e):
            st.warning("⚠️ **Storage Quota Exceeded**: File upload skipped, but Data saved to Sheet.")
            return "SKIPPED_QUOTA"
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
        "timeline": pd.DataFrame(), 
        "end_count_10": 0, "end_count_30": 0,
        "stickiness": 0,
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
                        end_10 = timeline.iloc[-10:] if len(timeline) >= 10 else timeline
                        metrics["end_count_10"] = end_10["Attendees"].mean()
                        end_30 = timeline.iloc[-30:] if len(timeline) >= 30 else timeline
                        metrics["end_count_30"] = end_30["Attendees"].mean()
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
                
                key_type = col
                if "overall" in clean: key_type = "Overall"
                elif "trainer" in clean: key_type = "Trainer"
                
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
# TAB 1: UPLOAD CENTER
# ==========================================
with tab_upload:
    status_area = st.empty()

    with st.sidebar:
        st.header("1. Who is Uploading")
        uploader_name = st.text_input("Enter Your Name *", placeholder="e.g. Yasin Kaif")
        
        st.divider()
        st.header("2. Upload Zoom Reports")
        attendee_file = st.file_uploader("Attendee CSV", type=["csv"], key=f"att_{st.session_state.upload_key}")
        poll_files = st.file_uploader("Poll CSV(s)", type=["csv"], accept_multiple_files=True, key=f"poll_{st.session_state.upload_key}")
        
        st.divider()
        st.header("3. Additional Resources (if any)")
        asset_files = st.file_uploader("Files (PDF, Chat Log)", accept_multiple_files=True, key=f"asset_{st.session_state.upload_key}")
        if asset_files:
            st.caption(f"✅ {len(asset_files)} file(s) attached")
            
        session_links = st.text_area("Important Links (Docs, Recordings)", placeholder="Paste links here...", height=100)
        
        st.divider()
        # Initial check for file presence
        has_files = (attendee_file is not None)
        if not has_files:
            st.caption("👈 Upload files to enable options.")

    if poll_files and attendee_file:
        stats = parse_attendee_smart(attendee_file)
        if stats["is_simulive"]: st.info("🟣 Detected **Simulive**")
        
        st.subheader("📝 Verify Details (Mandatory)")
        c1, c2 = st.columns(2)
        with c1:
            session_date = st.date_input("Date", value=stats["date"])
            trainer = st.text_input("Trainer Name *", value="Simulive Host" if stats["is_simulive"] else stats["trainer"])
        with c2:
            title = st.text_input("Title", value=stats["title"])
            batch = st.text_input("Batch Name *", placeholder="e.g. AI CAP B5")
        
        session_type = st.radio("Type", ["Live", "Simulive"], index=1 if stats["is_simulive"] else 0, horizontal=True)
        
        st.subheader("🛠️ Adjust Metrics")
        mc1, mc2, mc3 = st.columns(3)
        duration_val = mc1.number_input("Duration (mins) *", value=stats["duration"])
        peak_val = mc2.number_input("Peak Attendees", value=stats["peak"])
        unique_val = mc3.number_input("Unique Users", value=stats["unique"])
        
        st.caption("Optional: Set timings to calculate retention before QnA/Project starts.")
        rc1, rc2 = st.columns(2)
        qna_start = rc1.time_input("QnA Start Time", value=None)
        proj_start = rc2.time_input("Project Start Time", value=None)
        
        end_count_10 = 0
        end_count_30 = 0
        timeline_df = stats["timeline"]
        if not timeline_df.empty:
            cutoff_dt = None
            if qna_start or proj_start:
                times = []
                if qna_start: times.append(qna_start)
                if proj_start: times.append(proj_start)
                earliest_cutoff = min(times)
                cutoff_dt = datetime.combine(session_date, earliest_cutoff)
                valid_timeline = timeline_df[timeline_df["Time"] < cutoff_dt]
                if valid_timeline.empty: valid_timeline = timeline_df 
            else:
                valid_timeline = timeline_df
            
            if not valid_timeline.empty:
                end_10 = valid_timeline.iloc[-10:] if len(valid_timeline) >= 10 else valid_timeline
                end_count_10 = end_10["Attendees"].mean()
                end_30 = valid_timeline.iloc[-30:] if len(valid_timeline) >= 30 else valid_timeline
                end_count_30 = end_30["Attendees"].mean()

        analyzed_polls = []
        for p in poll_files:
            p_df = parse_poll_dynamic(p)
            if p_df is not None:
                analyzed_polls.append(analyze_dynamic_columns(p_df))
        
        final_ov_val = 0
        final_tr_val = 0
        final_nps_val = "N/A"
        final_json_dist = "{}"
        total_responses = 0
        
        if analyzed_polls:
            last_data = analyzed_polls[-1]
            ov_key = next((k for k in last_data["ratings"] if "overall" in k.lower()), None)
            tr_key = next((k for k in last_data["ratings"] if "trainer" in k.lower()), None)
            final_ov_val = last_data["ratings"][ov_key]["avg"] if ov_key else 0
            final_tr_val = last_data["ratings"][tr_key]["avg"] if tr_key else 0
            final_nps_val = list(last_data["nps"].values())[0] if last_data["nps"] else "N/A"
            final_json_dist = last_data["json_dist"]
            total_responses = last_data["responses"]

        stickiness_10 = (end_count_10 / peak_val) if peak_val > 0 else 0
        stickiness_30 = (end_count_30 / peak_val) if peak_val > 0 else 0
        trainer_score = (stickiness_10 * final_tr_val) if final_tr_val > 0 else 0

        st.divider()
        sc1, sc2, sc3 = st.columns(3)
        sc1.markdown(f"""<div class="score-card"><div class="score-label">Retention Score</div><div class="score-val">{trainer_score:.2f}</div></div>""", unsafe_allow_html=True)
        sc2.markdown(f"""<div class="score-card secondary"><div class="score-label">Stickiness (10m)</div><div class="score-val">{int(stickiness_10*100)}%</div></div>""", unsafe_allow_html=True)
        sc3.markdown(f"""<div class="score-card secondary" style="background: linear-gradient(145deg, #8E44AD, #9B59B6);"><div class="score-label">Stickiness (30m)</div><div class="score-val">{int(stickiness_30*100)}%</div></div>""", unsafe_allow_html=True)
        
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Duration", mins_to_hhmm(duration_val))
        m2.metric("Unique Users", unique_val)
        m3.metric("Overall Rating", final_ov_val)
        m4.metric("Trainer Rating", final_tr_val)
        m5.metric("NPS", final_nps_val)
        
        if stats["timeline"] is not None and not stats["timeline"].empty:
            st.subheader("📉 Retention Curve Preview")
            fig = px.area(stats["timeline"], x="Time", y="Attendees", template="plotly_white")
            if cutoff_dt:
                fig.add_vline(x=cutoff_dt, line_dash="dash", line_color="red")
                fig.add_annotation(x=cutoff_dt, y=1, yref="paper", text="QnA/Project Start", showarrow=False, font=dict(color="red"))
            fig.update_traces(line_color="#9b59b6", fillcolor="rgba(155, 89, 182, 0.2)", line_shape="spline")
            st.plotly_chart(fig, use_container_width=True)
        
        if analyzed_polls:
            st.subheader("📈 Poll Analysis")
            for idx, p_data in enumerate(analyzed_polls):
                if p_data["ratings"]:
                    st.markdown(f"#### 📊 Poll Report {idx+1}")
                    cols = st.columns(len(p_data["ratings"]))
                    for i, (q, m) in enumerate(p_data["ratings"].items()):
                        with cols[i]:
                            t_str = m.get("time_str", "")
                            st.caption(f"{q} \n *{t_str}*")
                            fig_bar = px.bar(m["dist"], x="Rating", y="Count", text="Count", template="plotly_white")
                            fig_bar.update_layout(height=200, margin=dict(l=0, r=0, t=0, b=0))
                            fig_bar.update_traces(marker_color=COLOR_LIVE)
                            st.plotly_chart(fig_bar, use_container_width=True)
                    st.divider()

        is_form_valid = uploader_name and attendee_file and trainer and batch and (duration_val > 0)
        
        if is_form_valid:
            save_btn = st.button("💾 Save All Data", type="primary", use_container_width=True)
            if save_btn:
                with status_area.container():
                    status = st.status("🚀 Starting Upload Process...", expanded=True)
                    p_bar = status.progress(0)
                    
                    ws = connect_gsheet()
                    if ws:
                        try:
                            status.write("📊 Saving Stats to Google Sheet...")
                            date_str = session_date.strftime("%Y-%m-%d")
                            qna_str = qna_start.strftime("%H:%M") if qna_start else ""
                            proj_str = proj_start.strftime("%H:%M") if proj_start else ""
                            
                            row = [date_str, trainer, title, batch, duration_val, peak_val, unique_val, int(end_count_10), f"{trainer_score:.2f}", final_ov_val, final_tr_val, total_responses, final_nps_val, session_type, stats["curve_str"], final_json_dist, uploader_name, int(stickiness_30*100), qna_str, proj_str]
                            ws.append_row(row, value_input_option="USER_ENTERED")
                            status.write("✅ Sheet Updated!")
                            
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
            st.warning("⚠️ Please enter Trainer Name, Batch, and Duration to enable saving.")
    else:
        st.info("👈 Upload files in the sidebar to start.")

# ==========================================
# TAB 2: SESSIONS HISTORY
# ==========================================
with tab_list:
    st.header("🔍 Recent Sessions Registry")
    if st.button("🔄 Refresh List", type="primary"):
        get_history_df.clear()
        st.rerun()
    
    df = get_history_df().copy()
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
        
        def get_col(candidates):
            for c in candidates:
                found = next((col for col in df.columns if c in col), None)
                if found: return found
            return None

        date_col = get_col(["Date"])
        title_col = get_col(["Title", "Session"])
        trainer_col = get_col(["Trainer"])
        batch_col = get_col(["Batch"])
        rating_col = get_col(["Overall"])
        tr_rating_col = get_col(["Trainer Rating"])
        peak_col = get_col(["Peak"])
        end_col = get_col(["End"])
        dur_col = get_col(["Duration"])
        type_col = get_col(["Type"])
        curve_col = get_col(["Curve"])
        dist_col = get_col(["Rating" and "Dist" or "json"])
        nps_col = get_col(["NPS"])
        resp_col = get_col(["Response", "Count"])
        
        if date_col and title_col:
            # FIX: Filter invalid dates first
            df = df.dropna(subset=[date_col])
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.dropna(subset=[date_col]) # Drop NaT results
            
            df_disp = df.sort_values(by=date_col, ascending=False).copy()
            
            # WEEK CALCULATION (Safe now)
            df_disp["Week Of"] = df_disp[date_col].apply(lambda d: d - pd.Timedelta(days=d.weekday()))
            df_disp["Week Of"] = df_disp["Week Of"].dt.date
            
            if dur_col:
                df_disp["Duration (hrs)"] = (pd.to_numeric(df_disp[dur_col], errors='coerce') / 60).round(1)
            
            df_disp = df_disp.sort_values(by=["Week Of", date_col], ascending=[False, False])
            
            disp_cols = ["Week Of", date_col, trainer_col, title_col, batch_col, "Duration (hrs)", type_col, tr_rating_col, rating_col, nps_col, resp_col, peak_col]
            disp_cols = [c for c in disp_cols if c]
            
            event = st.dataframe(
                df_disp[disp_cols],
                use_container_width=True,
                hide_index=True,
                selection_mode="single-row",
                on_select="rerun",
                column_config={
                    "Week Of": st.column_config.DateColumn("Week Commencing"),
                    date_col: st.column_config.DateColumn("Date", format="DD MM YY"),
                    rating_col: st.column_config.ProgressColumn("Overall Rating", format="%.2f", min_value=1, max_value=5),
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
                duration_mins = row[dur_col] if dur_col else 0
                
                stickiness_10 = (end / peak * 100) if peak > 0 else 0
                ret_score = (end/peak * ov_rate) if peak > 0 else 0
                
                # Stickiness 30 Calculation with Backward Compatibility
                stickiness_30_display = "Data not available"
                if len(row) > 17:
                    try:
                        val = row.iloc[17]
                        if pd.notnull(val) and val != "":
                            stickiness_30_display = f"{int(val)}%"
                    except: pass

                sc1, sc2, sc3 = st.columns(3)
                sc1.markdown(f"""<div class="score-card"><div class="score-label">Retention Score</div><div class="score-val">{ret_score:.2f}</div></div>""", unsafe_allow_html=True)
                sc2.markdown(f"""<div class="score-card secondary"><div class="score-label">Stickiness (10m)</div><div class="score-val">{int(stickiness_10)}%</div></div>""", unsafe_allow_html=True)
                sc3.markdown(f"""<div class="score-card secondary" style="background: linear-gradient(145deg, #8E44AD, #9B59B6);"><div class="score-label">Stickiness (30m)</div><div class="score-val" style="font-size: 1.8rem;">{stickiness_30_display}</div></div>""", unsafe_allow_html=True)
                
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Overall Rating", f"{ov_rate:.2f}" if ov_rate else "-")
                m2.metric("Trainer Rating", f"{tr_rate:.2f}" if tr_rate else "-")
                m3.metric("Peak", peak)
                m4.metric("Duration", mins_to_hhmm(duration_mins))
                m5.metric("End Count", end)
                
                st.divider()
                st.subheader("📉 Retention Curve")
                curve_data = str(row[curve_col]) if curve_col else ""
                if curve_data and "|" in curve_data:
                    try:
                        counts = [float(x) for x in curve_data.split("|")]
                        x_axis = [i * (duration_mins/len(counts)) for i in range(len(counts))]
                        chart_df = pd.DataFrame({"Time (min)": x_axis, "Attendees": counts})
                        fig = px.area(chart_df, x="Time (min)", y="Attendees", template="plotly_white")
                        fig.update_traces(line_color="#9b59b6", fillcolor="rgba(155, 89, 182, 0.2)", line_shape="spline")
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
                                fig_bar.update_traces(marker_color=COLOR_LIVE)
                                st.plotly_chart(fig_bar, use_container_width=True)
                        with rc2:
                            if "Trainer" in d_data:
                                vals = d_data["Trainer"]
                                df_d = pd.DataFrame(list(vals.items()), columns=['Rating', 'Count'])
                                fig_bar = px.bar(df_d, x="Rating", y="Count", text="Count", title="Trainer", template="plotly_white")
                                fig_bar.update_traces(marker_color=COLOR_SIMULIVE)
                                st.plotly_chart(fig_bar, use_container_width=True)
                        with rc3:
                            nps_key = next((k for k in d_data.keys() if "recommend" in k.lower() or "nps" in k.lower() or "friend" in k.lower()), None)
                            if nps_key:
                                vals = d_data[nps_key]
                                if "5" in vals and len(vals) < 4:
                                    df_d = pd.DataFrame(list(vals.items()), columns=['Category', 'Count'])
                                    fig_pie = px.pie(df_d, values='Count', names='Category', title="NPS Groups", template="plotly_white", hole=0.4)
                                    st.plotly_chart(fig_pie, use_container_width=True)
                                else:
                                    df_d = pd.DataFrame(list(vals.items()), columns=['Rating', 'Count'])
                                    fig_bar = px.bar(df_d, x="Rating", y="Count", text="Count", title="NPS Distribution", template="plotly_white")
                                    fig_bar.update_traces(marker_color="#e74c3c")
                                    st.plotly_chart(fig_bar, use_container_width=True)
                            else: st.info("No NPS data found.")
                    except: st.caption("Error parsing details.")

# ==========================================
# TAB 3: ANALYSIS
# ==========================================
with tab_analytics:
    st.header("📊 Analysis")
    if st.button("🔄 Refresh Analysis"):
        get_history_df.clear()
        st.rerun()
    
    df = get_history_df().copy()
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
        # Filter for valid dates first
        date_col = next((c for c in df.columns if "Date" in c), None)
        if date_col:
            df = df.dropna(subset=[date_col])
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.dropna(subset=[date_col])
            df = df.sort_values(by=date_col)
        
        # Helper cols
        trainer_col = next((c for c in df.columns if "Trainer" in c), None)
        rating_col = next((c for c in df.columns if "Overall" in c), None)
        nps_col = next((c for c in df.columns if "NPS" in c), None)
        type_col = next((c for c in df.columns if "Type" in c), None)
        dur_col = next((c for c in df.columns if "Duration" in c), None)
        title_col = next((c for c in df.columns if "Title" in c), None)
        tr_rating_col = next((c for c in df.columns if "Trainer Rating" in c), None)
        
        if date_col:
            min_d, max_d = df[date_col].min(), df[date_col].max()
            sel = st.date_input("Filter Date Range", value=(min_d, max_d), key="exec_d")
            if len(sel)==2: df = df[(df[date_col] >= pd.to_datetime(sel[0])) & (df[date_col] <= pd.to_datetime(sel[1]))]
            
            avg_dur = df[dur_col].mean() if dur_col else 0
            avg_rate = df[rating_col].mean() if rating_col else 0
            
            m1, m2 = st.columns(2)
            m1.metric("Average Session Duration", mins_to_hhmm(avg_dur))
            m2.metric("Average Overall Rating", f"{avg_rate:.2f}")
            st.divider()

            if trainer_col and rating_col:
                st.markdown("### 🏆 Trainer Matrix")
                with st.expander("ℹ️ How to read this chart"):
                    st.caption("Bubble Size = Volume of sessions. Color = Star % (Sessions > 4.6)")
                t_stats = df.groupby(trainer_col).agg(Count=(rating_col,'count'), Avg=(rating_col,'mean'), High=(rating_col, lambda x: (x>4.6).sum())).reset_index()
                t_stats['Star %'] = (t_stats['High']/t_stats['Count']*100).round(1)
                fig_bub = px.scatter(t_stats, x="Count", y="Avg", size="Count", color="Star %", hover_name=trainer_col, color_continuous_scale="RdYlGn", size_max=60, template="plotly_white")
                fig_bub.add_hline(y=4.5, line_dash="dot")
                st.plotly_chart(fig_bub, use_container_width=True)
            st.divider()

            if rating_col and tr_rating_col:
                st.markdown("### 📅 Chronological Performance Trend")
                chron_perf = df.groupby(date_col)[[rating_col, tr_rating_col]].mean().reset_index()
                chron_melt = chron_perf.melt(id_vars=date_col, value_vars=[rating_col, tr_rating_col], var_name='Metric', value_name='Rating')
                fig_trend = px.line(chron_melt, x=date_col, y='Rating', color='Metric', markers=True, template="plotly_white", title="Average Rating per Date")
                fig_trend.update_traces(line_width=3)
                st.plotly_chart(fig_trend, use_container_width=True)
            st.divider()

            if type_col and rating_col:
                st.markdown("### 🗓️ Average of Week Days (Live vs Simulive)")
                df['Day'] = df[date_col].dt.day_name()
                days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                df['Day'] = pd.Categorical(df['Day'], categories=days_order, ordered=True)
                day_stats = df.groupby(['Day', type_col], observed=True)[rating_col].agg(Average=('mean'), Sessions=('count')).reset_index()
                fig_day = px.bar(day_stats, x="Day", y="Average", color=type_col, barmode="group", text_auto=".2f",
                                 hover_data={"Average": True, "Sessions": True, "Day": False}, color_discrete_map=COLOR_MAP, template="plotly_white")
                st.plotly_chart(fig_day, use_container_width=True)
            st.divider()

            c1, c2 = st.columns([2, 1])
            with c1:
                if type_col and rating_col:
                    st.markdown("**🔴 Live vs. 🟣 Simulive Distribution**")
                    fig = px.box(df, x=type_col, y=rating_col, color=type_col, points="all", color_discrete_map=COLOR_MAP, template="plotly_white")
                    st.plotly_chart(fig, use_container_width=True)
            with c2:
                if nps_col:
                    st.markdown("**❤️ Weekly NPS Trend**")
                    df[nps_col] = pd.to_numeric(df[nps_col], errors='coerce')
                    nps_daily = df.groupby(date_col)[nps_col].mean().reset_index()
                    fig_nps = px.line(nps_daily, x=date_col, y=nps_col, markers=True, template="plotly_white")
                    fig_nps.update_traces(line_color="#e74c3c", line_width=3)
                    st.plotly_chart(fig_nps, use_container_width=True)
            st.divider()

            if dur_col and rating_col:
                st.markdown("### ⏱️ Duration vs. Rating Impact")
                if title_col: df["Hover_Title"] = df[title_col]
                else: df["Hover_Title"] = "Session"
                fig_s = px.scatter(df, x=dur_col, y=rating_col, color=type_col if type_col else None, 
                                   hover_name="Hover_Title", hover_data=[rating_col, dur_col],
                                   color_discrete_map=COLOR_MAP, template="plotly_white", opacity=0.8)
                fig_s.update_traces(marker=dict(size=14, line=dict(width=1, color='DarkSlateGrey')))
                st.plotly_chart(fig_s, use_container_width=True)