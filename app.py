import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date
import io
import plotly.express as px

# ─── 1. CONFIGURATION ───
SHEET_ID = "1jYRJe9APAlIZdMQ9svuOo9gR1DbYfrCUjThvtO1DXcI"

st.set_page_config(page_title="Weekly Sessions Automated", page_icon="📊", layout="wide")

# ─── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background-color: #f8f9fa; }
    [data-testid="stSidebar"] { background-color: #ffffff; border-right: 1px solid #e0e0e0; }
    div[data-testid="stMetric"] { background-color: #ffffff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 15px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); text-align: center; }
    .score-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 12px; padding: 20px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 10px; }
    .score-val { font-size: 2.2rem; font-weight: 800; margin: 0; }
    .score-label { font-size: 0.85rem; opacity: 0.9; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }
    .score-sub { font-size: 0.8rem; opacity: 0.8; margin-top: 5px; }
</style>
""", unsafe_allow_html=True)

# ─── CONNECTIONS ───
def mins_to_hhmm(minutes):
    try:
        m = int(minutes)
        return f"{m // 60}h {m % 60:02d}m"
    except: return "0h 00m"

# REMOVED CACHE to prevent "Stuck" issues
def connect_gsheet():
    if "gcp_service_account" not in st.secrets: 
        st.error("❌ Secrets not found in .streamlit/secrets.toml")
        return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    gc = gspread.authorize(creds)
    try: return gc.open_by_key(SHEET_ID).sheet1
    except Exception as e:
        st.error(f"❌ Connection Failed: {e}")
        return None

def get_history_df():
    with st.spinner("🔄 Fetching data from Google Sheets..."):
        ws = connect_gsheet()
        if not ws: return pd.DataFrame()
        data = ws.get_all_records()
        return pd.DataFrame(data)

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

def parse_attendee_smart(uploaded_file):
    metrics = {"trainer": "Unknown", "duration": 0, "peak": 0, "unique": 0, "title": "Unknown", "date": date.today(), "timeline": pd.DataFrame(), "end_count": 0, "stickiness": 0}
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
        p_start, a_start = -1, -1
        for i, line in enumerate(lines):
            if "Panelist Details" in line: p_start = i
            if "Attendee Details" in line: a_start = i
        
        # Panelist
        if p_start != -1:
            chunk = lines[p_start+1:a_start if a_start!=-1 else len(lines)]
            p_head = next((j for j, l in enumerate(chunk) if "User Name" in l and "Join Time" in l), -1)
            if p_head != -1:
                df_p = pd.read_csv(io.StringIO("\n".join(chunk[p_head:])), index_col=False)
                name, join, leave = [next((c for c in df_p.columns if k in c), None) for k in ["User Name", "Join Time", "Leave Time"]]
                if name and join and leave:
                    df_p = df_p[~df_p[name].astype(str).str.lower().str.contains('team be10x|host|notetaker|admin')]
                    df_p[join], df_p[leave] = pd.to_datetime(df_p[join], errors='coerce'), pd.to_datetime(df_p[leave], errors='coerce')
                    df_p.dropna(subset=[join, leave], inplace=True)
                    stats = [(p, calculate_precise_duration(list(zip(g[join], g[leave])))) for p, g in df_p.groupby(name)]
                    if stats:
                        stats.sort(key=lambda x: x[1], reverse=True)
                        metrics["trainer"], metrics["duration"] = stats[0]
        
        # Attendees
        if a_start != -1:
            chunk = lines[a_start+1:]
            a_head = next((j for j, l in enumerate(chunk) if "User Name" in l and "Email" in l), -1)
            if a_head != -1:
                df_a = pd.read_csv(io.StringIO("\n".join(chunk[a_head:])), index_col=False)
                email, join, leave = [next((c for c in df_a.columns if k in c), None) for k in ["Email", "Join Time", "Leave Time"]]
                if email: metrics["unique"] = df_a[email].astype(str).str.strip().str.lower().nunique()
                if join and leave:
                    df_a[join], df_a[leave] = pd.to_datetime(df_a[join], errors='coerce'), pd.to_datetime(df_a[leave], errors='coerce')
                    df_a.dropna(subset=[join, leave], inplace=True)
                    timeline, peak = generate_retention_curve(df_a, join, leave)
                    metrics["peak"], metrics["timeline"] = peak, timeline
                    if not timeline.empty:
                        metrics["end_count"] = timeline.iloc[-16:-1]["Attendees"].mean() if len(timeline)>15 else timeline.iloc[-1]["Attendees"]
                        metrics["stickiness"] = (timeline.iloc[len(timeline)//2]["Attendees"] / peak) if peak > 0 else 0
    except: pass
    return metrics

def parse_poll_dynamic(uploaded_file):
    try:
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
    metrics = {"ratings": {}, "nps": {}, "responses": len(df)}
    for col in df.columns:
        clean = col.lower()
        if any(x in clean for x in ['user', 'email', 'date', 'time', '#']): continue
        num = pd.to_numeric(df[col], errors='coerce')
        if num.notna().sum() > (len(df)*0.1):
            avg = num.mean()
            if 0<=avg<=5: metrics["ratings"][col] = {"avg": round(avg, 2), "dist": num.value_counts().reindex([5,4,3,2,1], fill_value=0)}
            if "recommend" in clean or "friend" in clean:
                prom, det = ((num>=9).sum(), (num<=6).sum()) if num.max()>5 else ((num==5).sum(), (num<=3).sum())
                metrics["nps"][col] = round(((prom-det)/num.notna().sum())*100)
    return metrics

# ─── UI STRUCTURE ───
tab_upload, tab_analytics, tab_retention = st.tabs(["📤 Upload Session", "📊 Executive Dashboard", "📉 Deep Retention"])

# ==========================================
# TAB 1: UPLOAD
# ==========================================
with tab_upload:
    with st.sidebar:
        st.header("1. Upload")
        attendee_file = st.file_uploader("Attendee CSV", type=["csv"])
        poll_files = st.file_uploader("Poll CSV(s)", type=["csv"], accept_multiple_files=True)
        st.markdown("---")
        st.header("2. Verify")
        stats = {"trainer": "Unknown", "duration": 0, "peak": 0, "unique": 0, "date": date.today(), "title": "Session", "end_count": 0, "timeline": None, "stickiness": 0}
        
        if attendee_file:
            stats = parse_attendee_smart(attendee_file)
            if stats["trainer"] != "Unknown": st.success(f"✅ Found: {stats['trainer']}")
        
        session_date = st.date_input("Date", value=stats["date"])
        trainer = st.text_input("Trainer", value=stats["trainer"])
        batch = st.text_input("Batch", placeholder="e.g. AI CAP B5")
        title = st.text_input("Title", value=stats["title"])
        session_type = st.radio("Type", ["Live", "Simulive"], horizontal=True)
        
        c1, c2 = st.columns(2)
        duration = c1.number_input("Dur (m)", value=stats["duration"])
        peak = c2.number_input("Peak", value=stats["peak"])
        unique = st.number_input("Unique", value=stats["unique"])
        
        st.markdown("---")
        save_btn = st.button("💾 Save to Cloud", type="primary", use_container_width=True)

    if poll_files and attendee_file:
        st.markdown(f"## 🗓️ {session_date} | 👤 {trainer} | 🔴 {session_type}")
        st.markdown(f"**{title}**")
        st.divider()

        poll_files.sort(key=lambda x: x.name)
        df_poll = parse_poll_dynamic(poll_files[-1])
        
        if df_poll is not None:
            data = analyze_dynamic_columns(df_poll)
            ov_key = next((k for k in data["ratings"] if "overall" in k.lower()), None)
            tr_key = next((k for k in data["ratings"] if "trainer" in k.lower()), None)
            ov_val = data["ratings"][ov_key]["avg"] if ov_key else 0
            tr_val = data["ratings"][tr_key]["avg"] if tr_key else 0
            nps_val = list(data["nps"].values())[0] if data["nps"] else "N/A"
            trainer_score, ret_rate = 0.0, 0.0
            if peak > 0 and tr_val > 0:
                ret_rate = (stats["end_count"] / peak)
                trainer_score = ret_rate * tr_val

            sc1, sc2 = st.columns(2)
            sc1.markdown(f"""<div class="score-card"><div class="score-label">Retention Score</div><div class="score-val">{trainer_score:.2f}</div><div class="score-sub">Retained {int(ret_rate*100)}% × Rating {tr_val}</div></div>""", unsafe_allow_html=True)
            sc2.markdown(f"""<div class="score-card" style="background: linear-gradient(135deg, #FF9966 0%, #FF5E62 100%);"><div class="score-label">Stickiness Ratio</div><div class="score-val">{int(stats["stickiness"]*100)}%</div><div class="score-sub">Audience remaining at half-time</div></div>""", unsafe_allow_html=True)
            st.write("")
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Duration", mins_to_hhmm(duration), help="Total active time.")
            m2.metric("Unique Users", unique, help="Count of distinct email addresses.")
            m3.metric("Overall Rating", ov_val, help="Overall Session Feedback.")
            m4.metric("Trainer Rating", tr_val, help="Trainer Feedback.")
            m5.metric("NPS", nps_val, help="Net Promoter Score.")
            st.divider()
            
            if stats["timeline"] is not None and not stats["timeline"].empty:
                st.subheader("📉 Retention Curve")
                fig = px.area(stats["timeline"], x="Time", y="Attendees", template="plotly_white")
                fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20), hovermode="x unified")
                fig.update_traces(line_color="#764ba2", fillcolor="rgba(118, 75, 162, 0.2)")
                st.plotly_chart(fig, use_container_width=True)
            
            if data["ratings"]:
                st.divider()
                st.subheader("📈 Ratings Breakdown")
                items = list(data["ratings"].items())
                rows = (len(items) + 2) // 3
                for row_idx in range(rows):
                    cols = st.columns(3)
                    for col_idx in range(3):
                        idx = row_idx * 3 + col_idx
                        if idx < len(items):
                            q, m = items[idx]
                            with cols[col_idx]:
                                with st.container():
                                    st.caption(f"{q[:45]}...")
                                    st.bar_chart(m["dist"], height=200, color="#4a90e2")

            if save_btn:
                ws = connect_gsheet()
                if ws:
                    try:
                        date_str = session_date.strftime("%Y-%m-%d")
                        row = [date_str, trainer, title, batch, duration, peak, unique, int(stats["end_count"]), f"{trainer_score:.2f}", ov_val, tr_val, data["responses"], nps_val, session_type]
                        ws.append_row(row, value_input_option="USER_ENTERED")
                        st.toast("✅ Saved!", icon="🎉")
                    except Exception as e: st.error(f"Error: {e}")
    else: st.info("👋 Go to Sidebar to Upload.")

# ==========================================
# TAB 2: ANALYTICS (EXECUTIVE)
# ==========================================
with tab_analytics:
    st.header("📊 Executive Dashboard")
    
    # Init State
    if "exec_df" not in st.session_state: st.session_state.exec_df = pd.DataFrame()

    if st.button("🔄 Refresh Data", key="refresh_exec", type="primary") or not st.session_state.exec_df.empty:
        if st.session_state.exec_df.empty: st.session_state.exec_df = get_history_df()
        
        df = st.session_state.exec_df.copy()
        
        if not df.empty:
            df.columns = [str(c).strip() for c in df.columns]
            date_col = next((c for c in df.columns if "Date" in c), None)
            trainer_col = next((c for c in df.columns if "Trainer" in c), None)
            rating_col = next((c for c in df.columns if "Overall" in c), None)
            dur_col = next((c for c in df.columns if "Duration" in c), None)
            resp_col = next((c for c in df.columns if "Responses" in c), None)
            type_col = next((c for c in df.columns if "Type" in c), None)

            if date_col and rating_col:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                df[rating_col] = pd.to_numeric(df[rating_col], errors='coerce')
                if dur_col: df[dur_col] = pd.to_numeric(df[dur_col], errors='coerce')
                if resp_col: df[resp_col] = pd.to_numeric(df[resp_col], errors='coerce').fillna(0)
                df = df.dropna(subset=[date_col]).sort_values(by=date_col)

                # DATE FILTER
                min_date, max_date = df[date_col].min(), df[date_col].max()
                sel_dates = st.date_input("Filter Date Range", value=(min_date, max_date), key="exec_dates")
                
                if len(sel_dates) == 2:
                    start_d, end_d = pd.to_datetime(sel_dates[0]), pd.to_datetime(sel_dates[1])
                    df_filt = df[(df[date_col] >= start_d) & (df[date_col] <= end_d)].copy()
                else:
                    df_filt = df.copy()

                if df_filt.empty:
                    st.warning("No data in selected date range.")
                else:
                    # LIVE vs SIMULIVE
                    st.markdown("### 🔴 Live vs. 🟣 Simulive Gap")
                    if type_col:
                        live_avg = df_filt[df_filt[type_col].str.lower() == 'live'][rating_col].mean()
                        sim_avg = df_filt[df_filt[type_col].str.lower() == 'simulive'][rating_col].mean()
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Live Avg Rating", f"{live_avg:.2f}" if pd.notna(live_avg) else "N/A")
                        c2.metric("Simulive Avg Rating", f"{sim_avg:.2f}" if pd.notna(sim_avg) else "N/A")
                        c3.metric("The Gap", f"{(live_avg - sim_avg):.2f}" if pd.notna(live_avg) and pd.notna(sim_avg) else "0.00")
                        fig_box = px.box(df_filt, x=type_col, y=rating_col, color=type_col, points="all", template="plotly_white", title="Rating Distribution")
                        st.plotly_chart(fig_box, use_container_width=True)
                    st.divider()

                    # TRAINER BUBBLE
                    st.markdown("### 🏆 Trainer Performance Matrix")
                    if trainer_col:
                        t_stats = df_filt.groupby(trainer_col).agg(
                            Sessions=(rating_col, 'count'),
                            Avg_Rating=(rating_col, 'mean'),
                            Responses=(resp_col, 'sum') if resp_col else (rating_col, 'count'),
                            High_Rated=(rating_col, lambda x: (x > 4.6).sum())
                        ).reset_index()
                        t_stats['Consistency %'] = (t_stats['High_Rated'] / t_stats['Sessions'] * 100).round(1)
                        t_stats['Avg_Rating'] = t_stats['Avg_Rating'].round(2)
                        
                        fig_bub = px.scatter(t_stats, x="Sessions", y="Avg_Rating", size="Responses", color="Consistency %",
                                             hover_name=trainer_col, color_continuous_scale="RdYlGn", size_max=60, template="plotly_white")
                        fig_bub.add_hline(y=4.5, line_dash="dot", annotation_text="Target 4.5")
                        st.plotly_chart(fig_bub, use_container_width=True)
                    st.divider()

                    # TRENDS
                    c_line, c_dur = st.columns(2)
                    with c_line:
                        st.markdown("#### 📅 Rating Trend")
                        daily = df_filt.groupby(date_col)[rating_col].mean().reset_index()
                        fig_line = px.line(daily, x=date_col, y=rating_col, markers=True, template="plotly_white")
                        fig_line.update_traces(line_color="#764ba2")
                        st.plotly_chart(fig_line, use_container_width=True)
                    with c_dur:
                        st.markdown("#### ⏱️ Duration vs. Rating")
                        if dur_col:
                            fig_sc = px.scatter(df_filt, x=dur_col, y=rating_col, color=rating_col, 
                                                range_color=[3, 5], color_continuous_scale="RdYlGn", template="plotly_white")
                            st.plotly_chart(fig_sc, use_container_width=True)

# ==========================================
# TAB 3: DEEP RETENTION LAB
# ==========================================
with tab_retention:
    st.header("📉 Retention Lab")
    
    if "ret_df" not in st.session_state: st.session_state.ret_df = pd.DataFrame()

    if st.button("🔄 Analyze Retention", key="refresh_retention", type="primary") or not st.session_state.ret_df.empty:
        if st.session_state.ret_df.empty: st.session_state.ret_df = get_history_df()
        
        df = st.session_state.ret_df.copy()

        if not df.empty:
            df.columns = [str(c).strip() for c in df.columns]
            peak_col = next((c for c in df.columns if "Peak" in c), None)
            end_col = next((c for c in df.columns if "End" in c), None)
            date_col = next((c for c in df.columns if "Date" in c), None)
            batch_col = next((c for c in df.columns if "Batch" in c), None)

            if peak_col and end_col and date_col:
                # Cleanup
                df[peak_col] = pd.to_numeric(df[peak_col], errors='coerce').fillna(0)
                df[end_col] = pd.to_numeric(df[end_col], errors='coerce').fillna(0)
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                
                # DATE FILTER (Global)
                min_date, max_date = df[date_col].min(), df[date_col].max()
                sel_dates = st.date_input("Filter Date Range", value=(min_date, max_date), key="ret_dates")
                
                if len(sel_dates) == 2:
                    start_d, end_d = pd.to_datetime(sel_dates[0]), pd.to_datetime(sel_dates[1])
                    df = df[(df[date_col] >= start_d) & (df[date_col] <= end_d)].copy()
                
                # FILTER: Valid data only
                df_ret = df[df[peak_col] > 0].copy()
                df_ret = df_ret.sort_values(by=date_col)
                
                if df_ret.empty:
                    st.warning("⚠️ No retention data found in selected range.")
                else:
                    # BATCH DECAY
                    st.markdown("### 🏚️ Batch Attendance Decay")
                    st.caption("Attendance as % of the First Session in the filtered range.")
                    if batch_col:
                        sel_batch = st.selectbox("Select Batch", df_ret[batch_col].unique())
                        b_df = df_ret[df_ret[batch_col] == sel_batch].sort_values(by=date_col)
                        
                        if not b_df.empty:
                            baseline = b_df.iloc[0][peak_col]
                            if baseline > 0:
                                b_df['Relative_Retention'] = (b_df[peak_col] / baseline) * 100
                            else:
                                b_df['Relative_Retention'] = 0
                            b_df['Session_Num'] = range(1, len(b_df) + 1)
                            
                            fig_dec = px.line(b_df, x='Session_Num', y='Relative_Retention', markers=True, title=f"Decay Curve: {sel_batch}",
                                              labels={'Relative_Retention': '% of Session 1'}, template="plotly_white")
                            fig_dec.update_traces(line_color="#e74c3c")
                            fig_dec.update_yaxes(range=[0, 110])
                            fig_dec.update_traces(hovertemplate='Session %{x}<br>Retention: %{y:.1f}%<br>Raw Attendees: %{customdata}')
                            fig_dec.data[0].customdata = b_df[peak_col]
                            st.plotly_chart(fig_dec, use_container_width=True)
                    st.divider()

                    # STICKINESS TREND
                    df_stick = df_ret[df_ret[end_col] > 0].copy()
                    if not df_stick.empty:
                        df_stick['Stickiness'] = (df_stick[end_col] / df_stick[peak_col]) * 100
                        st.markdown("### 🧲 Stickiness Trend (End/Peak %)")
                        fig_stick = px.line(df_stick, x=date_col, y="Stickiness", markers=True, title="Stickiness % Over Time", template="plotly_white")
                        fig_stick.update_traces(line_color="#27ae60") 
                        st.plotly_chart(fig_stick, use_container_width=True)
                    else: st.info("No 'End Count' data available for Stickiness analysis yet.")
        else: st.warning("No data found.")