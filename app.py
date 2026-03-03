import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date
import io
import plotly.express as px
import json

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
    .stDataFrame { background-color: white; border-radius: 10px; padding: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
</style>
""", unsafe_allow_html=True)

# ─── CONNECTIONS ───
def mins_to_hhmm(minutes):
    try:
        m = int(minutes)
        return f"{m // 60}h {m % 60:02d}m"
    except: return "0h 00m"

def connect_gsheet():
    if "gcp_service_account" not in st.secrets: 
        st.error("❌ Secrets not found.")
        return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    gc = gspread.authorize(creds)
    try: return gc.open_by_key(SHEET_ID).sheet1
    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        return None

def get_history_df():
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
        
        # Metadata
        for line in lines[:5]:
            if "Topic" in line and "Start Time" in line:
                try:
                    row = next(pd.read_csv(io.StringIO(lines[lines.index(line)+1]), header=None).iterrows())[1]
                    metrics["title"] = str(row[0]).strip()
                    metrics["date"] = pd.to_datetime(str(row[2]).split()[0]).date()
                except: pass
                break
        
        # Simulive Detection
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

        # Sections
        p_start, a_start = -1, -1
        for i, line in enumerate(lines):
            if "Panelist Details" in line: p_start = i
            if "Attendee Details" in line: a_start = i
        
        # Trainer
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
        
        # Attendees
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
    metrics = {"ratings": {}, "nps": {}, "responses": len(df), "json_dist": "{}"}
    dist_storage = {}
    for col in df.columns:
        clean = col.lower()
        if any(x in clean for x in ['user', 'email', 'date', 'time', '#']): continue
        num = pd.to_numeric(df[col], errors='coerce')
        if num.notna().sum() > (len(df)*0.1):
            avg = num.mean()
            if 0<=avg<=5: 
                counts = num.value_counts().reindex([5,4,3,2,1], fill_value=0)
                metrics["ratings"][col] = {"avg": round(avg, 2), "dist": counts}
                key_type = "Overall" if "overall" in clean else "Trainer" if "trainer" in clean else col
                dist_storage[key_type] = counts.to_dict()
            if "recommend" in clean or "friend" in clean:
                prom, det = ((num>=9).sum(), (num<=6).sum()) if num.max()>5 else ((num==5).sum(), (num<=3).sum())
                metrics["nps"][col] = round(((prom-det)/num.notna().sum())*100)
                if num.max() > 5:
                    dist_storage["NPS"] = {"Promoters": int(prom), "Detractors": int(det), "Passives": int(num.notna().sum() - prom - det)}
    metrics["json_dist"] = json.dumps(dist_storage)
    return metrics

# ─── UI ────────────────────────────────────────────────────────────────────────

tab_upload, tab_list, tab_analytics, tab_retention = st.tabs(["📤 Upload Session", "🔍 Recent Sessions", "📊 Dashboard", "📉 Retention Lab"])

# ==========================================
# TAB 1: UPLOAD
# ==========================================
with tab_upload:
    if "upload_key" not in st.session_state: st.session_state.upload_key = 0

    with st.sidebar:
        st.header("1. Upload")
        attendee_file = st.file_uploader("Attendee CSV", type=["csv"], key=f"att_{st.session_state.upload_key}")
        poll_files = st.file_uploader("Poll CSV(s)", type=["csv"], accept_multiple_files=True, key=f"poll_{st.session_state.upload_key}")
        
        st.markdown("---")
        st.header("2. Verify")
        stats = {"trainer": "Unknown", "duration": 0, "peak": 0, "unique": 0, "date": date.today(), "title": "Session", "end_count": 0, "stickiness": 0, "is_simulive": False, "timeline": None, "curve_str": ""}
        if attendee_file:
            stats = parse_attendee_smart(attendee_file)
            if stats["is_simulive"]: st.info("🟣 Detected **Simulive**")
            elif stats["trainer"] != "Unknown": st.success(f"✅ Found: {stats['trainer']}")
        
        session_date = st.date_input("Date", value=stats["date"])
        trainer_val = "Simulive Host" if stats["is_simulive"] else stats["trainer"]
        trainer = st.text_input("Trainer", value=trainer_val)
        batch = st.text_input("Batch", placeholder="e.g. AI CAP B5")
        title = st.text_input("Title", value=stats["title"])
        session_type = st.radio("Type", ["Live", "Simulive"], index=1 if stats["is_simulive"] else 0, horizontal=True)
        c1, c2 = st.columns(2)
        duration = c1.number_input("Dur (m)", value=stats["duration"])
        peak = c2.number_input("Peak", value=stats["peak"])
        unique = st.number_input("Unique", value=stats["unique"])
        st.markdown("---")
        save_btn = st.button("💾 Save to Cloud", type="primary", use_container_width=True)

    if poll_files and attendee_file:
        st.markdown(f"## 🗓️ {session_date} | 👤 {trainer} | {'🟣' if session_type=='Simulive' else '🔴'} {session_type}")
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
            m1.metric("Duration", mins_to_hhmm(duration))
            m2.metric("Unique Users", unique)
            m3.metric("Overall Rating", ov_val)
            m4.metric("Trainer Rating", tr_val)
            m5.metric("NPS", nps_val)
            st.divider()
            if stats["timeline"] is not None and not stats["timeline"].empty:
                st.subheader("📉 Retention Curve")
                fig = px.area(stats["timeline"], x="Time", y="Attendees", template="plotly_white")
                fig.update_traces(line_color="#764ba2", fillcolor="rgba(118, 75, 162, 0.2)", line_shape="spline")
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
                        row = [date_str, trainer, title, batch, duration, peak, unique, int(stats["end_count"]), f"{trainer_score:.2f}", ov_val, tr_val, data["responses"], nps_val, session_type, stats["curve_str"], data["json_dist"]]
                        ws.append_row(row, value_input_option="USER_ENTERED")
                        st.toast("✅ Saved!", icon="🎉")
                        st.session_state.upload_key += 1
                        st.rerun()
                    except Exception as e: st.error(f"Error: {e}")
    else: st.info("👋 Go to Sidebar to Upload.")

# ==========================================
# TAB 2: INTERACTIVE HISTORY
# ==========================================
with tab_list:
    st.header("🔍 Recent Sessions Registry")
    if st.button("🔄 Refresh List", type="primary"): st.session_state.pop('hist_df', None)
    
    if 'hist_df' not in st.session_state or st.session_state.hist_df.empty:
        st.session_state.hist_df = get_history_df()
    
    df = st.session_state.hist_df.copy()
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
        
        if date_col and title_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df_disp = df.sort_values(by=date_col, ascending=False).copy()
            
            table_cols = [date_col, trainer_col, title_col, batch_col, rating_col, type_col]
            table_cols = [c for c in table_cols if c]
            
            selection = st.dataframe(
                df_disp[table_cols],
                use_container_width=True,
                hide_index=True,
                selection_mode="single-row",
                on_select="rerun",
                column_config={
                    date_col: st.column_config.DateColumn("Date", format="DD MM YY"),
                    rating_col: st.column_config.ProgressColumn("Rating", format="%.2f", min_value=1, max_value=5),
                    type_col: st.column_config.TextColumn("Type", width="small")
                }
            )
            
            if selection.selection.rows:
                sel_idx = selection.selection.rows[0]
                row = df_disp.iloc[sel_idx]
                
                st.divider()
                st.markdown(f"## 📄 {row[title_col]}")
                st.caption(f"📅 {row[date_col].date()} | 👤 {row[trainer_col]} | 🎓 {row[batch_col]}")
                
                def safe_get(row, c, default=0):
                    return row[c] if c and c in row and pd.notnull(row[c]) else default

                peak = safe_get(row, peak_col)
                end = safe_get(row, end_col)
                ov_rate = safe_get(row, rating_col)
                tr_rate = safe_get(row, tr_rating_col)
                duration = safe_get(row, dur_col)
                
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Overall Rating", f"{ov_rate:.2f}" if ov_rate else "-")
                m2.metric("Trainer Rating", f"{tr_rate:.2f}" if tr_rate else "-")
                m3.metric("Peak", peak)
                m4.metric("Duration", mins_to_hhmm(duration))
                m5.metric("End Count", end)
                
                st.divider()
                st.subheader("📉 Retention Curve")
                
                curve_data = None
                if curve_col: curve_data = str(row[curve_col])
                elif len(row) > 14: curve_data = str(row.iloc[14])
                
                if curve_data and "|" in curve_data:
                    try:
                        counts = [float(x) for x in curve_data.split("|")]
                        x_axis = [i * (duration/len(counts)) for i in range(len(counts))]
                        chart_df = pd.DataFrame({"Time (min)": x_axis, "Attendees": counts})
                        fig = px.area(chart_df, x="Time (min)", y="Attendees", template="plotly_white")
                        fig.update_traces(line_color="#764ba2", fillcolor="rgba(118, 75, 162, 0.2)", line_shape="spline")
                        st.plotly_chart(fig, use_container_width=True)
                    except: st.caption("⚠️ Error parsing retention data.")
                elif peak > 0:
                    st.caption("ℹ️ Using Linear Approximation (Historical Data)")
                    sim_data = pd.DataFrame({"Progress": ["Start", "End"], "Attendees": [peak, end]})
                    fig = px.area(sim_data, x="Progress", y="Attendees", template="plotly_white")
                    fig.update_traces(line_color="#95a5a6", fillcolor="rgba(149, 165, 166, 0.2)")
                    st.plotly_chart(fig, use_container_width=True)
                
                st.subheader("📊 Rating Distributions")
                dist_json = None
                if dist_col: dist_json = str(row[dist_col])
                elif len(row) > 15: dist_json = str(row.iloc[15])
                
                if dist_json and "{" in dist_json:
                    try:
                        d_data = json.loads(dist_json)
                        cols = st.columns(3)
                        i = 0
                        for category, values in d_data.items():
                            if i < 3:
                                with cols[i]:
                                    st.caption(f"{category}")
                                    df_d = pd.DataFrame.from_dict(values, orient='index', columns=['Count'])
                                    st.bar_chart(df_d)
                                i += 1
                    except: st.caption("No detail data.")
                else: st.caption("ℹ️ Detailed rating counts not saved.")

# ==========================================
# TAB 3: EXECUTIVE DASHBOARD
# ==========================================
with tab_analytics:
    st.header("📊 Executive Dashboard")
    if st.button("🔄 Refresh Data", key="refresh_exec", type="primary"): st.session_state.pop('exec_df', None)
    
    if 'exec_df' not in st.session_state or st.session_state.exec_df.empty:
        st.session_state.exec_df = get_history_df()
    
    df = st.session_state.exec_df.copy()
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
        date_col = next((c for c in df.columns if "Date" in c), None)
        trainer_col = next((c for c in df.columns if "Trainer" in c), None)
        rating_col = next((c for c in df.columns if "Overall" in c), None)
        type_col = next((c for c in df.columns if "Type" in c), None)
        batch_col = next((c for c in df.columns if "Batch" in c), None)
        
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.sort_values(by=date_col)
            min_d, max_d = df[date_col].min(), df[date_col].max()
            sel = st.date_input("Filter Date Range", value=(min_d, max_d), key="exec_d")
            if len(sel)==2: df = df[(df[date_col] >= pd.to_datetime(sel[0])) & (df[date_col] <= pd.to_datetime(sel[1]))]
            
            st.markdown("### 🎓 Batch Health Heatmap")
            if batch_col and rating_col:
                df['Month'] = df[date_col].dt.strftime('%Y-%m')
                pivot = df.pivot_table(index=batch_col, columns='Month', values=rating_col, aggfunc='mean')
                fig_h = px.imshow(pivot, text_auto=".1f", aspect="auto", color_continuous_scale="RdYlGn", origin='lower')
                st.plotly_chart(fig_h, use_container_width=True)
            st.divider()

            if type_col and rating_col:
                st.markdown("### 🔴 Live vs. 🟣 Simulive")
                df['Type_Norm'] = df[type_col].astype(str).str.lower()
                live_avg = df[df['Type_Norm'] == 'live'][rating_col].mean()
                sim_avg = df[df['Type_Norm'] == 'simulive'][rating_col].mean()
                c1, c2, c3 = st.columns(3)
                c1.metric("Live Avg", f"{live_avg:.2f}" if pd.notna(live_avg) else "-")
                c2.metric("Simulive Avg", f"{sim_avg:.2f}" if pd.notna(sim_avg) else "-")
                c3.metric("Gap", f"{(live_avg - sim_avg):.2f}" if pd.notna(live_avg) and pd.notna(sim_avg) else "-")
                fig = px.box(df, x=type_col, y=rating_col, color=type_col, points="all", template="plotly_white")
                st.plotly_chart(fig, use_container_width=True)
            st.divider()
            
            if trainer_col and rating_col:
                st.markdown("### 🏆 Trainer Matrix")
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

# ==========================================
# TAB 4: RETENTION LAB
# ==========================================
with tab_retention:
    st.header("📉 Retention Lab")
    if st.button("🔄 Refresh Data", key="refresh_ret", type="primary"): st.session_state.pop('ret_df', None)
    
    if 'ret_df' not in st.session_state or st.session_state.ret_df.empty:
        st.session_state.ret_df = get_history_df()
        
    df = st.session_state.ret_df.copy()
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
        peak_col = next((c for c in df.columns if "Peak" in c), None)
        end_col = next((c for c in df.columns if "End" in c), None)
        date_col = next((c for c in df.columns if "Date" in c), None)
        batch_col = next((c for c in df.columns if "Batch" in c), None)
        
        if peak_col and date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df[peak_col] = pd.to_numeric(df[peak_col], errors='coerce').fillna(0)
            df[end_col] = pd.to_numeric(df[end_col], errors='coerce').fillna(0)
            
            min_d, max_d = df[date_col].min(), df[date_col].max()
            sel = st.date_input("Filter Date Range", value=(min_d, max_d), key="ret_d")
            if len(sel)==2: df = df[(df[date_col] >= pd.to_datetime(sel[0])) & (df[date_col] <= pd.to_datetime(sel[1]))]
            
            df = df[df[peak_col] > 0].sort_values(by=date_col)
            
            if not df.empty:
                st.markdown("### 🏚️ Batch Decay")
                if batch_col:
                    sb = st.selectbox("Select Batch", df[batch_col].unique())
                    b_df = df[df[batch_col] == sb].sort_values(by=date_col)
                    if not b_df.empty:
                        base = b_df.iloc[0][peak_col]
                        b_df['Rel'] = (b_df[peak_col]/base*100) if base > 0 else 0
                        b_df['Seq'] = range(1, len(b_df)+1)
                        fig = px.line(b_df, x='Seq', y='Rel', markers=True, title=f"Decay: {sb}", template="plotly_white")
                        fig.update_yaxes(range=[0, 110])
                        st.plotly_chart(fig, use_container_width=True)
                
                st.divider()
                if end_col:
                    df['Stick'] = (df[end_col]/df[peak_col]*100)
                    st.markdown("### 🧲 Stickiness Trend")
                    fig_s = px.line(df, x=date_col, y="Stick", markers=True, title="Stickiness % (End/Peak)", template="plotly_white")
                    fig_s.update_traces(line_color="#27ae60")
                    st.plotly_chart(fig_s, use_container_width=True)
            else:
                st.warning("No valid retention data in range.")