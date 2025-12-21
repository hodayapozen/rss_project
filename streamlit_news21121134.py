import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import json
import plotly.express as px
import re
import requests

def trigger_airflow_dag():
    auth = ("airflow_user", "airflow_password")
    url = "http://localhost:8080/api/v1/dags/rss_to_mysql_pipeline/dagRuns"
    response = requests.post(url, json={}, auth=auth)
    if response.status_code == 200:
        st.success("Airflow DAG triggered!")
def local_css():
    st.markdown("""
        <style>
        /* הגדרות כלליות */
        html, body, [data-testid="stAppViewContainer"] {
            direction: rtl;
            text-align: right;
        }

        /* כרטיסי חדשות גמישים */
        .news-card {
            background-color: white; 
            padding: 1.5rem; 
            border-radius: 12px;
            border-right: 5px solid #007bff; 
            box-shadow: 0 4px 10px rgba(0,0,0,0.05);
            margin-bottom: 1rem;
            width: 100%; /* תופס את כל רוחב העמודה שבה הוא נמצא */
        }

        /* התאמות למסכים קטנים */
        @media (max-width: 640px) {
            [data-testid="stMetricValue"] {
                font-size: 28px !important;
            }
            .news-title {
                font-size: 18px !important;
            }
        }
        </style>
    """, unsafe_allow_html=True)


# ==========================================
# 1. פונקציות עזר ועיצוב CSS
# ==========================================
def clean_html(raw_html):
    if not raw_html: return ""
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return " ".join(cleantext.split())

def local_css():
    st.markdown("""
        <style>
        .stApp { background-color: #f8f9fa; font-family: 'Segoe UI', system-ui, sans-serif; }
        
        /* עיצוב המדדים (Metrics) - גדול ובולט במיוחד */
        [data-testid="stMetricLabel"] p {
            font-size: 36px !important;  /* גודל כותרת המדד */
            font-weight: 800 !important;
            color: #1a1a1a !important;
            line-height: 1.2 !important;
            margin-bottom: 10px !important;
        }
        
        [data-testid="stMetricValue"] {
            font-size: 48px !important;  /* גודל המספר/ערך המדד */
            color: #007bff !important;   /* צבע כחול בולט למספרים */
            font-weight: 900 !important;
        }

        /* הוספת רקע וריווח למדדים */
        [data-testid="stMetric"] {
            background: white;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            border: 1px solid #eee;
        }

        .news-card {
            background-color: white; padding: 24px; border-radius: 16px;
            border-right: 6px solid #007bff; box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            margin-bottom: 24px; transition: all 0.3s ease; height: 100%;
            display: flex; flex-direction: column; direction: rtl; text-align: right;
        }
        .news-card:hover { transform: translateY(-8px); box-shadow: 0 12px 24px rgba(0,0,0,0.12); }
        .news-title { color: #1a1a1a; font-size: 22px; font-weight: 700; margin-bottom: 12px; line-height: 1.4; }
        .news-meta { color: #6c757d; font-size: 14px; margin-bottom: 16px; display: flex; align-items: center; gap: 10px; }
        .source-tag { background-color: #e7f1ff; color: #007bff; padding: 4px 12px; border-radius: 50px; font-weight: 700; font-size: 12px; }
        .news-desc { color: #4a4a4a; font-size: 16px; line-height: 1.6; margin-bottom: 20px; flex-grow: 1; }
        .read-more-link { color: #007bff; font-weight: 700; text-decoration: none; display: inline-flex; align-items: center; gap: 5px; }
        
        @keyframes pulse { 0% { transform: scale(1); } 50% { transform: scale(1.15); } 100% { transform: scale(1); } }
        .animated-icon { display: inline-block; animation: pulse 2.5s infinite ease-in-out; }
        [data-testid="stSidebar"] { background-color: #ffffff; border-left: 1px solid #eee; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("""
        <style>
        /* הגדרת כיוון כללי לימין */
        .main { direction: rtl; text-align: right; }
        
        /* העברת הסיידבר לצד ימין */
        [data-testid="stSidebar"] {
            direction: rtl;
            text-align: right;
        }

        /* תיקון מיקום כפתור הפתיחה/סגירה של הסיידבר */
        [data-testid="stSidebarCollapsedControl"] {
            right: 0;
            left: auto;
        }

        .stApp { background-color: #f8f9fa; font-family: 'Segoe UI', system-ui, sans-serif; }
        
        /* ... שאר ה-CSS הקיים שלך ... */
        </style>
    """, unsafe_allow_html=True)
    st.markdown("""
   <style>
        /* 1. הגדרת כיוון כתיבה כללי לימין */
        .main {
            direction: rtl;
            text-align: right;
        }

        /* 2. העברת התפריט (Sidebar) לצד ימין */
        [data-testid="stSidebar"] {
            position: fixed;
            right: 0 !important;
            left: auto !important;
            direction: rtl;
        }

        /* 3. הזזת התוכן הראשי שמאלה כדי שלא יוסתר על ידי התפריט */
        [data-testid="stAppViewContainer"] {
            direction: rtl;
        }
        
        /* תיקון שוליים לאזור הראשי */
        [data-testid="stMainViewContainer"] {
            margin-right: 0;
            margin-left: auto;
        }

        /* 4. תיקון כפתור פתיחת/סגירת התפריט שיופיע בצד ימין */
        [data-testid="stSidebarCollapsedControl"] {
            right: 20px;
            left: auto;
        }

        /* עיצוב כרטיסי החדשות והמדדים */
        .news-card {
            background-color: white; 
            padding: 24px; 
            border-radius: 16px;
            border-right: 6px solid #007bff; 
            box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            margin-bottom: 24px; 
            direction: rtl; 
            text-align: right;
        }

        [data-testid="stMetricValue"] {
            font-size: 40px !important;
            color: #007bff !important;
        }
        </style>
    """, unsafe_allow_html=True)
# ==========================================
# 2. חיבור לדאטאבייס
# ==========================================
DB_CONFIG = {"user": "hodaya", "password": "hodaya123", "host": "localhost", "port": 3307, "database": "rss_project"}
DB_CONNECTION_STRING = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

@st.cache_data(ttl=300)
def load_data():
    engine = create_engine(DB_CONNECTION_STRING)
    try:
        df = pd.read_sql("SELECT * FROM rss_raw_items ORDER BY published_date DESC", engine)
        if 'published_date' in df.columns: df['published_date'] = pd.to_datetime(df['published_date'])
        return df
    except Exception as e:
        st.error(f"שגיאה: {e}")
        return pd.DataFrame()
    finally: engine.dispose()

# ==========================================
# 3. ממשק משתמש
# ==========================================
st.set_page_config(page_title="RSS Analytics Pro", layout="wide", page_icon="🗞️")
local_css()

st.markdown("""
    <div style='text-align: center; padding: 20px 0;'>
        <h1 style='font-size: 55px; color: #1a1a1a; margin-bottom: 0;'>
            <span class='animated-icon'>📡</span> כל החדשות והעדכונים בזמן אמת <span class='animated-icon'>📊</span>
        </h1>
    </div>
    """, unsafe_allow_html=True)

df = load_data()

if not df.empty:
    st.sidebar.image("https://cdn-icons-png.flaticon.com/512/2540/2540832.png", width=120)
    st.sidebar.title("מסננים")
    search_query = st.sidebar.text_input("🔍 חיפוש חופשי בכותרות", "")
    selected_cat = st.sidebar.selectbox("📂 טאגים", ["הכל"] + sorted(df['category'].unique().tolist()))
    selected_source = st.sidebar.selectbox("🏠 מקור", ["הכל"] + sorted(df['source'].unique().tolist()))
    
    if st.sidebar.button('🔄 רענן נתונים'):
        st.cache_data.clear()
        st.rerun()

    filtered_df = df.copy()
    if selected_cat != "הכל": filtered_df = filtered_df[filtered_df['category'] == selected_cat]
    if selected_source != "הכל": filtered_df = filtered_df[filtered_df['source'] == selected_source]
    if search_query: filtered_df = filtered_df[filtered_df['title'].str.contains(search_query, case=False, na=False)]

    # --- דאשבורד עליון ---
    # הוספת מרווחים (Padding) בין המדדים לגרף
    col_stat1, col_stat2, col_stat3, col_chart = st.columns([1, 1, 1.3, 1.7])
    
    with col_stat1: st.metric("סה\"כ כתבות", len(filtered_df))
    with col_stat2: st.metric("מקורות פעילים", filtered_df['source'].nunique())
    with col_stat3:
        latest = filtered_df['published_date'].max().strftime('%H:%M') if not filtered_df.empty else "--:--"
        st.metric("עדכון אחרון", latest)

    with col_chart:
        if not filtered_df.empty:
            source_counts = filtered_df['source'].value_counts().reset_index()
            source_counts.columns = ['מקור', 'כמות']
            
            fig = px.pie(source_counts, values='כמות', names='מקור', hole=0.6, height=300,
                         color_discrete_sequence=px.colors.qualitative.Pastel)
            
            fig.update_traces(
                textposition='inside', textinfo='percent',
                marker=dict(line=dict(color='#f8f9fa', width=3)),
                pull=[0.05] * len(source_counts)
            )
            
            fig.update_layout(
                showlegend=True,
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.1),
                margin=dict(l=0, r=0, t=10, b=10),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                annotations=[dict(text='מקורות', x=0.5, y=0.5, font_size=18, showarrow=False, font_weight="bold")]
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

    st.markdown("<br><br>", unsafe_allow_html=True)
    st.divider()

    # --- גריד כתבות ---
    if filtered_df.empty:
        st.info("לא נמצאו כתבות.")
    else:
        col_left, col_right = st.columns(2)
        for i, (idx, row) in enumerate(filtered_df.iterrows()):
            target_col = col_left if i % 2 == 0 else col_right
            with target_col:
                clean_description = clean_html(row['description'])
                st.markdown(f"""
                    <div class="news-card">
                        <div class="news-meta">
                            <span class="source-tag">{row['source']}</span>
                            <span>{row['category']} • {row['published_date'].strftime('%H:%M') if pd.notnull(row['published_date']) else ''}</span>
                        </div>
                        <div class="news-title">{row['title']}</div>
                        <div class="news-desc">{clean_description[:200]}...</div>
                        <a href="{row['link']}" target="_blank" class="read-more-link">קרא עוד ב-{row['source']} ←</a>
                    </div>
                """, unsafe_allow_html=True)
                st.write("")
else:
    st.warning("אין נתונים.")