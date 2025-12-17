import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import json
import plotly.express as px

# ==========================================
# הגדרות חיבור
# ==========================================
DB_CONFIG = {
    "user": "hodaya",
    "password": "hodaya123",
    "host": "localhost",
    "port": 3307,
    "database": "rss_project"
}

DB_CONNECTION_STRING = (
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

@st.cache_data(ttl=300)
def load_data():
    engine = create_engine(DB_CONNECTION_STRING)
    query = "SELECT * FROM rss_raw_items ORDER BY published_date DESC"
    try:
        df = pd.read_sql(query, engine)
        if 'published_date' in df.columns:
            df['published_date'] = pd.to_datetime(df['published_date'])
        return df
    except Exception as e:
        st.error(f"שגיאה בשליפת נתונים: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

# ==========================================
# עיצוב דף
# ==========================================
st.set_page_config(page_title="RSS Analytics Hub", layout="wide", page_icon="📊")

st.title("📊 דאשבורד חדשות RSS וניתוח נתונים")

df = load_data()

if not df.empty:
    # --- חלק האנליטיקה (גרפים) ---
    st.subheader("📈 סטטיסטיקת מקורות מידע")
    
    col_chart, col_stats = st.columns([2, 1])
    
    with col_chart:
        # יצירת נתונים לגרף: כמות כתבות לפי מקור
        source_counts = df['source'].value_counts().reset_index()
        source_counts.columns = ['מקור', 'כמות כתבות']
        
        fig = px.bar(source_counts, x='מקור', y='כמות כתבות', 
                     color='מקור', title="התפלגות כתבות לפי מקור",
                     template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

    with col_stats:
        st.write("### נתונים מהירים")
        st.metric("סה\"כ כתבות", len(df))
        st.metric("מספר מקורות", df['source'].nunique())
        st.metric("קטגוריות", df['category'].nunique())
        
        # כתבה אחרונה שעלתה
        last_update = df['published_date'].max()
        st.info(f"עדכון אחרון בדאטאבייס: \n{last_update}")

    st.divider()

    # --- חלק החיפוש והסינון ---
    st.sidebar.header("🔍 סינון וחיפוש")
    search_query = st.sidebar.text_input("חפש מילת מפתח:", "")
    
    selected_cat = st.sidebar.selectbox("קטגוריה:", ["הכל"] + sorted(df['category'].unique().tolist()))
    selected_source = st.sidebar.selectbox("מקור:", ["הכל"] + sorted(df['source'].unique().tolist()))

    # סינון ה-DataFrame
    filtered_df = df.copy()
    if search_query:
        filtered_df = filtered_df[
            filtered_df['title'].str.contains(search_query, case=False, na=False) |
            filtered_df['description'].str.contains(search_query, case=False, na=False)
        ]
    if selected_cat != "הכל":
        filtered_df = filtered_df[filtered_df['category'] == selected_cat]
    if selected_source != "הכל":
        filtered_df = filtered_df[filtered_df['source'] == selected_source]

    # --- תצוגת תוצאות ---
    st.subheader(f"📑 כתבות נמצאו: {len(filtered_df)}")
    
    for _, row in filtered_df.iterrows():
        with st.expander(f"{row['title']} | {row['source']} ({row['published_date'].strftime('%H:%M') if not pd.isna(row['published_date']) else ''})"):
            st.write(f"**מקור:** {row['source']} | **קטגוריה:** {row['category']}")
            st.write(row['description'])
            st.markdown(f"[🔗 קישור לכתבה]({row['link']})")
            
            # הצגת תגיות
            if row['tags'] and row['tags'] != '[]':
                try:
                    tags = json.loads(row['tags'])
                    st.caption(f"🏷️ תגיות: {', '.join(tags)}")
                except:
                    pass

    if st.sidebar.button('רענן דאטאבייס 🔄'):
        st.cache_data.clear()
        st.rerun()

else:
    st.warning("לא נמצאו נתונים להצגה.")