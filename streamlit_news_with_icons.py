import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import json
import plotly.express as px
import re
import os
import base64
from pathlib import Path

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
        /* 1. ביטול הרווח העליון הגדול של Streamlit */
        .block-container {
            padding-top: 1rem !important; /* היה במקור סביב 5rem */
            padding-bottom: 0rem !important;
        }
        
        /* 2. הקטנת רווחים בכותרות */
        h1 {
            margin-bottom: 0px !important;
            padding-bottom: 0px !important;
        }

        .stApp { background-color: #f8f9fa; font-family: 'Segoe UI', system-ui, sans-serif; }
        
        /* עיצוב המדדים - צמצום Padding פנימי */
        [data-testid="stMetric"] {
            background: white;
            padding: 10px; /* הקטנו מ-20 ל-10 */
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            border: 1px solid #eee;
            text-align: center;
        }

        [data-testid="stMetricLabel"] p {
            font-size: 24px !important; /* הקטנו מעט */
            margin-bottom: 0px !important;
        }
        
        [data-testid="stMetricValue"] {
            font-size: 36px !important;
            color: #007bff !important;
        }

        /* שאר ה-CSS שלך נשאר זהה... */
        .news-card { ... }
        .main { direction: rtl; text-align: right; }
        [data-testid="stSidebar"] { right: 0; }
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
# 2. מאגר אייקונים למקורות
# ==========================================
# הגדר כאן את הנתיבים לתמונות האייקונים שלך
# אפשר להשתמש בנתיבים מקומיים או URLs
SOURCE_ICONS = {
    "ynet": "icons/ynet.png",  # שנה לנתיב של התמונה שלך
    "walla": "icons/walla.png",
    "maariv": "icons/maariv.png",
    "mako": "icons/mako.png",
    "haaretz": "icons/haaretz.png",
    # הוסף עוד מקורות לפי הצורך
    # "ynetnews": "icons/ynetnews.png",
}

@st.cache_data
def load_all_icons_base64() -> dict:
    """
    טוען את כל התמונות פעם אחת וממיר אותן ל-base64.
    נשמר ב-cache כדי למנוע טעינה חוזרת.
    """
    icons_cache = {}
    for source_name, image_path in SOURCE_ICONS.items():
        # רק תמונות מקומיות (לא URLs)
        if not (image_path.startswith('http://') or image_path.startswith('https://')):
            try:
                if os.path.exists(image_path):
                    with open(image_path, "rb") as img_file:
                        encoded = base64.b64encode(img_file.read()).decode()
                        ext = Path(image_path).suffix.lower()
                        mime_type = {
                            '.png': 'image/png',
                            '.jpg': 'image/jpeg',
                            '.jpeg': 'image/jpeg',
                            '.gif': 'image/gif',
                            '.svg': 'image/svg+xml',
                            '.webp': 'image/webp'
                        }.get(ext, 'image/png')
                        icons_cache[source_name.lower()] = f"data:{mime_type};base64,{encoded}"
            except Exception as e:
                print(f"⚠️ Error loading image {image_path}: {e}")
    return icons_cache

def get_source_icon_html(source_name: str, icons_cache: dict) -> str:
    """
    מחזיר HTML של אייקון למקור נתון.
    משתמש ב-cache שכבר נטען.
    """
    # נסה למצוא התאמה מדויקת
    source_key = None
    icon_path = None
    
    if source_name.lower() in SOURCE_ICONS:
        icon_path = SOURCE_ICONS[source_name.lower()]
        source_key = source_name.lower()
    else:
        # נסה למצוא התאמה חלקית
        for key, path in SOURCE_ICONS.items():
            if key.lower() in source_name.lower():
                icon_path = path
                source_key = key.lower()
                break
    
    if not icon_path:
        return ''  # אין אייקון
    
    # בדוק אם זה URL או נתיב מקומי
    if icon_path.startswith('http://') or icon_path.startswith('https://'):
        return f'<img src="{icon_path}" style="width:18px;height:18px;object-fit:contain;border-radius:3px;" loading="lazy" onerror="this.style.display=\'none\'">'
    else:
        # זה נתיב מקומי - השתמש ב-cache
        if source_key and source_key in icons_cache:
            return f'<img src="{icons_cache[source_key]}" style="width:18px;height:18px;object-fit:contain;border-radius:3px;" loading="lazy">'
    
    return ''

# ==========================================
# 3. חיבור לדאטאבייס
# ==========================================
DB_CONFIG = {"user": "hodaya", "password": "hodaya123", "host": "localhost", "port": 3307, "database": "rss_project"}
DB_CONNECTION_STRING = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

@st.cache_data(ttl=300)
def load_data():
    engine = create_engine(DB_CONNECTION_STRING)
    try:
        # 1. שליפת הכתבות (Items) + שמות המקורות והקטגוריות (Sources)
        # אנחנו מבצעים JOIN כדי לקבל את ה-source_name וה-feed_category
        # שימו לב: אנו לוקחים את item_id וקוראים לו id כדי שהדאשבורד יזהה אותו
        items_query = """
            SELECT 
                ri.item_id AS id,
                rs.source_name AS source,
                rs.feed_category AS category,
                ri.title,
                ri.link,
                ri.published_date,
                ri.description
            FROM RSS_Items ri
            JOIN RSS_Sources rs ON ri.source_id = rs.source_id
            ORDER BY ri.published_date DESC
        """
        df_items = pd.read_sql(items_query, engine)
        
        # המרת תאריך
        if 'published_date' in df_items.columns: 
            df_items['published_date'] = pd.to_datetime(df_items['published_date'])
        
        # וידוא שה-ID הוא מספר (חשוב לחיבור עם התגיות)
        if 'id' in df_items.columns:
            df_items['id'] = pd.to_numeric(df_items['id'], errors='coerce').fillna(0).astype(int)

        # 2. שליפת התגיות (Tags)
        # חיבור בין טבלת הקישור (Item_Tags) לבין שמות התגיות (RSS_Tags)
        tags_query = """
            SELECT 
                it.item_id, 
                rt.tag_name
            FROM Item_Tags it
            JOIN RSS_Tags rt ON it.tag_id = rt.tag_id
        """
        df_tags = pd.read_sql(tags_query, engine)
        
        # וידוא שה-item_id בתגיות הוא מספר
        if 'item_id' in df_tags.columns:
            df_tags['item_id'] = pd.to_numeric(df_tags['item_id'], errors='coerce').fillna(0).astype(int)
        
        return df_items, df_tags

    except Exception as e:
        st.error(f"שגיאה בטעינת נתונים: {e}")
        return pd.DataFrame(), pd.DataFrame()
    finally: 
        engine.dispose()

# ==========================================
# 4. ממשק משתמש
# ==========================================
st.set_page_config(page_title="RSS Analytics Pro", layout="wide", page_icon="🗞️")
local_css()

st.markdown("""
    <div style='text-align: center; padding-bottom: 10px;'>
        <h1 style='font-size: 40px; color: #1a1a1a; margin: 0;'>
            <span class='animated-icon'>📡</span> כל החדשות והעדכונים <span class='animated-icon'>📊</span>
        </h1>
    </div>
    """, unsafe_allow_html=True)

# --- טעינת נתונים (עכשיו מקבלים 2 דאטה-פריימים) ---
df, df_tags = load_data()

if not df.empty:
    st.sidebar.image("https://cdn-icons-png.flaticon.com/512/2540/2540832.png", width=120)
    st.sidebar.title("מסננים")
    
    # 1. חיפוש חופשי
    search_query = st.sidebar.text_input("🔍 חיפוש חופשי בכותרות", "")
    st.sidebar.markdown("---")

    # 2. פילטר מקור
    selected_source = st.sidebar.selectbox("🏠 מקור", ["הכל"] + sorted(df['source'].unique().tolist()))

    # --- חישוב קטגוריות דינמי ---
    if selected_source == "הכל":
        available_categories = sorted(df['category'].unique().tolist())
    else:
        available_categories = sorted(df[df['source'] == selected_source]['category'].unique().tolist())
    
    # 3. פילטר קטגוריה
    selected_cat = st.sidebar.selectbox("📂 קטגוריה", ["הכל"] + available_categories)
    
    # --- יצירת בסיס לסינון ראשוני (כדי לחשב תגיות רלוונטיות) ---
    temp_df = df.copy()
    if selected_source != "הכל":
        temp_df = temp_df[temp_df['source'] == selected_source]
    if selected_cat != "הכל":
        temp_df = temp_df[temp_df['category'] == selected_cat]
    
    # 4. פילטר תגיות חכם
    st.sidebar.markdown("---")
    
    selected_tags = []
    
    # --- בלוק דיבוג זמני (יופיע בסיידבר) ---
    # st.sidebar.write(f"סך כתבות: {len(temp_df)}")
    # st.sidebar.write(f"סך תגיות ב-DB: {len(df_tags)}")
    # ------------------------------------

    if not df_tags.empty and not temp_df.empty:
        # א. מוצאים את ה-IDs של הכתבות שמוצגות כרגע
        visible_ids = temp_df['id'].tolist()
        
        # ב. מסננים את טבלת התגיות
        relevant_tags = df_tags[df_tags['item_id'].isin(visible_ids)]
        
        # --- בדיקה האם נמצאו תגיות ---
        if relevant_tags.empty:
            st.sidebar.warning("לא נמצאו תגיות לכתבות המוצגות. נא בחר מקורות או קטגוריות אחרות.")
        else:
            # ג. סופרים ולוקחים את ה-50 הנפוצות ביותר
            top_tags_counts = relevant_tags['tag_name'].value_counts().head(50)
            
            # ד. מכינים מפה לתצוגה
            tag_display_map = {f"{tag} ({count})": tag for tag, count in top_tags_counts.items()}
            
            # ה. הצגת הפילטר
            selected_tags_display = st.sidebar.multiselect(
                "🏷️ תגיות נפוצות (Top 50)", 
                options=list(tag_display_map.keys())
            )
            
            selected_tags = [tag_display_map[t] for t in selected_tags_display]

    if st.sidebar.button('🔄 רענן נתונים'):
        st.cache_data.clear()
        st.rerun()

    # --- יישום הפילטרים הסופיים על הטבלה ---
    filtered_df = df.copy()
    
    # סינון לפי מקור
    if selected_source != "הכל": 
        filtered_df = filtered_df[filtered_df['source'] == selected_source]

    # סינון לפי קטגוריה
    if selected_cat != "הכל": 
        filtered_df = filtered_df[filtered_df['category'] == selected_cat]
    
    # סינון לפי תגיות (החלק החדש)
    # if selected_tags:
    #     # מוצאים את ה-item_id שיש להם את התגיות שנבחרו
    #     ids_with_tags = df_tags[df_tags['tag_name'].isin(selected_tags)]['item_id'].unique()
    #     filtered_df = filtered_df[filtered_df['id'].isin(ids_with_tags)] 
    
    # סינון לפי תגיות (לוגיקה של AND: הכתבה חייבת להכיל את כל התגיות שנבחרו)
    if selected_tags:
        # 1. מסננים את טבלת התגיות רק לשורות שרלוונטיות לתגיות שנבחרו
        relevant_rows = df_tags[df_tags['tag_name'].isin(selected_tags)]
        
        # 2. סופרים כמה תגיות *ייחודיות* מתוך הבחירה יש לכל כתבה
        # (למשל: אם בחרת "מלחמה" ו"פוליטיקה", נחפש כתבות שיש להן count של 2)
        id_counts = relevant_rows.groupby('item_id')['tag_name'].nunique()
        
        # 3. שומרים רק את ה-IDs של הכתבות שהמספר הזה שווה למספר התגיות שנבחרו
        ids_with_all_tags = id_counts[id_counts == len(selected_tags)].index.tolist()
        
        # 4. מסננים את הטבלה הראשית
        filtered_df = filtered_df[filtered_df['id'].isin(ids_with_all_tags)]

    # סינון לפי חיפוש טקסט
    if search_query: 
        filtered_df = filtered_df[filtered_df['title'].str.contains(search_query, case=False, na=False)]

    # מיון התוצאות
    filtered_df = filtered_df.sort_values(by='published_date', ascending=False)
    # --- דאשבורד עליון ---
    # שלב 1: מדדים רחבים
    m1, m2, m3 = st.columns(3)

    with m1:
        # עכשיו קטגוריות מופיעות ראשונות מימין
        st.metric("קטגוריות פעילות", filtered_df['category'].nunique())

    with m2:
        # סה"כ כתבות עבר לאמצע
        st.metric("סה\"כ כתבות", len(filtered_df))
    
    with m3:
        latest = filtered_df['published_date'].max().strftime('%H:%M') if not filtered_df.empty else "--:--"
        st.metric("עדכון אחרון", latest)

    # st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # כאן מחקנו את ה-<br> שהיה קודם

    # שלב 2: גרף בר בודד (צמוד למדדים)
    if not filtered_df.empty:
        source_counts = filtered_df['source'].value_counts().reset_index()
        source_counts.columns = ['מקור', 'כמות']
        source_counts['all'] = 'התפלגות'

        fig = px.bar(source_counts, x='כמות', y='all', color='מקור', orientation='h',
                     text='כמות', 
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        
        fig.update_layout(
            height=120,
            showlegend=True,
            # שינוי 1: הרמנו את y ל-1.3 כדי להרחיק את המקרא מהבר
            legend=dict(orientation="h", yanchor="bottom", y=1.3, xanchor="right", x=1),
            # שינוי 2: הוספנו מרווח עליון (t=40) כדי לפנות מקום למקרא המורם
            margin=dict(l=0, r=0, t=40, b=0),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=False, visible=False),
            yaxis=dict(showgrid=False, visible=False, title=None)
        )
        
        fig.update_traces(textposition='inside', textfont_size=14)
        
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

    # הסרת רווחים לפני הקו המפריד
    st.divider()
    
    # --- גריד כתבות ---
    if filtered_df.empty:
        st.info("לא נמצאו כתבות.")
    else:
        # טען את כל האייקונים פעם אחת לפני הלולאה (ב-cache)
        icons_cache = load_all_icons_base64()
        
        # שינוי: הורדנו את st.columns(2) ואת החלוקה לעמודות
        for i, (idx, row) in enumerate(filtered_df.iterrows()):
            
            clean_description = clean_html(row['description'])
            
            # קבל את האייקון מה-cache
            icon_html = get_source_icon_html(row['source'], icons_cache)
            
            # יצירת הכרטיס ישירות בדף (ללא with target_col)
            st.markdown(f"""
                <div class="news-card">
                    <div class="news-meta">
                        <span class="source-tag">{icon_html} {row['source']}</span>
                        <span>{row['category']} • {row['published_date'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['published_date']) else ''}</span>
                    </div>
                    <div class="news-title">{row['title']}</div>
                    <div class="news-desc">{clean_description[:200]}...</div>
                    <a href="{row['link']}" target="_blank" class="read-more-link">קרא עוד ב-{row['source']} ←</a>
                </div>
            """, unsafe_allow_html=True)
            st.write("")  # מרווח קטן בין כרטיסים
else:
    st.warning("אין נתונים.")