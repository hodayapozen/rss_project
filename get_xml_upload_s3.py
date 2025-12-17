"""
Fetch RSS feeds and upload XML data to S3 buckets.
"""
from typing import Tuple, Optional
from bs4 import BeautifulSoup
from datetime import datetime
import boto3
from urllib.parse import urlparse
import re
import unicodedata
import requests

# ייבוא המילון מהקובץ החיצוני
try:
    from rss_feeds import RSS_FEEDS
except ImportError:
    print("❌ Error: rss_feeds.py not found. Using empty dictionary.")
    RSS_FEEDS = {}

# ============================================================================
# Configuration
# ============================================================================
RAW_DATA_BUCKET = "rss-raw-data"
REQUEST_TIMEOUT = 30  # seconds

# ============================================================================
# S3 Client Initialization
# ============================================================================
def init_s3_client() -> boto3.client:
    """
    Initialize AWS S3 client and ensure buckets exist.
    """
    s3 = boto3.client("s3")
    try:
        # בדיקה אם הבאקט קיים על ידי ניסיון לקרוא את ה-Metadata שלו
        s3.head_bucket(Bucket=RAW_DATA_BUCKET)
        print(f"🪣 Bucket already exists: {RAW_DATA_BUCKET}")
    except:
        # אם הבאקט לא קיים או שאין הרשאה ל-head, ננסה ליצור אותו
        my_region = s3.meta.region_name or "us-east-1"
        print(f"🪣 Creating bucket: {RAW_DATA_BUCKET} in region: {my_region}")
        try:
            if my_region == "us-east-1":
                s3.create_bucket(Bucket=RAW_DATA_BUCKET)
            else:
                s3.create_bucket(
                    Bucket=RAW_DATA_BUCKET,
                    CreateBucketConfiguration={"LocationConstraint": my_region}
                )
        except s3.exceptions.BucketAlreadyOwnedByYou:
            print(f"🪣 Bucket {RAW_DATA_BUCKET} already exists.")
            
    return s3

# ============================================================================
# Text Processing & S3 Upload
# ============================================================================
def clean_for_filename(text: str) -> str:
    if not text: return "unknown"
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r'[\\/:*?"<>|]', '-', text)
    text = re.sub(r'[\s,()]+', '_', text)
    text = re.sub(r'[^a-zA-Z0-9\u0590-\u05FF._-]', '', text)
    return text.strip('_-.')

def upload_to_s3(s3: boto3.client, bucket_name: str, filename: str, xml_data: bytes) -> None:
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=xml_data,
            ContentType="application/xml"
        )
        print(f"✅ Uploaded: {filename}")
    except Exception as e:
        print(f"❌ Error uploading {filename}: {e}")

# ============================================================================
# RSS Processing Logic
# ============================================================================
def extract_source_and_category(soup: BeautifulSoup, category: str) -> Tuple[str, str]:
    channel = soup.find("channel")
    if not channel: return "unknown", clean_for_filename(category)
    
    link_elem = channel.find("link")
    title_elem = channel.find("title")
    
    link = (link_elem.text or "").strip() if link_elem else ""
    title = (title_elem.text or "").strip() if title_elem else ""
    
    netloc = urlparse(link).netloc.lower()
    domain_map = {"ynet": "ynet", "maariv": "maariv", "walla": "walla", "mako": "mako"}
    source = next((mapped for key, mapped in domain_map.items() if key in netloc), netloc.split(".")[0] or "unknown")
    
    category_value = title if title else category
    return clean_for_filename(source), clean_for_filename(category_value)

def process_rss_feed(s3: boto3.client, category: str, url: str) -> bool:
    try:
        print(f"📡 Fetching {category}...")
        """""
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        """""
        response = requests.get(url, verify=False)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, "xml")
        source, category_clean = extract_source_and_category(soup, category)
        
        # יצירת מבנה היררכי: source/year/month/day/file
        now = datetime.now()
        timestamp = now.strftime("%H%M%S")
        datestamp = now.strftime("%Y-%m-%d")
        
        # שם הקובץ החדש ייראה כך: ynet/2023-10-27/news_143005.xml
        filename = f"{source}/{datestamp}/{category_clean}_{timestamp}.xml"
        
        # שימוש ב-soup.encode כדי לשמור על קידוד XML תקין
        xml_data = soup.encode("utf-8")
        
        upload_to_s3(s3, RAW_DATA_BUCKET, filename, xml_data)
        return True
        
    except Exception as e:
        print(f"❌ Error processing {category}: {e}")
        return False

def main() -> None:
    try:
        s3 = init_s3_client()
        total = len(RSS_FEEDS)
        success = 0
        
        for category, url in RSS_FEEDS.items():
            if process_rss_feed(s3, category, url):
                success += 1
        
        print(f"\n✨ Finished! {success}/{total} feeds processed.")
    except Exception as e:
        print(f"❌ Fatal error: {e}")

if __name__ == "__main__":
    main()