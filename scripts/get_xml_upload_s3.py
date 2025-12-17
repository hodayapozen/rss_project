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

from rss_feeds import RSS_FEEDS

# ============================================================================
# Configuration
# ============================================================================
RAW_DATA_BUCKET = "rss-raw-data"

# Request configuration
REQUEST_TIMEOUT = 30  # seconds


# ============================================================================
# S3 Client Initialization
# ============================================================================
def init_s3_client() -> boto3.client:
    """
    Initialize AWS S3 client and ensure buckets exist.
    
    Returns:
        Boto3 S3 client
    """
    s3 = boto3.client("s3")
    my_region = s3.meta.region_name or "us-east-1"

    for bucket in [RAW_DATA_BUCKET]:
        existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        if bucket not in existing:
            print(f"🪣 Creating bucket: {bucket} in region: {my_region}")
            if my_region == "us-east-1":
                s3.create_bucket(Bucket=bucket)
            else:
                s3.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": my_region}
                )
        else:
            print(f"🪣 Bucket already exists: {bucket}")

    return s3


# ============================================================================
# S3 Upload Operations
# ============================================================================
def upload_to_s3(
    s3: boto3.client,
    bucket_name: str,
    filename: str,
    xml_data: bytes,
    content_type: str = "application/xml"
) -> None:
    """
    Upload XML data to S3 bucket.
    
    Args:
        s3: Boto3 S3 client
        bucket_name: Name of the S3 bucket
        filename: Object key (filename) in S3
        xml_data: XML data as bytes
        content_type: Content type for the object
    """
    if not xml_data:
        print(f"⚠️ No data to upload for file: {filename}")
        return
    
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=xml_data,
            ContentType=content_type
        )
        print(f"✅ Uploaded {filename} to {bucket_name}/{filename}")
    except Exception as e:
        print(f"❌ Error uploading {filename}: {e}")
        raise


# ============================================================================
# Text Processing
# ============================================================================
def clean_for_filename(text: str) -> str:
    """
    Clean text for safe use as S3 object name (supports Hebrew).
    
    Args:
        text: Text to clean
        
    Returns:
        Cleaned text safe for use as filename
    """
    if not text:
        return "unknown"

    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r'[\\/:*?"<>|]', '-', text)
    text = re.sub(r'[\s,()]+', '_', text)
    text = re.sub(r'[^a-zA-Z0-9\u0590-\u05FF._-]', '', text)
    text = re.sub(r'[-_]+', '_', text).strip('_-.')
    
    return text


# ============================================================================
# RSS Parsing
# ============================================================================
def extract_source_and_category(soup: BeautifulSoup, category: str) -> Tuple[str, str]:
    """
    Extract source and category from RSS feed.
    
    Args:
        soup: BeautifulSoup object of the RSS feed
        
    Returns:
        Tuple of (source, category) as cleaned filenames
    """
    channel = soup.find("channel")
    if not channel:
        return "unknown", "unknown"
    
    link_elem = channel.find("link")
    title_elem = channel.find("title")
    
    if not link_elem or not title_elem:
        return "unknown", "unknown"
    
    link = (link_elem.text or "").strip()
    title = (title_elem.text or "").strip()
    
    netloc = urlparse(link).netloc.lower()
    domain_map = {
        "ynet": "ynet",
        "maariv": "maariv",
        "walla": "walla",
        "mako": "mako",
    }
    source = next((mapped for key, mapped in domain_map.items() if key in netloc), "")
    if not source:
        # fallback: use the domain prefix if exists, otherwise unknown
        source = netloc.split(".")[0] if netloc else "unknown"
    
    # Prefer category from title pattern only if title starts with the source.
    # Otherwise, keep the full title as category.
    if source and title.lower().startswith(source) and " - " in title:
        category_value = title.split(" - ", 1)[1].strip()
    elif title:
        category_value = title
    else:
        category_value = category
    
    return clean_for_filename(source or "unknown"), clean_for_filename(category_value or "unknown")


def parse_rss_feed(url: str) -> Optional[BeautifulSoup]:
    """
    Fetch and parse RSS feed from URL.
    
    Args:
        url: RSS feed URL
        
    Returns:
        BeautifulSoup object of the parsed XML or None if failed
    """
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        if not response.encoding:
            response.encoding = response.apparent_encoding or "utf-8"

        # Use raw bytes so BeautifulSoup can honor XML-declared encoding
        xml_bytes = response.content
        soup = BeautifulSoup(xml_bytes, "xml")

        return soup
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch RSS feed from {url}: {e}")
        return None
    except Exception as e:
        print(f"❌ Error parsing RSS feed from {url}: {e}")
        return None


# ============================================================================
# RSS Processing
# ============================================================================
# def process_feed_items(
#     s3: boto3.client,
#     soup: BeautifulSoup,
#     source: str,
#     category: str
# ) -> int:
#     """
#     Process and upload individual RSS items to S3.
    
#     Args:
#         s3: Boto3 S3 client
#         soup: BeautifulSoup object of the RSS feed
#         source: RSS source name
#         category: RSS category
        
#     Returns:
#         Number of items processed
#     """
#     channel = soup.find("channel")
#     if not channel:
#         return 0
    
#     items = channel.find_all("item")
#     items_count = 0
    
#     for item in items:
#         try:
#             guid_elem = item.find("guid")
#             if not guid_elem:
#                 continue
            
#             guid = guid_elem.text
#             item_name = f"{source}_{category}_{clean_for_filename(guid)}.xml"
#             item_data = str(item).encode("utf-8")
            
#             upload_to_s3(s3, ITEMS_BUCKET, item_name, item_data)
#             items_count += 1
#         except Exception as e:
#             print(f"⚠️ Error processing item: {e}")
#             continue
    
#     return items_count


def process_rss_feed(
    s3: boto3.client,
    category: str,
    url: str
) -> bool:
    """
    Process a single RSS feed: fetch, parse, and upload to S3.
    
    Args:
        s3: Boto3 S3 client
        category: Feed category name
        url: RSS feed URL
        
    Returns:
        True if processing was successful, False otherwise
    """
    try:
        print(f"📡 Fetching {category} from {url}")
        
        soup = parse_rss_feed(url)
        if not soup:
            return False
        
        source, category_clean = extract_source_and_category(soup, category)
        filename = f"{source}_{category_clean}.xml"
        
        # Upload full feed XML
        xml_data = str(soup).encode("utf-8")
        upload_to_s3(s3, RAW_DATA_BUCKET, filename, xml_data)
        
        # Process and upload individual items
        # items_count = process_feed_items(s3, soup, source, category_clean)
        # print(f"📦 Processed {items_count} items from {category}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing {category}: {e}")
        return False


# ============================================================================
# Main Processing
# ============================================================================
def get_rss_xml(s3: boto3.client) -> None:
    """
    Fetch and process all RSS feeds from RSS_FEEDS.
    
    Args:
        s3: Boto3 S3 client
    """
    total_feeds = 0
    successful_feeds = 0
    
    for category, url in RSS_FEEDS.items():
        total_feeds += 1
        if process_rss_feed(s3, category, url):
            successful_feeds += 1
    
    print(f"✨ Completed! Processed {successful_feeds}/{total_feeds} feeds successfully")


# ============================================================================
# Main Execution
# ============================================================================
def main() -> None:
    """Main execution function."""
    try:
        s3 = init_s3_client()
        get_rss_xml(s3)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
