"""
Process RSS raw data from S3 and upsert to MySQL database.
"""
from typing import List, Tuple, Optional, Dict, Any
import os
from bs4 import BeautifulSoup
import boto3
import pandas as pd
from dateutil import parser
import json
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.engine import Engine
from datetime import timedelta, datetime, timezone
import pytz
from utils import setup_logging, get_logger, init_s3_client, upload_log_to_s3

setup_logging()
logger = get_logger("RSS_Processor")


# ============================================================================
# Configuration
# ============================================================================
RAW_DATA_BUCKET = "rss-raw-data-test"
TABLE_NAME = "rss_raw_items"

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 3307),
    "database": os.getenv("DB_NAME", "rss_project"),
    "user": os.getenv("DB_USER", "hodaya"),
    "password": os.getenv("DB_PASSWORD", "hodaya123"),
}


DB_CONNECTION_STRING = (
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)


# ============================================================================
# Client Initialization
# ============================================================================
# Using shared init_s3_client from utils


def init_mysql_engine(echo: bool = True) -> Engine:
    """Initialize and return SQLAlchemy MySQL engine."""
    return create_engine(DB_CONNECTION_STRING, echo=echo)


# ============================================================================
# S3 Data Retrieval
# ============================================================================
def get_raw_data(s3: boto3.client, bucket_name: str) -> List[Tuple[str, str]]:
    """
    Retrieve all XML files from S3 bucket.
    
    Args:
        s3: Boto3 S3 client
        bucket_name: Name of the S3 bucket
        
    Returns:
        List of tuples containing (file_name, file_content)
    """
    xml_files = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            try:
                response = s3.get_object(Bucket=bucket_name, Key=key)
                data = response["Body"].read().decode("utf-8")
                xml_files.append((key, data))
                logger.info(f"Processing {key}")
            except Exception as e:
                logger.error(f"Error processing {key}: {e}")
                continue
                
    return xml_files


# ============================================================================
# XML Parsing
# ============================================================================
def parse_xml_item(item, source: str, category: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single RSS item from XML.
    
    Args:
        item: BeautifulSoup item element
        source: RSS source name
        category: RSS category
        
    Returns:
        Dictionary with parsed item data or None if invalid
    """
    guid = item.find("guid")
    if not guid:
        return None
    guid_text = guid.text if guid else None
    title_elem = item.find("title")
    title = title_elem.text if title_elem else ""
    title = title.replace('""', '"').strip()
    
    link_elem = item.find("link")
    link = link_elem.text if link_elem else ""
    published_date_raw = None
    pub_date_elem = item.find("pubDate")
    if pub_date_elem:
        published_date_raw = pub_date_elem.text
    
    published_date = parse_published_date(published_date_raw, source)
    
    # Skip items with future dates (likely parsing errors)
    # Use Israel timezone for comparison
    if published_date:
        try:
            parsed_date = parser.parse(published_date)
            # Get current time in Israel timezone
            israel_tz = pytz.timezone('Asia/Jerusalem')
            now_israel = datetime.now(israel_tz)
            
            # If parsed date is timezone-naive, assume it's already in Israel time
            if parsed_date.tzinfo is None:
                if parsed_date > now_israel.replace(tzinfo=None):
                    logger.error(f"Skipping item {guid_text} with future date: {published_date}")
                    return None
            else:
                # Convert parsed date to Israel timezone for comparison
                if parsed_date.tzinfo:
                    parsed_date_israel = parsed_date.astimezone(israel_tz)
                    if parsed_date_israel > now_israel:
                        logger.error(f"Skipping item {guid_text} with future date: {published_date}")
                        return None
        except Exception as e:
            # If date parsing fails, continue with the item
            logger.warning(f"Could not parse date '{published_date}' for item {guid_text}: {e}")
    description = extract_description(item)
    
    tags = extract_tags(item)
    
    return {
        "id": guid_text,
        "source": source,
        "category": category.strip(),
        "title": title,
        "link": link.strip(),
        "published_date": published_date,
        "description": description,
        "tags": tags
    }


def parse_published_date(date_str: Optional[str], source: str) -> Optional[str]:
    """
    Parse and format published date string.
    
    Args:
        date_str: Raw date string from RSS feed
        source: Source name (for source-specific date handling)
        
    Returns:
        Formatted date string or None
    """
    if not date_str:
        return None

    try:
        # Parse the date string to datetime object
        dt = parser.parse(date_str)
        
        # Source-specific timezone handling
        if "GMT" in date_str.upper() or "UTC" in date_str.upper() or dt.tzinfo is None:
            # Assume UTC if timezone-naive or explicitly GMT/UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            
            # Source-specific adjustments
            if source == "walla":
                # Walla: subtract 1 hour from the time
                dt = dt.replace(tzinfo=None) - timedelta(hours=1)
            elif source == "maariv":
                # Maariv - convert to Israel timezone
                israel_offset_hours = 3 if (dt.month >= 4 and dt.month <= 10) else 2
                dt = dt.replace(tzinfo=None) + timedelta(hours=israel_offset_hours)
            else:
                # Default: convert to Israel timezone
                israel_offset_hours = 3 if (dt.month >= 4 and dt.month <= 10) else 2
                dt = dt.replace(tzinfo=None) + timedelta(hours=israel_offset_hours)
        
        # Return as string in Israel timezone
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.warning(f"Error parsing date '{date_str}': {e}")
        return date_str


def extract_description(item) -> str:
    """
    Extract and clean description from RSS item.
    
    Args:
        item: BeautifulSoup item element
        
    Returns:
        Cleaned description text
    """
    raw_desc = item.find("description")
    if not raw_desc:
        return ""
    
    description = BeautifulSoup(raw_desc.text, "html.parser").get_text(" ", strip=True)
    return description.replace('""', '"').strip()


def extract_tags(item) -> List[str]:
    """
    Extract tags from RSS item.
    
    Args:
        item: BeautifulSoup item element
        
    Returns:
        List of tag strings
    """
    tags_tag = item.find("tags")
    if not tags_tag:
        return []
    
    tags = tags_tag.get_text(strip=True).split(",")
    return [tag.strip() for tag in tags if tag.strip()]


def parse_file_name(file_name: str) -> Tuple[str, str]:
    """
    Parse source and category from file name.
    
    Args:
        file_name: File name in format "source_category.xml"
        
    Returns:
        Tuple of (source, category)
    """
    parts = file_name.replace(".xml", "").split("_")
    source = parts[0] if parts else ""
    category = " ".join(parts[1:]) if len(parts) > 1 else ""
    return source, category


# ============================================================================
# Data Processing
# ============================================================================
def process_raw_data(xml_files: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    Process XML files and convert to cleaned DataFrame.
    
    Args:
        xml_files: List of tuples containing (file_name, file_content)
        
    Returns:
        Cleaned DataFrame with RSS items
    """
    items_list = []
    for file_name, file_data in xml_files:
        try:
            soup = BeautifulSoup(file_data, "xml")
            source, category = parse_file_name(file_name)
            items = soup.find_all("item")

            for item in items:
                parsed_item = parse_xml_item(item, source, category)
                if parsed_item:
                    items_list.append(parsed_item)
        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
            continue

    if not items_list:
        logger.warning("No items found in XML files")
        return pd.DataFrame()

    df = pd.DataFrame(items_list)
    df = clean_dataframe(df)
    
    logger.info(f"Cleaned {len(df)} records")
    
    
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize DataFrame columns.
    
    Args:
        df: Raw DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    # Clean string columns
    string_columns = ["id", "source", "category", "title", "link", "description"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace("\n", " ").str.strip()
    
    # Convert published_date to datetime
    if "published_date" in df.columns:
        df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")
    
    # Serialize tags to JSON string
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else str(x)
        )
    
    return df


# ============================================================================
# Database Operations
# ============================================================================
def upsert_to_mysql(df: pd.DataFrame, table_name: str = TABLE_NAME) -> None:
    """
    Upsert DataFrame records to MySQL table.
    
    Args:
        df: DataFrame to upsert
        table_name: Name of the MySQL table
    """
    if df.empty:
        logger.warning("DataFrame is empty, nothing to upsert")
        return
    
    engine = init_mysql_engine(echo=False)
    metadata = MetaData()
    metadata.reflect(engine)
    
    if table_name not in metadata.tables:
        raise ValueError(f"Table '{table_name}' not found in database")
    
    table = metadata.tables[table_name]
    
    try:
        with engine.begin() as conn:
            for _, row in df.iterrows():
                stmt = insert(table).values(**row.to_dict())
                stmt = stmt.on_duplicate_key_update(
                    title=stmt.inserted.title,
                    description=stmt.inserted.description,
                    published_date=stmt.inserted.published_date,
                    tags=stmt.inserted.tags
                )
                conn.execute(stmt)
        
        logger.info(f"Upserted {len(df)} records successfully!")
    except Exception as e:
        logger.error(f"Error upserting to MySQL: {e}")
        raise
    finally:
        engine.dispose()


def call_normalize_rss_data(procedure_name: str = "NormalizeRSSData") -> None:
    """
    Execute MySQL stored procedure to normalize RSS data.
    
    Args:
        procedure_name: Name of the stored procedure to call
    """
    engine = init_mysql_engine(echo=False)
    
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CALL {procedure_name}()"))
        logger.info(f"Successfully executed stored procedure: {procedure_name}")
    except Exception as e:
        logger.error(f"Error executing stored procedure {procedure_name}: {e}")
        raise
    finally:
        engine.dispose()


# ============================================================================
# Main Execution
# ============================================================================
def main() -> None:
    """Main execution function."""
    try:
        s3 = init_s3_client()
        xml_files = get_raw_data(s3, RAW_DATA_BUCKET)
        
        if not xml_files:
            logger.warning("No XML files found in S3 bucket")
            return
        
        df = process_raw_data(xml_files)
        
        if not df.empty:
            upsert_to_mysql(df)
            # Execute stored procedure to normalize data
            call_normalize_rss_data()
        else:
            logger.warning("No data to upsert")
        upload_log_to_s3(s3)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        upload_log_to_s3(s3)
        raise


if __name__ == "__main__":
    main()
