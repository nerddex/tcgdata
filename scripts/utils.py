import os
import requests
import py7zr
import json
import logging
import shutil
import datetime
from pathlib import Path
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ============================================================
# CONFIGURATION: Add or remove trading card categories here
# Format: 'tcgplayer_category_id': 'Category Name'
# ============================================================
TARGET_CATEGORIES = {
    '1': 'Magic: The Gathering',
    '2': 'Yu-Gi-Oh!',
    '3': 'Pokemon',
    '68': 'One Piece'
}
# ============================================================

# Price fields to track
PRICE_FIELDS = ['lowPrice', 'midPrice', 'highPrice', 'marketPrice', 'directLowPrice']


def download_file(url, dest_path):
    """Downloads a file from a URL to a destination path."""
    logging.info(f"Downloading {url} to {dest_path}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download {url}: {e}")
        return False
    
    # Verify file size
    if os.path.getsize(dest_path) == 0:
        logging.error(f"Downloaded file {dest_path} is empty.")
        os.remove(dest_path)
        return False
        
    logging.info("Download complete.")
    return True


def extract_prices_from_archive(archive_path, date_str, target_categories):
    """
    Extracts price data from the archive for the specified categories.
    
    Returns a dict: (category_id, product_id, subTypeName) -> list of price records
    """
    logging.info(f"Extracting prices from {archive_path} for date {date_str}...")
    
    price_data = defaultdict(list)
    
    try:
        with py7zr.SevenZipFile(archive_path, mode='r') as z:
            all_files = z.getnames()
            
            # Filter files for target categories
            # Structure: {date}/{categoryId}/{groupId}/prices
            files_to_process = []
            for fname in all_files:
                parts = fname.replace('\\', '/').split('/')
                if len(parts) >= 4 and parts[1] in target_categories and parts[-1] == 'prices':
                    files_to_process.append(fname)
            
            if not files_to_process:
                logging.warning(f"No relevant files found in {archive_path}")
                return price_data

            logging.info(f"Found {len(files_to_process)} files to process.")
            
            # Extract to a temporary directory
            temp_extract_dir = Path(archive_path).parent / f"temp_{date_str}"
            if temp_extract_dir.exists():
                shutil.rmtree(temp_extract_dir)
            
            z.extract(targets=files_to_process, path=temp_extract_dir)
            
            # Process extracted files
            for fname in files_to_process:
                parts = fname.replace('\\', '/').split('/')
                category_id = parts[1]
                file_path = temp_extract_dir / fname
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = json.load(f)
                        
                        items = []
                        if isinstance(content, dict) and 'results' in content:
                            items = content['results']
                        elif isinstance(content, list):
                            items = content
                        elif isinstance(content, dict):
                            items = [content]
                        else:
                            continue
                            
                        for item in items:
                            product_id = item.get('productId')
                            sub_type = item.get('subTypeName', 'Normal')
                            
                            if not product_id:
                                continue
                            
                            # Extract price data
                            record = {
                                'date': date_str,
                                'productId': product_id,
                                'subTypeName': sub_type
                            }
                            
                            for field in PRICE_FIELDS:
                                record[field] = item.get(field)
                            
                            key = (category_id, str(product_id), sub_type)
                            price_data[key].append(record)
                            
                except json.JSONDecodeError:
                    logging.error(f"Failed to decode JSON in {fname}")
                except Exception as e:
                    logging.error(f"Error processing file {fname}: {e}")

            # Cleanup temp dir
            shutil.rmtree(temp_extract_dir)
            
    except Exception as e:
        logging.error(f"Error processing archive {archive_path}: {e}")
        raise
    
    return price_data


def update_product_file(data_dir, category_id, product_id, sub_type, price_records):
    """
    Update the product JSON file with price data from day 7 and day 30.
    Each file contains exactly 2 entries: one for day 7 and one for day 30.
    """
    output_dir = Path(data_dir) / category_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{product_id}_{sub_type}.json"
    
    # Read existing data if file exists
    existing_data = {}
    if output_file.exists():
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            existing_data = {}
    
    # Update with new price records
    # price_records is a list of records (should be 1 or 2 items)
    for record in price_records:
        date_str = record['date']
        
        # Determine if this is day 7 or day 30 data
        today = datetime.datetime.now(datetime.timezone.utc).date()
        record_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        days_diff = (today - record_date).days
        
        if days_diff == 7:
            key = 'day7'
        elif days_diff == 30:
            key = 'day30'
        else:
            continue
        
        # Store the price data
        existing_data[key] = {
            'date': date_str,
            'lowPrice': record.get('lowPrice'),
            'midPrice': record.get('midPrice'),
            'highPrice': record.get('highPrice'),
            'marketPrice': record.get('marketPrice'),
            'directLowPrice': record.get('directLowPrice')
        }
    
    # Add metadata
    existing_data['productId'] = int(product_id)
    existing_data['subTypeName'] = sub_type
    existing_data['lastUpdated'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=2)


def process_daily_data(archive_path, date_str, data_dir):
    """
    Process a single day's archive and update all product files.
    """
    logging.info(f"Processing daily data for {date_str}...")
    
    # Extract prices from archive
    price_data = extract_prices_from_archive(archive_path, date_str, TARGET_CATEGORIES.keys())
    
    if not price_data:
        logging.warning("No price data extracted.")
        return
    
    # Update each product file
    logging.info(f"Updating {len(price_data)} product files...")
    
    for (category_id, product_id, sub_type), records in price_data.items():
        update_product_file(data_dir, category_id, product_id, sub_type, records)
    
    logging.info("Daily processing complete.")
