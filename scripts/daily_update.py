import os
import datetime
from pathlib import Path
import utils
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_if_data_exists(data_dir):
    """Check if the database already has data."""
    data_path = Path(data_dir)
    if not data_path.exists():
        return False
    
    # Check if any category directories have JSON files
    for category_id in utils.TARGET_CATEGORIES.keys():
        category_path = data_path / category_id
        if category_path.exists() and any(category_path.glob('*.json')):
            return True
    
    return False

def main():
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Check if this is the first run (no existing data)
    if not check_if_data_exists(data_dir):
        logging.info("No existing data found. Building initial 30-day history...")
        utils.fetch_and_build_history(days=30, data_dir=data_dir)
        logging.info("Initial data build complete.")
        return
    
    # Regular daily update
    # Target date: Yesterday (since today's data might not be complete)
    yesterday = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    
    logging.info(f"Starting daily update for {date_str}")
    
    url = f"https://tcgcsv.com/archive/tcgplayer/prices-{date_str}.ppmd.7z"
    archive_name = f"prices-{date_str}.ppmd.7z"
    archive_path = Path(archive_name)
    
    if utils.download_file(url, archive_path):
        try:
            utils.process_daily_data(archive_path, date_str, data_dir)
            logging.info(f"Successfully processed data for {date_str}")
        except Exception as e:
            logging.error(f"Failed to process {date_str}: {e}")
            exit(1)
        finally:
            if archive_path.exists():
                os.remove(archive_path)
    else:
        logging.error(f"Failed to download data for {date_str}")
        exit(1)

if __name__ == "__main__":
    main()
