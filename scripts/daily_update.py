import os
import datetime
from pathlib import Path
import utils
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    today = datetime.datetime.now(datetime.timezone.utc).date()
    
    # Calculate dates: 7 days ago and 30 days ago
    day_7 = today - datetime.timedelta(days=7)
    day_30 = today - datetime.timedelta(days=30)
    
    dates_to_fetch = [
        (day_7, "7-day"),
        (day_30, "30-day")
    ]
    
    for date_obj, label in dates_to_fetch:
        date_str = date_obj.strftime("%Y-%m-%d")
        logging.info(f"Fetching {label} data for {date_str}")
        
        url = f"https://tcgcsv.com/archive/tcgplayer/prices-{date_str}.ppmd.7z"
        archive_name = f"prices-{date_str}.ppmd.7z"
        archive_path = Path(archive_name)
        
        if utils.download_file(url, archive_path):
            try:
                utils.process_daily_data(archive_path, date_str, data_dir)
                logging.info(f"Successfully processed {label} data for {date_str}")
            except Exception as e:
                logging.error(f"Failed to process {label} data for {date_str}: {e}")
            finally:
                if archive_path.exists():
                    os.remove(archive_path)
        else:
            logging.warning(f"Failed to download {label} data for {date_str}")

if __name__ == "__main__":
    main()
