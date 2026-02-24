import os
import shutil

from spark_session import create_spark
from transformer import transform_data
from loader import load_to_db
from metadata import is_file_processed, mark_file_processed
from logger_config import setup_logger


# Setup logger
logger = setup_logger()


# Resolve project base directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_PATH = os.path.join(BASE_DIR, "data", "raw")
ARCHIVE_PATH = os.path.join(BASE_DIR, "data", "archive")


def main():

    logger.info("Starting batch pipeline execution")

    # Ensure archive folder exists
    os.makedirs(ARCHIVE_PATH, exist_ok=True)

    spark = create_spark()

    # Process only CSV files
    files = [f for f in os.listdir(RAW_PATH) if f.endswith(".csv")]

    if not files:
        logger.info("No files found to process.")
        spark.stop()
        return

    for file in files:

        file_path = os.path.join(RAW_PATH, file)

        # Incremental check
        if is_file_processed(file):
            logger.info(f"Skipping already processed file: {file}")
            continue

        logger.info(f"Processing file: {file}")

        try:
            # Transform
            df = transform_data(spark, file_path)

            # Load to DB
            count = load_to_db(df)

            # Mark as processed
            mark_file_processed(file)

            # Move to archive
            archive_path = os.path.join(ARCHIVE_PATH, file)
            shutil.move(file_path, archive_path)

            logger.info(f"Loaded {count} records.")
            logger.info(f"Moved {file} to archive.")

        except Exception as e:
            logger.error(f"Error processing {file}: {str(e)}")

    spark.stop()
    logger.info("Batch pipeline execution completed.")


if __name__ == "__main__":
    main()