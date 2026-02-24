import logging
import os

def setup_logger():

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(BASE_DIR, "logs")

    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "pipeline.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)