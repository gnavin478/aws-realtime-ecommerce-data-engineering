import logging
import os

LOG_FOLDER = "logs"

os.makedirs(LOG_FOLDER, exist_ok=True)

def get_logger(logger_name, log_file):

    logger = logging.getLogger(logger_name)

    logger.setLevel(logging.INFO)

    if not logger.handlers:

        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )

        file_handler = logging.FileHandler(
            os.path.join(LOG_FOLDER, log_file)
        )

        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger