import logging
from config_setup import config

logger_name = config['logger_name'].get(str)
def get_project_logger(logger=logger_name):
    return logging.getLogger(logger)
