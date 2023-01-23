import argparse
import data_gen
import logging

from utils.logger import get_project_logger

logging.basicConfig(level=logging.INFO)
logger = get_project_logger()

# parser = argparse.ArgumentParser(description='Generate Sensor Data.', allow_abbrev=True)
# parser.add_argument('--debug', type=str, help='Print messages at stdout.')
# parser.add_argument('--kafka', type=str, help='Send messages to kafka broker.')

def main():
    logger.info('*'*50)
    data_gen.run()


if __name__ == '__main__':
    main()
