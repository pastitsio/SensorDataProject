from termcolor import colored

from config_setup import config
from sensors import ThermoSensor, HVACSensor, ElectricalSensor, MotionSensor, \
    WaterSensor, TotalEnergySensor, TotalWaterSensor
from utils.logger import get_project_logger

logger = get_project_logger()


def run():
    
    logger.info('***** Generating Sensors Data *****')
    _interval = colored(str(config["base_interval_mins"].get(int)) + ' mins', 'yellow')
    logger.info(f'******** Interval: {_interval} ********')
    logger.info(35*'*' + '\n')

    print('Press ^C to end run!\n')
    # Sensors
    sensor_threads = [
        #HVACSensor(name='HVAC1', data_range=(0, 100)),
        #HVACSensor(name='HVAC2', data_range=(0, 200)),
        # ElectricalSensor(name='MiAC1', data_range=(0, 150)),
        # ElectricalSensor(name='MiAC2', data_range=(0, 200)),
        # TotalEnergySensor(name='Etot'),
        # ThermoSensor(name='TH1', data_range=(12, 35)),
        # ThermoSensor(name='TH2', data_range=(12, 35)),
        # MotionSensor(name='Mov1'),
        WaterSensor(name='W1', data_range=(0, 1), async_days=[-2, -10]),
        TotalWaterSensor(name='Wtot'),
        ]

    # KafkaProducer is thread-safe.
    # It's recommended to be utilized by threads for faster runtimes.
    try:
        # Start the threads
        for t in sensor_threads:
            t.start()
        while True:
            pass
    except KeyboardInterrupt:
        for t in sensor_threads:
            t.stop()
        for t in sensor_threads:
            t.join()

