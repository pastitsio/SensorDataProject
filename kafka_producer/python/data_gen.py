from config_setup import config
from sensors import ThermoSensor, HVACSensor, ElectricalDeviceSensor, MotionSensor, \
    WaterConsumptionSensor, TotalEnergySensor, TotalWaterConsumptionSensor
from utils.logger import get_project_logger

logger = get_project_logger()


def run():
    
    logger.info('***** Generating Sensors Data *****')
    logger.info(f"***** Interval: {config['base_interval_mins'].get(int)} mins *****")

    # Sensors
    sensor_threads = [
        # ThermoSensor(name='TH1', data_range=(12, 35), k_topic='sensors.thermo'),
        # ThermoSensor(name='TH2', data_range=(12, 35), k_topic='sensors.thermo'),
        # HVACSensor(name='HVAC1', data_range=(0, 100), k_topic='sensors.hvac'),
        # HVACSensor(name='HVAC2', data_range=(0, 200), k_topic='sensors.hvac'),
        # ElectricalDeviceSensor(name='MiAC1', data_range=(0, 150), k_topic='sensors.electrical'),
        # ElectricalDeviceSensor(name='MiAC2', data_range=(0, 200), k_topic='sensors.electrical'),
        # MotionSensor(name='Mov1', k_topic='sensors.motion'),
        WaterConsumptionSensor(name='W1', data_range=(0, 1), async_days=[-2, -10], k_topic='sensors.water'),
        # TotalEnergySensor(name='Etot', k_topic='sensors.total'),
        # TotalWaterConsumptionSensor(name='Wtot', k_topic='sensors.total'),
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

