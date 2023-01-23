import numpy as np
import sys

from abc import ABC, abstractmethod
from datetime import timedelta
from kafka import KafkaProducer
from threading import Event, Thread

from config_setup import config
from utils.helpers import _BASE_INTERVAL_MINS, get_time_now, pairwise, sleep
from utils.logger import get_project_logger

np.random.seed(5)

logger = get_project_logger()


class Producer(ABC, Thread):
    def __init__(self, interval_in_mins, k_topic):
        super().__init__(daemon=True)
        self._stop_event = Event()
        self._producer = None
        self._interval_in_mins = interval_in_mins
        self._k_topic = k_topic

    def stop(self):

        self._stop_event.set()

    def run(self):
        k_host = config['KAFKA_HOST'].get(str)
        k_port = config['KAFKA_CLIENT_PORT'].get(int)

        self._producer = KafkaProducer(bootstrap_servers=[f'{k_host}:{k_port}'],
                                       value_serializer=lambda x: bytes(
                                           str(x), 'utf-8')
                                       )
        logger.info(30*'*' + '\n')
        logger.info(f" Sending to {k_host}:{k_port} on topic: {self._k_topic}")
        while not self._stop_event.is_set():
            msg = self._generate_event()
            self._produce_msg(msg)

            sleep(self._interval_in_mins)

        self._producer.close()

    @abstractmethod
    def _generate_event(self):
        raise NotImplementedError

    def _produce_msg(self, msg):
        # used for debugging
        if ('-1' in sys.argv):
            msg = list(msg)
            msg[-1] = 1.0
            msg = tuple(msg)

        if ('-d' in sys.argv):
            print(msg)
            
        if ('-k' in sys.argv):
            self._producer.send(topic=self._k_topic, value=msg)


class Sensor(Producer):
    """Sensor base class.
    Most of the sensors adhere to it, if there's no specification for
    values generation (i.e. Motion => 1)

    Args:
        Producer (Producer): Kafka Producer, thread based.

    """
    _start_time = get_time_now()

    def __init__(self, name, interval_in_mins, data_range, unit, k_topic):
        """_summary_

        Args:
            name (_type_): name of sensor
            interval_in_mins (_type_): interval for data generation
            data_range (_type_): range for random data generation
            unit (_type_): Unit of measure
            k_topic (_type_): Kafka topic to write data
        """
        super().__init__(interval_in_mins, k_topic)
        self.name = name
        self._data_range = data_range
        self._last_timestamp = Sensor._start_time
        self._unit = unit

    def _generate_event(self):
        """Generates value and timestamp

        Returns:
            Tuple(str, str, float): name, timestamp, value
        """
        value = self._generate_sensor_value()
        timestamp = self._generate_timestamp().strftime('%Y-%m-%d %H:%M')

        return (self.name, timestamp, value)

    def _generate_timestamp(self):
        """Get timestamp and advance."""
        timestamp = self._last_timestamp
        self._last_timestamp += timedelta(minutes=int(self._interval_in_mins))

        return timestamp

    def _generate_sensor_value(self):
        """Return random value in self._data_range."""
        low, high = self._data_range
        value = np.random.uniform(low, high)
        return np.round(value, 4)

    def _minutes_to_midnight(self, from_time):
        tomorrow_midnight = (from_time + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)  # get tomorrow midnight
        
        minutes_to_midnight = (tomorrow_midnight - from_time).seconds // 60 # sec to min
        if minutes_to_midnight == 0:
            minutes_to_midnight = 24*60

        return minutes_to_midnight

    def __str__(self):
        return f'{self.name} ({self._unit})'


# Common Sensors
class ThermoSensor(Sensor):
    def __init__(self, name, data_range, interval_in_mins=_BASE_INTERVAL_MINS, k_topic=''):
        super().__init__(name, interval_in_mins, data_range, 'Celsius', k_topic)


class HVACSensor(Sensor):
    def __init__(self, name, data_range, interval_in_mins=_BASE_INTERVAL_MINS, k_topic=''):
        super().__init__(name, interval_in_mins, data_range, 'Wh', k_topic)


class ElectricalDeviceSensor(Sensor):
    def __init__(self, name, data_range, interval_in_mins=_BASE_INTERVAL_MINS, k_topic=''):
        super().__init__(name, interval_in_mins, data_range, 'Wh', k_topic)


class MotionSensor(Sensor):
    def __init__(self, name, k_topic):
        super().__init__(name, 0, None, None, k_topic)
        self._event_counter = 0
        self._random_event_time = []

        mtm = self._minutes_to_midnight(self._last_timestamp)
        self._setup_daily_run(duration=mtm)

    def _setup_daily_run(self, duration):
        """Generates #daily_msgs events between two midnights or 
        at first run, because of running mid day, has 
        to generate from _start_time till midnight.

        Args:
            duration (int): Interval of data generation in mins.
        """
        daily_msgs = 4 if np.random.uniform() < .5 else 5  # random 4 or 5

        rand_events = list(np.sort(np.random.randint(
            low=0, high=duration, size=daily_msgs)))
        self._random_event_time = [
            rand_events[0]] + list(map(lambda x: x[1]-x[0], pairwise(rand_events)))
        self._event_counter = 0

    def _generate_event(self):
        if len(self._random_event_time) == 0:
            self._setup_daily_run(duration=24*60)
            # skip to end of day for first element
            self._random_event_time[0] += self._minutes_to_midnight(
                self._last_timestamp)

        while self._event_counter != self._random_event_time[0]:
            self._event_counter += 1

        self._event_counter = 0
        mins = self._random_event_time.pop(0)
        sleep(mins)
        self._last_timestamp += timedelta(minutes=int(mins))
        return super()._generate_event()

    def _generate_timestamp(self):
        return self._last_timestamp

    def _generate_sensor_value(self):
        return 1


class WaterConsumptionSensor(Sensor):
    def __init__(self, name, data_range, async_days=[-2, -10], interval_in_mins=_BASE_INTERVAL_MINS, k_topic=''):
        super().__init__(name, interval_in_mins, data_range, 'lt', k_topic=k_topic)
        self._async_every_n_days = dict(zip(async_days, [0]*len(async_days)))

    def _generate_event(self):
        """Water Sensors produce also async.
        Object keeps a dict, i.e. {-2:0, -10:0}, with keys the async days.
        AT every function call, it increases the counter of each key and when the time 
        comes, it produces async.
        Example:
            at -2 days async, the value of dict[-2] will
            be (24*60)*abs(-2)/simtime.
            
            For (15 minutes simtime) per (1 sec runtime),
            we get an async message every 2*96 seconds.

        """
        for day in self._async_every_n_days:
            self._async_every_n_days[day] += 1  # +1 each entry

        for async_day in self._async_every_n_days.keys():
            if self._async_every_n_days[async_day] == np.abs(async_day)*(24*60)/_BASE_INTERVAL_MINS:
                self._async_every_n_days[async_day] = 0
                self._generate_async_event(days=async_day)

        return super()._generate_event()

    def _generate_async_event(self, days):
        value = self._generate_sensor_value()

        # if timestamp is at :30, generate at :15, not to conflict with older.
        timestamp = self._last_timestamp + \
            timedelta(days=days, minutes=-_BASE_INTERVAL_MINS//2)
        timestamp = timestamp.strftime('%Y-%m-%d %H:%M')

        msg = (self.name, timestamp, value)
        self._produce_msg(msg)


# Total Sensors
class TotalSensor(Sensor):
    def __init__(self, name, daily_increment, daily_error, unit, k_topic):
        super().__init__(name, interval_in_mins=24*60,
                         data_range=None, unit=unit, k_topic=k_topic)
        self._daily_increment = daily_increment
        self._daily_error = daily_error
        self._last_value = 0

    def _generate_event(self):
        if self._last_value == 0: # first run, sleep to midnight
            mtm = self._minutes_to_midnight(self._last_timestamp)
            self._last_timestamp += timedelta(days=-1, minutes=mtm)
            sleep(mtm)

        return super()._generate_event()

    def _generate_sensor_value(self):
        sign = 1 if np.random.uniform() < .5 else -1 # random sign for increment
        value = self._last_value + self._daily_increment + sign * \
            np.random.randint(self._daily_error)

        self._last_value = value
        return np.random(value, 4)


class TotalEnergySensor(TotalSensor):
    def __init__(self, name, k_topic):
        super().__init__(name, daily_increment=2600*24, daily_error=1000, unit='Wh', k_topic=k_topic)


class TotalWaterConsumptionSensor(TotalSensor):
    def __init__(self, name, k_topic):
        super().__init__(name, daily_increment=110, daily_error=10, unit='lt', k_topic=k_topic)
