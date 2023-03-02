import time
from config_setup import config
from datetime import datetime, timedelta
from pytz import timezone


def get_time_now():
    """
    Get current time and round to nearest midnight.
    """

    _start_time = datetime.now().replace(second=0, microsecond=0)
    _start_time += timedelta(minutes=15)
    _start_time -= timedelta(minutes=_start_time.minute % 30)

    # _start_time = datetime.now().replace(day=4, hour=23, minute=0, second=0)
    return _start_time


def pairwise(iterable):
    return list(zip(iterable, iterable[1:]))


_MSGS_PER_SEC = config['msgs_per_sec'].get(int)  # msg generated per second
# 15min is base-interval according to pdf instructions.
_BASE_INTERVAL_MINS = config['base_interval_mins'].get(int)
_TIME_SCALING_FACTOR = _BASE_INTERVAL_MINS * _MSGS_PER_SEC


def sleep(_interval: int):
    time.sleep(_interval / _TIME_SCALING_FACTOR)
