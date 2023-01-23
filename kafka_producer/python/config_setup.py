import confuse

from settings import DEFAULT_CONFIG_FILEPATH

config_obj = confuse.Configuration('IotSensors', __name__)
config_obj.set_file(DEFAULT_CONFIG_FILEPATH)
config = dict(config_obj)