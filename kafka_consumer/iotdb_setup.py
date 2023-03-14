from iotdb.Session import Session
from iotdb.tsfile.common.constant.TsFileConstant import TsFileConstant
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from jproperties import Properties

config = Properties()
with open("./java/demo/src/main/resources/config.properties", 'rb') as f:
    config.load(f)


iotdb_host = config.get('iotdb.host').data
iotdb_port = config.get('iotdb.port').data
iotdb_user = config.get('iotdb.user').data
iotdb_password = config.get('iotdb.password').data

group_processed = config.get('iotdb.group.processed').data
group_raw = config.get('iotdb.group.raw').data

sinks = config.get('iotdb.sinks').data.split(',')

session = Session(iotdb_host, iotdb_port, iotdb_user, iotdb_password, fetch_size=1024, zone_id="UTC+2")
session.open(False)

separator = TsFileConstant.PATH_SEPARATOR
data_type = TSDataType.DOUBLE
encoding = TSEncoding.GORILLA
compressor = Compressor.SNAPPY

for sink in sinks:
    # processed
    processed = config.get(f'iotdb.sinks.processed.{sink}.name').data.split(',')
    for sink_name in processed:
        ts_path = separator.join([group_processed, sink, sink_name, 'value'])
        session.create_time_series(ts_path, data_type, encoding, compressor,
        props=None, tags=None, attributes=None, alias=None)
    # raw    
    raw = config.get(f'iotdb.sinks.raw.{sink}.name').data.split(',')
    for sink_name in raw:
        ts_path = separator.join([group_raw, sink, sink_name, 'value'])
        session.create_time_series(ts_path, data_type, encoding, compressor,
        props=None, tags=None, attributes=None, alias=None)

session.close()
