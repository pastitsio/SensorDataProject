import os
from config_setup import config

KAFKA_HOME = '/opt/kafka'

topics = config['KAFKA_TOPICS'].get(list)

kafka_host = config['KAFKA_HOST'].get(str)
kafka_port = config['KAFKA_CLIENT_PORT'].get(int)
kafka_partitions = config['KAFKA_PARTITIONS'].get(int)
bootstrap_server = f'{kafka_host}:{kafka_port}'


def create_cmd(topic):
    return f'{KAFKA_HOME}/bin/kafka-topics.sh \
                        --bootstrap-server {bootstrap_server}\
                        --create --topic {topic} \
                        --replication-factor 1 \
                        --partitions {kafka_partitions}'


for t in topics:
    topic = f'sensors.{t}'
    os.system(create_cmd(topic))
