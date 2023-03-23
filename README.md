# SensorDataProject
[Analysis and Design of Information Systems](https://www.ece.ntua.gr/gr/undergraduate/courses/3321)
---
## Project setup
1. Run [Kafka setup](kafka_producer/python/setup_kafka.py)
2. Run [IoTDB setup](kafka_consumer/iotdb_setup.py)

## Run
1. Run [Flink App](kafka_consumer/java/demo/src/main/java/com/example/App.java)
2. Run [Data Gen](kafka_producer/python/main.py) with option `-k`.

For debugging purposes, we can run [data_gen.py](kafka_producer/python/main.py) with options:
- `-1`: all sensors values are 1.
- `-k`: produces to Kafka. **MANDATORY FOR KAFKA**.
- `-d`: prints generated messages on stdout.

In file [data_gen.py](kafka_producer/python/data_gen.py#34) we can modify the array `sensor_threads` to select which sensors produce messages.
