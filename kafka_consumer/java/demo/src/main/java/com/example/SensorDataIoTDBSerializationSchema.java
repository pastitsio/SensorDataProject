package com.example;

import org.apache.iotdb.flink.Event;
import org.apache.iotdb.flink.IoTSerializationSchema;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;


import java.util.List;

public class SensorDataIoTDBSerializationSchema implements IoTSerializationSchema<SensorData> {
    private String device;

    public SensorDataIoTDBSerializationSchema(String device) {
        this.device = device;
    }
    @Override
    public Event serialize(SensorData sensorData) {
        Long timestamp = sensorData.timestamp;
        List<String> measurements = List.of("value");
        List<TSDataType> types = List.of(TSDataType.DOUBLE);
        List<Object> values = List.of(sensorData.value);
        return new Event(this.device, timestamp, measurements, types, values);
    }
}
