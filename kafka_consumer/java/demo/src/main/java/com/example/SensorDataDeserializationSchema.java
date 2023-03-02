package com.example;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class SensorDataDeserializationSchema implements DeserializationSchema<SensorData> {
    @Override
    public SensorData deserialize(byte[] bytes) {
        String msg = new String(bytes, StandardCharsets.UTF_8);
        try {
            return new SensorData(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEndOfStream(SensorData sensorData) {
        return false;
    }

    @Override
    public TypeInformation<SensorData> getProducedType() {
        return TypeInformation.of(SensorData.class);
    }
}

