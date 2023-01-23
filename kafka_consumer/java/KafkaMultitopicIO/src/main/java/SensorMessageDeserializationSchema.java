import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;

public class SensorMessageDeserializationSchema implements DeserializationSchema<SensorMessage> {
    @Override
    public SensorMessage deserialize(byte[] bytes) throws IOException {
        try {
            return new SensorMessage(new String(bytes, StandardCharsets.UTF_8));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEndOfStream(SensorMessage sensorMessage) {
        return false;
    }

    @Override
    public TypeInformation<SensorMessage> getProducedType() {
        return TypeInformation.of(SensorMessage.class);
    }
}

