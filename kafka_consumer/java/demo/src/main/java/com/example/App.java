package com.example;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.iotdb.flink.IoTDBSink;
import org.apache.iotdb.flink.options.IoTDBSinkOptions;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.Hashtable;
import java.util.TimeZone;

public class App {
    private final Hashtable<String, KafkaSource<SensorData>> kafkaSourcesDict;
    private final Hashtable<String, IoTDBSink<SensorData>> iotDBSinksDict;


    public App(Hashtable<String, KafkaSource<SensorData>> kafkaSourcesDict, Hashtable<String, IoTDBSink<SensorData>> iotDBSinksDict) {
        this.kafkaSourcesDict = kafkaSourcesDict;
        this.iotDBSinksDict = iotDBSinksDict;
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromPropertiesFile("src/main/resources/config.properties");

        // Kafka config
        String[] topics = params.get("kafka.topics").split(",");
        // IoTDB config
        String iotDBHost = (params.get("iotdb.host"));
        int iotDBPort = (Integer.parseInt(params.get("iotdb.port")));
        String iotDBUser = params.get("iotdb.user");
        String iotDBPassword = params.get("iotdb.password");
        String iotDBStorageGroup = params.get("iotdb.storage_group");

        Hashtable<String, KafkaSource<SensorData>> kafkaSourcesDict = new Hashtable<>();
        Hashtable<String, IoTDBSink<SensorData>> iotDBSinksDict = new Hashtable<>();
        for (String topic : topics) {
            // create a kafka consumer for each topic
            KafkaSource<SensorData> kafkaSource = KafkaSource.<SensorData>builder()
                    .setBootstrapServers(params.get("kafka.bootstrap.server"))
                    .setTopics(String.format("sensors.%s", topic))
                    .setGroupId(params.get("kafka.group.id"))
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SensorDataDeserializationSchema()).build();
            kafkaSourcesDict.put(topic, kafkaSource);

            // create a kafka consumer for each sensor
            String sinkName = params.get(String.format("kafka.topics.%s.name", topic));
            int sinkSensorCount = Integer.parseInt(params.get(String.format("kafka.topics.%s.count", topic)));

            for (int i = 1; i <= sinkSensorCount; i++) {
                String sensorName;
                // sensorName matches actual sensor name and will be used to dict-get the sink for each stream.
                if (sinkSensorCount > 1) sensorName = sinkName + i;
                else sensorName = sinkName;

                // device example:
                // storageGroup = root.sth | topic = electrical | sensorData.name = MiAC1 | PATH_SEPARATOR = -
                // => device = root.sth-electrical-MiAC1
                // => measurement = ${device}-value
                String device = String.join(TsFileConstant.PATH_SEPARATOR, iotDBStorageGroup, topic, sensorName);

                IoTDBSinkOptions options = new IoTDBSinkOptions(
                        iotDBHost, iotDBPort, iotDBUser, iotDBPassword,
                        Lists.newArrayList(new IoTDBSinkOptions.TimeseriesOption(device, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY)));
                SensorDataIoTDBSerializationSchema serializationSchema = new SensorDataIoTDBSerializationSchema(device);
                IoTDBSink<SensorData> ioTDBSink = new IoTDBSink<>(options, serializationSchema);
                iotDBSinksDict.put(sensorName, ioTDBSink);
            }

        }

        App kafkaIOJob = new App(kafkaSourcesDict, iotDBSinksDict);
        SensorData.simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        kafkaIOJob.execute();
    }

    void execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        WatermarkStrategy<SensorData> watermarkStrategy = WatermarkStrategy
                .<SensorData>forMonotonousTimestamps()// don't wait for window trigger
                .withTimestampAssigner((sensorData, l) -> sensorData.timestamp); // get timestamp from incoming event

        String topic = "electrical";
        DataStream<SensorData> electricalStream = env.fromSource(
                this.kafkaSourcesDict.get(topic), watermarkStrategy, String.format("job-%s", topic));

        IoTDBSink<SensorData> sink1 = iotDBSinksDict.get("MiAC1");
        electricalStream
                .filter(s -> s.name.equals("MiAC1"))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .sum("value")
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sink1);

        IoTDBSink<SensorData> sink2 = iotDBSinksDict.get("MiAC2");
        electricalStream
                .filter(s -> s.name.equals("MiAC2"))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .sum("value")
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sink2);


//        topic = "totalenergy";
//        DataStream<SensorData> totalEnergyStream = env.fromSource(
//                this.kafkaSourcesDict.get(topic), watermarkStrategy, String.format("job-%s", topic));
//        SingleOutputStreamOperator<SensorData> totalEnergyDailyDiffStream = totalEnergyStream
//                .map(new Aggregators.SubtractAggregate());
//
//        totalEnergyDailyDiffStream
//                .map(SensorData::toString)
//                .sinkTo(new PrintSink<>("Δetot: "));
//
//        // energy leak
//        totalEnergyStream
//                .union(electricalStream.map(s -> new SensorData(Tuple3.of(s.name, s.timestamp, -s.value))))
////                .union(hvacStream.map(s->new SensorData(Tuple3.of(s.name, s.timestamp, -s.value))))
//                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
//                .sum("value")
//                .map(SensorData::WithTimestampEndOfDay)
//                .map(SensorData::toString)
//                .sinkTo(new PrintSink<>("leak: "));


        env.execute("Kafka IO");
    }
}
