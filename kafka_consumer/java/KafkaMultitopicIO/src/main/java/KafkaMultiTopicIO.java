import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class KafkaMultiTopicIO {
        private final KafkaSource<SensorMessage> kafkaSource;
        private final KafkaSink<String> stringKafkaSink;

        public KafkaMultiTopicIO(KafkaSource<SensorMessage> source, KafkaSink<String> sink) {
                this.kafkaSource = source;
                this.stringKafkaSink = sink;
        }

        public static void main(String[] args) throws Exception {
                // connection config
                ParameterTool parameterTool = ParameterTool
                                .fromPropertiesFile("kafka_consumer/java/KafkaMultitopicIO/src/main/resources/config.properties");
                // String[] topics = parameterTool.get("topics").split(",");
                String topic = "water";

                // create a kafka consumer for topic "sensors.thermo"
                KafkaSource<SensorMessage> kafkaSource = KafkaSource.<SensorMessage>builder()
                                .setBootstrapServers(parameterTool.get("bootstrap.servers"))
                                .setTopics(String.format("sensors.%s", topic))
                                .setGroupId(parameterTool.get("group.id"))
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SensorMessageDeserializationSchema()).build();

                // create a kafka producer for pipeline results
                KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                                .setBootstrapServers(
                                                parameterTool.get("bootstrap.servers"))
                                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                                .setTopic(parameterTool.get("output.topic"))
                                                .setValueSerializationSchema(new SimpleStringSchema())
                                                .build())
                                .build();

                KafkaMultiTopicIO kafkaIOJob = new KafkaMultiTopicIO(kafkaSource, kafkaSink);
                kafkaIOJob.execute(topic);
        }

        public void execute(String topic) throws Exception {
                // create execution environment
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

                WatermarkStrategy<SensorMessage> watermarkStrategy = WatermarkStrategy
                                .<SensorMessage>forMonotonousTimestamps()// don't wait for window trigger
                                .withTimestampAssigner((sensorMessage, l) -> sensorMessage.getTimestamp()); 
                                // get timestamp from incoming event

                String tagId = "side-output-" + topic + "-10";
                OutputTag<SensorMessage> late10DaysTag = new OutputTag<>(tagId) {
                };

                SingleOutputStreamOperator<SensorMessage> sensorData = env
                                .fromSource(kafkaSource, watermarkStrategy, String.format("job-%s", topic));

                sensorData
                                .windowAll(TumblingEventTimeWindows.of(Time.days(1))) // sum 1 day
                                .allowedLateness(Time.days(3)) 
                                .sideOutputLateData(late10DaysTag) // side output data over 3 days late. just in case
                                .sum("value")
                                .map(s -> s.toString() + "   -> on-time data")
                                .sinkTo(new PrintSink<>());

                DataStream<SensorMessage> sideOutputStream = sensorData.getSideOutput(late10DaysTag);
                sideOutputStream
                                .map(s -> (s.toString() + "   -> late Data"))
                                .sinkTo(new PrintSink<>());

                env.execute("Kafka IO");
        }
}