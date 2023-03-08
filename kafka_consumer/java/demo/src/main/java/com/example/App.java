package com.example;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
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
    private final Hashtable<String, IoTDBSink<SensorData>> iotDBSinksDictProcessed;
    private final Hashtable<String, IoTDBSink<SensorData>> iotDBSinksDictRaw;


    public App(Hashtable<String, KafkaSource<SensorData>> kafkaSourcesDict, Hashtable<String, IoTDBSink<SensorData>> iotDBSinksDictProcessed,
               Hashtable<String, IoTDBSink<SensorData>> iotDBSinksDictRaw) {
        this.kafkaSourcesDict = kafkaSourcesDict;
        this.iotDBSinksDictProcessed = iotDBSinksDictProcessed;
        this.iotDBSinksDictRaw = iotDBSinksDictRaw;
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
        String[] iotDBsinks = params.get("iotdb.sinks").split(",");
        String iotDBGroupProcessed = params.get("iotdb.group.processed");
        String iotDBGroupRaw = params.get("iotdb.group.raw");

        Hashtable<String, KafkaSource<SensorData>> kafkaSourcesDict = new Hashtable<>();

        for (String topic : topics) {
            // create a kafka consumer for each topic
            KafkaSource<SensorData> kafkaSource = KafkaSource.<SensorData>builder()
                    .setBootstrapServers(params.get("kafka.bootstrap.server"))
                    .setTopics(String.format("sensors.%s", topic))
                    .setGroupId(params.get("kafka.group.id"))
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SensorDataDeserializationSchema()).build();
            kafkaSourcesDict.put(topic, kafkaSource);
        }

        Hashtable<String, IoTDBSink<SensorData>> iotDBSinksDictProcessed = new Hashtable<>();
        Hashtable<String, IoTDBSink<SensorData>> iotDBSinksDictRaw = new Hashtable<>();
        for (String sink : iotDBsinks) {
            String[] sinkNamesProcessed = params.get(String.format("iotdb.sinks.processed.%s.name", sink)).split(",");
            // create iotdb sink for each sensor, post-processed data
            for (String sinkName : sinkNamesProcessed) {
                iotDBSinksDictProcessed.put(sinkName, getIoTDBSink(iotDBHost, iotDBPort, iotDBUser, iotDBPassword, iotDBGroupProcessed, sink, sinkName));
            }
            // create iotdb sink for each sensor, raw data
            String[] sinkNamesRaw = params.get(String.format("iotdb.sinks.raw.%s.name", sink)).split(",");
            for (String sinkName : sinkNamesRaw) {
                iotDBSinksDictRaw.put(sinkName, getIoTDBSink(iotDBHost, iotDBPort, iotDBUser, iotDBPassword, iotDBGroupRaw, sink, sinkName));
            }
        }

        App kafkaIOJob = new App(kafkaSourcesDict, iotDBSinksDictProcessed, iotDBSinksDictRaw);
        SensorData.simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        kafkaIOJob.execute();
    }

    private static IoTDBSink<SensorData> getIoTDBSink(String iotDBHost, int iotDBPort, String iotDBUser, String iotDBPassword, String iotDBGroup, String sink, String sinkName) {
        // Example:
        // storageGroup |     topic | sensorData.name | PATH_SEPARATOR | measurement(fixed)
        // -------------|-----------|-----------------|----------------|-------------------
        //     root.sth | electrical|           MiAC1 |              - |              value
        // We get data to:
        //      root.sth-electrical-MiAC1-value
        String device = String.join(TsFileConstant.PATH_SEPARATOR, iotDBGroup, sink, sinkName);

        IoTDBSinkOptions options = new IoTDBSinkOptions(
                iotDBHost,
                iotDBPort,
                iotDBUser,
                iotDBPassword,
                Lists.newArrayList(new IoTDBSinkOptions.TimeseriesOption(device, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY)));

        SensorDataIoTDBSerializationSchema serializationSchema = new SensorDataIoTDBSerializationSchema(device);
//        System.out.println(device + " " + sinkName);
        return new IoTDBSink<>(options, serializationSchema);
    }

    void execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<SensorData> watermarkStrategy = WatermarkStrategy
                .<SensorData>forMonotonousTimestamps() // don't wait for window trigger
                .withTimestampAssigner((sensorData, l) -> sensorData.timestamp); // get timestamp from incoming event

        /* ***************************
         * ELECTRICAL ENERGY SENSORS *
         *****************************/
        // Input/Output: Sensors MiAC{1,2}
        DataStream<SensorData> electricalStream = env.fromSource(
                this.kafkaSourcesDict.get("electrical"), watermarkStrategy, String.format("job-%s", "electrical"));

        IoTDBSink<SensorData> sinkMiAC1 = iotDBSinksDictProcessed.get("MiAC1");
        electricalStream
                .filter(s -> s.name.equals("MiAC1"))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .sum("value")
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkMiAC1);

        IoTDBSink<SensorData> sinkMiAC2 = iotDBSinksDictProcessed.get("MiAC2");
        electricalStream
                .filter(s -> s.name.equals("MiAC2"))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .sum("value")
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkMiAC2);

        // Sensors: HVAC{1,2}
        DataStream<SensorData> hvacStream = env.fromSource(
                kafkaSourcesDict.get("hvac"), watermarkStrategy, String.format("job-%s", "hvac"));

        IoTDBSink<SensorData> sinkHVAC1 = iotDBSinksDictProcessed.get("HVAC1");
        hvacStream
                .filter(s -> s.name.equals("HVAC1"))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .sum("value")
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkHVAC1);

        IoTDBSink<SensorData> sinkHVAC2 = iotDBSinksDictProcessed.get("HVAC2");
        hvacStream
                .filter(s -> s.name.equals("HVAC2"))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .sum("value")
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkHVAC2);

        // Input/Output: Sensor Etot
        IoTDBSink<SensorData> sinkEtot = iotDBSinksDictProcessed.get("Etot");
        DataStream<SensorData> totalEnergyStream = env.fromSource(
                this.kafkaSourcesDict.get("totalenergy"), watermarkStrategy, String.format("job-%s", "totalenergy"));
        totalEnergyStream
                .addSink(sinkEtot);

        // Output: Total energy daily diffs
        IoTDBSink<SensorData> sinkEdiff = iotDBSinksDictProcessed.get("Ediff");
        SingleOutputStreamOperator<SensorData> energyDailyDiffStream = totalEnergyStream
                .map(new Aggregators.SubtractAggregate());
        energyDailyDiffStream
                .addSink(sinkEdiff);

        // Output: Energy Leak
        IoTDBSink<SensorData> sinkEleak = iotDBSinksDictProcessed.get("Eleak");
        totalEnergyStream
                // union the streams and invert value of devices to be subtracted
                .union(electricalStream.map(s -> new SensorData(Tuple3.of(s.name, s.timestamp, -s.value))))
                .union(hvacStream.map(s->new SensorData(Tuple3.of(s.name, s.timestamp, -s.value))))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .sum("value")
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkEleak);


        /* ***************
         * WATER SENSORS *
         *****************/
        // Input/Output: Sensor W1
        DataStream<SensorData> waterStream = env.fromSource(
                kafkaSourcesDict.get("water"), watermarkStrategy, String.format("job-%s", "water"));
        OutputTag<SensorData> waterLateDataOutputTag = new OutputTag<>("water-late-data-output", TypeInformation.of(SensorData.class));

        IoTDBSink<SensorData> sinkW1 = iotDBSinksDictProcessed.get("W1");
        SingleOutputStreamOperator<SensorData> waterStreamNoLateData = waterStream
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .allowedLateness(Time.days(3))
                .sideOutputLateData(waterLateDataOutputTag)
                .sum("value");
        waterStreamNoLateData.map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkW1);

        // Input/Output: Sensor Wtot
        IoTDBSink<SensorData> sinkWtot = iotDBSinksDictProcessed.get("Wtot");
        DataStream<SensorData> totalWaterStream = env.fromSource(
                this.kafkaSourcesDict.get("totalwater"), watermarkStrategy, String.format("job-%s", "totalwater"));
        totalWaterStream
                .addSink(sinkWtot);

        // Output: Water late data
        IoTDBSink<SensorData> sinkWaterlate = iotDBSinksDictProcessed.get("Waterlate");
        SideOutputDataStream<SensorData> waterLateDataStream = waterStreamNoLateData.getSideOutput(waterLateDataOutputTag);
        waterLateDataStream
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkWaterlate);

        // Output: Total water daily diffs
        IoTDBSink<SensorData> sinkWdiff = iotDBSinksDictProcessed.get("Wdiff");
        SingleOutputStreamOperator<SensorData> waterDailyDiffStream = totalWaterStream
                .map(new Aggregators.SubtractAggregate());
        waterDailyDiffStream
                .addSink(sinkWdiff);

        // Output: Water leakage
        IoTDBSink<SensorData> sinkWaterLeak = iotDBSinksDictProcessed.get("Wleak");
        totalWaterStream
                .union(waterStreamNoLateData.map(s -> new SensorData(Tuple3.of(s.name, s.timestamp, -s.value))))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .sum("value")
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkWaterLeak);

        /* *****************
         * THERMAL SENSORS *
         *******************/
        // Input/Output: Sensors TH{1,2}
        DataStream<SensorData> thermoStream = env.fromSource(
                kafkaSourcesDict.get("thermo"), watermarkStrategy, String.format("job-%s", "thermo"));

        IoTDBSink<SensorData> sinkTH1 = iotDBSinksDictProcessed.get("TH1");
        thermoStream
                .filter(s -> s.name.equals("TH1"))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Aggregators.AverageAggregate())
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkTH1);

        IoTDBSink<SensorData> sinkTH2 = iotDBSinksDictProcessed.get("TH2");
        thermoStream
                .filter(s -> s.name.equals("TH2"))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Aggregators.AverageAggregate())
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkTH2);

        /* *****************
         * MOTION SENSOR *
         *******************/

        // Input/Output: Sensor Mov1
        DataStream<SensorData> motionStream = env.fromSource(
                kafkaSourcesDict.get("motion"), watermarkStrategy, String.format("job-%s", "motion"));
        IoTDBSink<SensorData> sinkMov1 = iotDBSinksDictProcessed.get("Mov1");
        motionStream
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .sum("value")
                .map(SensorData::WithTimestampEndOfDay)
                .addSink(sinkMov1);

        /* **********
         * RAW DATA *
         ************/
        IoTDBSink<SensorData> sinkMiAC1raw = iotDBSinksDictRaw.get("MiAC1");
        IoTDBSink<SensorData> sinkMiAC2raw = iotDBSinksDictRaw.get("MiAC2");
        IoTDBSink<SensorData> sinkEtotraw = iotDBSinksDictRaw.get("Etot");
        IoTDBSink<SensorData> sinkHVAC1raw = iotDBSinksDictRaw.get("HVAC1");
        IoTDBSink<SensorData> sinkHVAC2aw = iotDBSinksDictRaw.get("HVAC2");
        IoTDBSink<SensorData> sinkMov1raw = iotDBSinksDictRaw.get("Mov1");
        IoTDBSink<SensorData> sinkTH1raw = iotDBSinksDictRaw.get("TH1");
        IoTDBSink<SensorData> sinkTH2raw = iotDBSinksDictRaw.get("TH2");
        IoTDBSink<SensorData> sinkW1raw = iotDBSinksDictRaw.get("W1");
        IoTDBSink<SensorData> sinkWtotraw = iotDBSinksDictRaw.get("Wtot");

        electricalStream.filter(s->s.name.equals("MiAC1")).addSink(sinkMiAC1raw);
        electricalStream.filter(s->s.name.equals("MiAC2")).addSink(sinkMiAC2raw);
        totalEnergyStream.addSink(sinkEtotraw);
        hvacStream.filter(s->s.name.equals("HVAC1")).addSink(sinkHVAC1raw);
        hvacStream.filter(s->s.name.equals("HVAC2")).addSink(sinkHVAC2aw);
        motionStream.addSink(sinkMov1raw);
        thermoStream.filter(s->s.name.equals("TH1")).addSink(sinkTH1raw);
        thermoStream.filter(s->s.name.equals("TH2")).addSink(sinkTH2raw);
        waterStream.addSink(sinkW1raw);
        totalWaterStream.addSink(sinkWtotraw);


        env.execute("Kafka IO");
    }
}
