import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimeoutFunction
        extends KeyedProcessFunction<SensorMessage, SensorMessage, SensorMessage> {

    static final long TIMEOUT_MS = 100;
//    private static final Logger LOG = LoggerFactory.getLogger(TimeoutFunction.class);

    private ValueState<SensorMessageState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("DPProcessState", SensorMessageState.class));
    }

    @Override
    public void processElement(
            SensorMessage in,
            Context ctx,
            Collector<SensorMessage> out) throws Exception {

        SensorMessageState processState = state.value();
        if (processState == null) {
            processState = new SensorMessageState();
        }

        // Cancel previous timer
        if (processState.getTimerSetFor() != 0) {
            ctx.timerService().deleteEventTimeTimer(processState.getTimerSetFor());
        }

        // Schedule a timeout
        long trigTime = in.timestamp + TIMEOUT_MS;
        ctx.timerService().registerEventTimeTimer(trigTime);
        out.collect(in);

        // write the state back
        processState.setTimerSetFor(trigTime);
        processState.setPrevMsg(in);
        state.update(processState);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<SensorMessage> out) throws Exception {

        SensorMessageState processState = state.value();
        out.collect(new SensorMessage(
                processState.getPrevMsg().name,
                timestamp,
                -1.0
        ));

        System.out.println("Timer: " + timestamp + " -> " + processState.getPrevMsg().name);
    }
}
