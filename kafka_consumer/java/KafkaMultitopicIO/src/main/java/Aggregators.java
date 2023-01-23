import org.apache.flink.api.common.functions.AggregateFunction;

public interface Aggregators {

    // the accumulator, which holds the state of the in-flight aggregate
    class AverageAccumulator {
        public long count = 0;
        public Double sum = 0.0;
    }

    // implementation of an aggregation function for an 'average'
    class AverageAggregate implements AggregateFunction<SensorMessage, AverageAccumulator, Double> {

        public AverageAccumulator createAccumulator() {
            return new AverageAccumulator();
        }

        public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
            a.count += b.count;
            a.sum += b.sum;
            return a;
        }

        public AverageAccumulator add(SensorMessage s, AverageAccumulator acc) {
            acc.sum += s.value;
            acc.count++;
            return acc;
        }

        public Double getResult(AverageAccumulator acc) {
            return acc.sum / (double) acc.count;
        }
    }

    class SumAggregate extends AverageAggregate {
        @Override
        public Double getResult(AverageAccumulator acc) {
            return acc.sum;
        }
    }
}
