package com.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serial;

public interface Aggregators {

    // the accumulator, which holds the state of the in-flight aggregate
    class AverageAccumulator {
        public long count = 0;
        public Double sum = 0.0;
        public String sensorName;
        public Long lastTimestamp;

    }

    // implementation of an aggregation function for an 'average'
    class AverageAggregate implements AggregateFunction<SensorData, AverageAccumulator, SensorData> {

        public AverageAccumulator createAccumulator() {
            return new AverageAccumulator();
        }

        public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
            a.count += b.count;
            a.sum += b.sum;
            return a;
        }

        public AverageAccumulator add(SensorData s, AverageAccumulator acc) {
            acc.sum += s.value;
            acc.count++;
            acc.sensorName = s.name;
            acc.lastTimestamp = s.timestamp;
            return acc;
        }

        public SensorData getResult(AverageAccumulator acc) {
            return new SensorData(Tuple3.of(acc.sensorName, acc.lastTimestamp, acc.sum / (double) acc.count));
        }
    }

    class SubtractAggregate implements MapFunction<SensorData, SensorData>{
        @Serial
        private static final long serialVersionUID = 1L;
        private Double prev = null;

        @Override
        public SensorData map(SensorData s) {
            // first time returns first value, then consecutive diffs.
            double diff = s.value;
            if (prev != null) {
                diff = diff - prev;
            }
            prev = s.value;
            return new SensorData(Tuple3.of(s.name, s.timestamp, diff));
        }
    }
}
